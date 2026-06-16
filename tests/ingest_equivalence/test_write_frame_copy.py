"""Deterministic proof that ``write_frame``'s Postgres COPY fast-path produces rows
identical to the equivalence-gated bulk_upsert path.

Unlike the end-to-end benchmark (which relaxes dedup indexes and so can't isolate the
write method from dedup non-determinism), this feeds the SAME frame to both paths into the
SAME table and compares — so any divergence is the COPY serialization/staging logic itself.

Skipped unless a local PostgreSQL is reachable (``BENCH_PG_BASE`` or localhost:5432). It
creates and drops a throwaway database; it does not touch project data.
"""

from __future__ import annotations

import os

import polars as pl
import pytest
from sqlalchemy import create_engine, delete

from app.core import models  # noqa: F401 — register tables
from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
from app.core.ingest_vectorized import common
from app.core.models import UnifiedCommittee, UnifiedPerson

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")
_TEST_DB = "cf_writeframe_copytest"


def _pg_available() -> bool:
    try:
        eng = create_engine(f"{_PG_BASE}/postgres")
        with eng.connect():
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


def _drop_create(db_name: str) -> None:
    from psycopg2 import sql

    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name))
        )
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        cur.close()
    finally:
        raw.close()
        admin.dispose()


def _drop(db_name: str) -> None:
    from psycopg2 import sql

    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name))
        )
        cur.close()
    finally:
        raw.close()
        admin.dispose()


@pytest.fixture()
def pg_engine():
    """A freshly created+bootstrapped Postgres DB with the unified schema; dropped after."""
    from sqlmodel import SQLModel

    _drop_create(_TEST_DB)
    engine = create_engine(f"{_PG_BASE}/{_TEST_DB}")
    # Create the state schemas so create_all succeeds even when an earlier test in the same
    # session has imported the schema-qualified state-validator models (schema='texas'/
    # 'oklahoma') into the shared SQLModel.metadata (documented cross-test pollution; no-op in
    # isolation).
    from sqlalchemy import text

    with engine.begin() as _conn:
        _conn.execute(text("CREATE SCHEMA IF NOT EXISTS texas"))
        _conn.execute(text("CREATE SCHEMA IF NOT EXISTS oklahoma"))
    SQLModel.metadata.create_all(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        _drop(_TEST_DB)


def _session(engine):
    from sqlmodel import Session

    return Session(engine, expire_on_commit=False)


def _snap_one(engine, table: str) -> dict:
    return {table: snapshot_unified(engine)[table]}


def _clear(engine, model) -> None:
    """Empty a table between the two write methods (core construct — no string SQL)."""
    with engine.connect() as conn:
        conn.execute(delete(model.__table__))
        conn.commit()


def _write(engine, model, frame, *, conflict_cols, update_cols=None, disable_copy: bool) -> None:
    prev = os.environ.get("VECTORIZED_DISABLE_COPY")
    if disable_copy:
        os.environ["VECTORIZED_DISABLE_COPY"] = "1"
    else:
        os.environ.pop("VECTORIZED_DISABLE_COPY", None)
    try:
        with _session(engine) as s:
            common.write_frame(
                s, model, frame, conflict_cols=conflict_cols, update_cols=update_cols
            )
    finally:
        if prev is None:
            os.environ.pop("VECTORIZED_DISABLE_COPY", None)
        else:
            os.environ["VECTORIZED_DISABLE_COPY"] = prev


def test_copy_equals_bulk_upsert_plain_insert(pg_engine):
    """conflict_cols=None: COPY == bulk_upsert for the tricky cases —
    - empty string "" must round-trip as "" (NOT NULL — distinct from None),
    - comma/quote-bearing text (CSV quoting),
    - ``person_type`` OMITTED so the scalar enum default (PersonType.UNKNOWN) is injected
      as an enum object and must serialize as its NAME, not the lowercase value."""
    frame = pl.DataFrame(
        {
            "first_name": ["Ann", "", None],  # "" preserved; None -> NULL
            "last_name": ["Lee", None, "Solo"],
            "organization": [None, "Acme, Inc.", 'O"Hare "LLC"'],
            # person_type intentionally omitted -> default-injected enum object
        }
    )
    _write(pg_engine, UnifiedPerson, frame, conflict_cols=None, disable_copy=False)
    copy_snap = _snap_one(pg_engine, "unified_persons")

    _clear(pg_engine, UnifiedPerson)
    _write(pg_engine, UnifiedPerson, frame, conflict_cols=None, disable_copy=True)
    upsert_snap = _snap_one(pg_engine, "unified_persons")

    assert diff_snapshots(copy_snap, upsert_snap) == []
    rows = copy_snap["unified_persons"]
    assert len(rows) == 3
    # "" preserved as empty string (the COPY NULL-sentinel fix); None stored as NULL.
    first_names = {r["first_name"] for r in rows}
    assert "" in first_names and None in first_names


@pytest.mark.parametrize("disable_copy", [False, True], ids=["copy", "bulk_upsert"])
def test_write_frame_isolates_bad_rows_to_ingest_errors(pg_engine, disable_copy):
    """A row-level integrity failure (the dominant dirty-data case: an FK orphan) must NOT
    fail the whole batch — the good rows commit and the bad row is routed verbatim to
    ingest_errors, on BOTH the COPY and bulk_upsert write paths. This is the vectorized
    equivalent of the ORM loader's tier-3 row isolation."""
    from sqlmodel import select

    from app.core.models import State
    from app.core.models.tables import IngestError

    with _session(pg_engine) as s:
        s.add(State(id=1, code="TX", name="Texas"))
        s.commit()

    # Row 2 references a non-existent state (FK orphan); rows 1 and 3 are valid.
    frame = pl.DataFrame(
        {
            "first_name": ["Ann", "Bad", "Cy"],
            "last_name": ["Lee", "Orphan", "Solo"],
            "state_id": [1, 999, 1],
        }
    )
    prev = os.environ.get("VECTORIZED_DISABLE_COPY")
    if disable_copy:
        os.environ["VECTORIZED_DISABLE_COPY"] = "1"
    else:
        os.environ.pop("VECTORIZED_DISABLE_COPY", None)
    try:
        with _session(pg_engine) as s:
            written = common.write_frame(
                s,
                UnifiedPerson,
                frame,
                conflict_cols=None,
                error_ctx={"state_id": 1, "record_type": "RCPT", "source_file": "contribs.parquet"},
            )
    finally:
        if prev is None:
            os.environ.pop("VECTORIZED_DISABLE_COPY", None)
        else:
            os.environ["VECTORIZED_DISABLE_COPY"] = prev

    assert written == 2
    with _session(pg_engine) as s:
        persons = s.exec(select(UnifiedPerson)).all()
        errs = s.exec(select(IngestError)).all()
    assert {p.first_name for p in persons} == {"Ann", "Cy"}  # good rows committed
    assert len(errs) == 1  # exactly the orphan isolated
    err = errs[0]
    assert err.record_type == "RCPT" and err.state_id == 1
    assert err.source_file == "contribs.parquet"
    assert err.error_type  # populated with the DB exception class
    assert "Orphan" in err.raw_data  # verbatim bad row captured


def test_copy_equals_bulk_upsert_on_conflict(pg_engine):
    """conflict_cols + update_cols: COPY's staging + INSERT...ON CONFLICT DO UPDATE matches
    bulk_upsert — both for the initial insert and the conflicting update."""
    base = pl.DataFrame({"filer_id": ["A1", "B2"], "name": ["First A", "First B"]})
    update = pl.DataFrame({"filer_id": ["B2", "C3"], "name": ["Updated B", "First C"]})
    cc = ["filer_id"]
    uc = ["name"]

    _write(pg_engine, UnifiedCommittee, base, conflict_cols=cc, update_cols=uc, disable_copy=False)
    _write(
        pg_engine, UnifiedCommittee, update, conflict_cols=cc, update_cols=uc, disable_copy=False
    )
    copy_snap = _snap_one(pg_engine, "unified_committees")

    _clear(pg_engine, UnifiedCommittee)
    _write(pg_engine, UnifiedCommittee, base, conflict_cols=cc, update_cols=uc, disable_copy=True)
    _write(pg_engine, UnifiedCommittee, update, conflict_cols=cc, update_cols=uc, disable_copy=True)
    upsert_snap = _snap_one(pg_engine, "unified_committees")

    assert diff_snapshots(copy_snap, upsert_snap) == []
    # 3 distinct filer_ids; B2 reflects the update on both paths.
    rows = {r["filer_id"]: r for r in copy_snap["unified_committees"]}
    assert set(rows) == {"A1", "B2", "C3"}
    assert rows["B2"]["name"] == "Updated B"
