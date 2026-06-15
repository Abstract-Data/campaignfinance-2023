"""PG-gated regression: one representative person per entity (Blocker #2).

The vectorized engine USED to assign an entity's representative ``person_id`` in three
families independently. Because the person dedup key excludes the suffix
(``(lower(first), lower(last))``) while an entity's ``normalized_name`` INCLUDES it, one
person ("JOHN ANDERSON" / "John Anderson JR" collapse to one person) could be assigned to
TWO entities -> ``unified_entities_person_id_key`` UniqueViolation on real Postgres.

``finalize_entity_representatives`` now assigns each entity ONE representative
deterministically, after all families run. This test loads a small real slice WITH a
deliberate suffix-variant collision into a Postgres database that KEEPS the dedup indexes
and the one-to-one UNIQUE constraints (only FK constraints are dropped, since a capped slice
has dangling refs), and asserts:

  * the load COMPLETES (no UniqueViolation), and
  * no ``person_id`` appears on >1 entity, and
  * no ``committee_id`` appears on >1 entity.

Skipped unless a local PostgreSQL is reachable (``BENCH_PG_BASE`` or localhost:5432). It
creates and drops a throwaway database; it does not touch project data.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import create_engine, text

from app.core import models  # noqa: F401 — register tables
from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")
_TEST_DB = "cf_entity_one_to_one_test"

# RCPT/EXPN golden fixtures drive the dim/entity/person families. We add a synthetic
# cross-family suffix-variant collision on top (RCPT contributor with NO suffix + a LOAN
# lender of the SAME first/last WITH a suffix) so the actual blocker condition is exercised,
# not just absent. The two share ONE person (suffix-excluded key) but spawn TWO entities
# (suffix-included normalized_name) — the exact shape that produced the old violation.
_GOLDEN = {"RCPT": "contribs_golden.parquet", "EXPN": "expend_golden.parquet"}


def _pg_available() -> bool:
    try:
        eng = create_engine(f"{_PG_BASE}/postgres")
        with eng.connect():
            return True
    except Exception:
        return False
    finally:
        try:
            eng.dispose()
        except Exception:
            pass


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


def _drop_create(db_name: str) -> None:
    from psycopg2 import sql

    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))
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
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))
        cur.close()
    finally:
        raw.close()
        admin.dispose()


def _drop_fks_only(engine) -> None:
    """Drop only FOREIGN KEY constraints; KEEP unique constraints + dedup indexes.

    A capped real slice has dangling refs the ORM would null in app logic, so FK enforcement
    is irrelevant noise here. The one-to-one UNIQUE constraints on
    ``unified_entities.person_id`` / ``committee_id`` — the thing under test — are NOT touched.
    """
    raw = engine.raw_connection()
    try:
        pg = raw.driver_connection
        cur = pg.cursor()
        cur.execute(
            "SELECT format('ALTER TABLE %s DROP CONSTRAINT %I', conrelid::regclass, conname) "
            "FROM pg_constraint WHERE contype='f' AND connamespace='public'::regnamespace"
        )
        for (stmt,) in cur.fetchall():
            cur.execute(stmt)
        pg.commit()
        cur.close()
    finally:
        raw.close()


def _rcpt_no_suffix_row() -> pl.DataFrame:
    """An RCPT contributor "JOHN ZZSUFFIXTEST" with NO suffix -> person + entity
    ``PERSON:"john zzsuffixtest"``."""
    return pl.DataFrame([{
        "recordType": "RCPT", "formTypeCd": "MPAC", "schedFormTypeCd": "A1",
        "reportInfoIdent": "730", "receivedDt": "20000705", "infoOnlyFlag": "N",
        "filerIdent": "00010883", "filerTypeCd": "MPAC", "filerName": "El Paso Energy Corp. PAC",
        "contributionInfoId": "990000001", "contributionDt": "20000530",
        "contributionAmount": "50.00", "contributorPersentTypeCd": "INDIVIDUAL",
        "contributorNameLast": "ZZSUFFIXTEST", "contributorNameFirst": "JOHN",
        "contributorNameSuffixCd": None, "contributorStreetCity": "AUSTIN",
        "contributorStreetStateCd": "TX", "contributorStreetCountryCd": "USA",
        "contributorStreetPostalCode": "78701", "contributorOosPacFlag": "N",
        "itemizeFlag": "Y", "travelFlag": "N",
    }])


def _loan_with_suffix_row() -> pl.DataFrame:
    """A LOAN lender "JOHN ZZSUFFIXTEST JR" — the SAME (first, last) as the RCPT contributor
    but WITH a suffix. The suffix-EXCLUDED person key reuses the existing person, but the
    detail family builds a SECOND entity ``PERSON:"john zzsuffixtest jr"`` from the
    suffix-included name. One person, two entities -> the blocker condition."""
    return pl.DataFrame([{
        "recordType": "LOAN", "formTypeCd": "MPAC", "schedFormTypeCd": "G1",
        "reportInfoIdent": "730", "receivedDt": "20000705", "infoOnlyFlag": "N",
        "filerIdent": "00010883", "filerTypeCd": "MPAC", "filerName": "El Paso Energy Corp. PAC",
        "loanInfoId": "770000001", "loanAmount": "100.00", "loanDt": "20000601",
        "lenderNameLast": "ZZSUFFIXTEST", "lenderNameFirst": "JOHN", "lenderNameSuffixCd": "JR",
        "lenderStreetCity": "AUSTIN", "lenderStreetStateCd": "TX",
        "lenderStreetPostalCode": "78701",
    }])


def _build_fixtures(slice_dir: Path) -> None:
    """Golden RCPT/EXPN + the synthetic cross-family suffix-variant collision.

    The RCPT contributor (no suffix) is appended to the golden contribs file; the LOAN lender
    (with suffix) goes into its own loans file so the detail_children family runs.
    """
    if slice_dir.exists():
        shutil.rmtree(slice_dir)
    slice_dir.mkdir(parents=True)
    shutil.copy2(FIXTURES / _GOLDEN["EXPN"], slice_dir / _GOLDEN["EXPN"])
    rcpt = pl.read_parquet(FIXTURES / _GOLDEN["RCPT"])
    pl.concat([rcpt, _rcpt_no_suffix_row()], how="diagonal_relaxed").write_parquet(
        slice_dir / _GOLDEN["RCPT"]
    )
    _loan_with_suffix_row().write_parquet(slice_dir / "loans_golden.parquet")


@pytest.fixture()
def pg_engine():
    from sqlmodel import SQLModel

    _drop_create(_TEST_DB)
    engine = create_engine(f"{_PG_BASE}/{_TEST_DB}")
    SQLModel.metadata.create_all(engine)
    _drop_fks_only(engine)
    try:
        yield engine
    finally:
        engine.dispose()
        _drop(_TEST_DB)


def test_entity_representatives_one_to_one(pg_engine, tmp_path: Path):
    """The load completes with one-to-one constraints enforced, and no person_id /
    committee_id is shared across entities."""
    slice_dir = tmp_path / "entity_one_to_one_slice"
    _build_fixtures(slice_dir)

    # Must NOT raise (no unified_entities_person_id_key violation).
    run_vectorized(pg_engine, slice_dir)

    with pg_engine.connect() as c:
        dup_person = c.execute(
            text(
                "SELECT count(*) FROM (SELECT person_id FROM unified_entities "
                "WHERE person_id IS NOT NULL GROUP BY person_id HAVING count(*) > 1) q"
            )
        ).scalar()
        dup_committee = c.execute(
            text(
                "SELECT count(*) FROM (SELECT committee_id FROM unified_entities "
                "WHERE committee_id IS NOT NULL GROUP BY committee_id HAVING count(*) > 1) q"
            )
        ).scalar()
        # Sanity: the cross-family suffix variant really produced TWO entities for ONE
        # person, so the finalize step had a genuine collision to resolve (otherwise the
        # test would pass vacuously).
        n_zz_entities = c.execute(
            text(
                "SELECT count(*) FROM unified_entities "
                "WHERE normalized_name LIKE 'john zzsuffixtest%'"
            )
        ).scalar()
        n_zz_persons = c.execute(
            text("SELECT count(*) FROM unified_persons WHERE last_name = 'ZZSUFFIXTEST'")
        ).scalar()

    assert dup_person == 0, "a person was assigned as representative of >1 entity"
    assert dup_committee == 0, "a committee_id appears on >1 entity"
    assert n_zz_persons == 1, "expected the suffix-variant pair to collapse to one person"
    assert n_zz_entities == 2, "expected the suffix-variant pair to create two distinct entities"
