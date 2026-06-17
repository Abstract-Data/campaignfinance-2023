"""Revision 0002 must dedup legacy ``unified_transactions`` duplicates and then create the
strict unique index — on an existing (stamped-at-baseline) polluted DB — while leaving a fresh
DB untouched. PG-gated: the dedup + partial unique index are Postgres-specific (sqlite skips them).

Simulates the real upgrade path for a pre-Wave-2 database: schema present (``create_all``) but
WITHOUT the unique index and WITH duplicate ``(state_id, transaction_type, transaction_id)``
groups, marked ``alembic stamp 0001_baseline`` (the baseline never runs on existing DBs). Then
``upgrade head`` runs 0002 and must:

  * drop the non-surviving duplicates (keep the lowest ``id`` per group) and their children,
  * leave non-duplicate rows and NULL-``transaction_id`` rows untouched,
  * create ``uix_transactions_state_type_sourceid``,
  * be idempotent (and the resulting index forbids new duplicates).

Rows are inserted via the ORM so all Python-side defaults (uuid, timestamps, ...) are populated.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, inspect, text

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")
_TEST_DB = "cf_dedup_migration_test"
_INDEX = "uix_transactions_state_type_sourceid"


def _pg_available() -> bool:
    try:
        with create_engine(f"{_PG_BASE}/postgres").connect():
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


def _admin_exec(stmt) -> None:
    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(stmt)
        cur.close()
    finally:
        raw.close()
        admin.dispose()


def _drop_create(name: str) -> None:
    from psycopg2 import sql

    _admin_exec(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
    _admin_exec(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def _drop(name: str) -> None:
    from psycopg2 import sql

    _admin_exec(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def _ensure_state_schemas(url: str) -> None:
    eng = create_engine(url)
    try:
        with eng.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS texas"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS oklahoma"))
    finally:
        eng.dispose()


def _bootstrap_unmanaged_schema(url: str) -> None:
    """Create all tables (no Alembic, no dedup indexes) — a pre-Alembic 'legacy' schema. Defensively
    drop the unique index in case a future model ever defines it inline (today it does not)."""
    from sqlmodel import SQLModel

    from app.core import models  # noqa: F401 — register tables

    eng = create_engine(url)
    try:
        SQLModel.metadata.create_all(eng)
        with eng.begin() as conn:
            conn.execute(text("DROP INDEX IF EXISTS uix_transactions_state_type_sourceid"))
    finally:
        eng.dispose()


def _index_exists(url: str) -> bool:
    insp = inspect(create_engine(url))
    return any(i["name"] == _INDEX for i in insp.get_indexes("unified_transactions"))


def _dup_group_count(conn) -> int:
    return conn.execute(
        text(
            "SELECT count(*) FROM (SELECT 1 FROM unified_transactions "
            "WHERE transaction_id IS NOT NULL "
            "GROUP BY state_id, transaction_type, transaction_id HAVING count(*) > 1) g"
        )
    ).scalar_one()


def _version_count(conn, txn_id: int) -> int:
    return conn.execute(
        text("SELECT count(*) FROM unified_transaction_versions WHERE transaction_id = :t"),
        {"t": txn_id},
    ).scalar_one()


def test_0002_dedups_polluted_db_then_indexes():
    from alembic import command
    from sqlmodel import Session

    from app.core.enums import TransactionType
    from app.core.models.tables import State, UnifiedTransaction, UnifiedTransactionVersion
    from app.db_migrate import alembic_config, upgrade_head

    _drop_create(_TEST_DB)
    url = f"{_PG_BASE}/{_TEST_DB}"
    try:
        _ensure_state_schemas(url)
        _bootstrap_unmanaged_schema(url)
        eng = create_engine(url)

        def _txn(tid):
            return UnifiedTransaction(
                state_id=1, transaction_type=TransactionType.CONTRIBUTION, transaction_id=tid
            )

        with Session(eng) as s:
            s.add(State(id=1, code="TX", name="Texas"))
            s.commit()
            # survivor committed before doomed => survivor gets the lower id (it must survive).
            survivor = _txn("DUP1")
            s.add(survivor)
            s.commit()
            s.refresh(survivor)
            doomed = _txn("DUP1")
            s.add(doomed)
            s.commit()
            s.refresh(doomed)
            survivor_id, doomed_id = survivor.id, doomed.id
            # a child on each parent — the doomed one's child must be purged, the survivor's kept
            s.add(UnifiedTransactionVersion(transaction_id=doomed_id, version_number=1))
            s.add(UnifiedTransactionVersion(transaction_id=survivor_id, version_number=1))
            # a distinct non-dup row and a NULL-transaction_id row (both must survive untouched)
            non_dup = _txn("SOLO")
            null_tid = _txn(None)
            s.add(non_dup)
            s.add(null_tid)
            s.commit()
            s.refresh(non_dup)
            s.refresh(null_tid)
            non_dup_id, null_tid_id = non_dup.id, null_tid.id

        assert survivor_id < doomed_id  # sanity: survivor is the lowest id in the group
        with eng.connect() as c:
            assert _dup_group_count(c) == 1
        assert not _index_exists(url)  # legacy DB has no unique index yet

        # Mark the DB at baseline (existing schema present), then upgrade -> runs 0002.
        command.stamp(alembic_config(url), "0001_baseline")
        upgrade_head(url)

        with eng.connect() as c:
            assert _dup_group_count(c) == 0
            ids = {r[0] for r in c.execute(text("SELECT id FROM unified_transactions")).all()}
            assert {survivor_id, non_dup_id, null_tid_id} <= ids  # all survive
            assert doomed_id not in ids  # doomed purged
            assert _version_count(c, doomed_id) == 0  # doomed child purged
            assert _version_count(c, survivor_id) == 1  # survivor child kept
        assert _index_exists(url)

        # Idempotent: re-running upgrade is a no-op (already at head); dedup stays clean.
        upgrade_head(url)
        with eng.connect() as c:
            assert _dup_group_count(c) == 0

        # The unique index now forbids inserting a new duplicate of the surviving group.
        with pytest.raises(Exception):  # noqa: B017 — psycopg2 UniqueViolation wrapped by SQLAlchemy
            with Session(eng) as s:
                s.add(_txn("DUP1"))
                s.commit()
        eng.dispose()
    finally:
        _drop(_TEST_DB)


def test_0002_is_noop_on_fresh_db():
    """A fresh ``upgrade head`` (baseline THEN 0002) lands clean: index present, nothing to dedup."""
    from app.db_migrate import current_revision, upgrade_head

    _drop_create(_TEST_DB)
    url = f"{_PG_BASE}/{_TEST_DB}"
    try:
        _ensure_state_schemas(url)
        upgrade_head(url)  # baseline creates the index; 0002 finds 0 dups
        assert current_revision(url) == "0002_dedup_legacy_transactions"
        assert _index_exists(url)
        eng = create_engine(url)
        with eng.connect() as c:
            assert _dup_group_count(c) == 0
            assert c.execute(text("SELECT count(*) FROM unified_transactions")).scalar_one() == 0
        eng.dispose()
    finally:
        _drop(_TEST_DB)
