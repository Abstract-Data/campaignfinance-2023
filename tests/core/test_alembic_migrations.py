"""Alembic baseline must reproduce the create_all bootstrap schema, and `cf migrate` must be
idempotent. PG-gated (the dedup indexes + the bootstrap path are Postgres-specific).
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, inspect

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")


def _pg_available() -> bool:
    try:
        with create_engine(f"{_PG_BASE}/postgres").connect():
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


def _drop_create(name: str) -> None:
    from psycopg2 import sql

    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        cur.close()
    finally:
        raw.close()
        admin.dispose()


def _drop(name: str) -> None:
    from psycopg2 import sql

    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        cur.close()
    finally:
        raw.close()
        admin.dispose()


def _ensure_state_schemas(url: str) -> None:
    """Create the state schemas so create_all succeeds even when an earlier test has imported
    the schema-qualified state-validator models (schema='texas'/'oklahoma') into the shared
    SQLModel.metadata — a documented cross-test pollution. No-op in isolation (those tables
    aren't in metadata then). Mirrors what a real multi-state DB would have."""
    from sqlalchemy import text

    eng = create_engine(url)
    try:
        with eng.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS texas"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS oklahoma"))
    finally:
        eng.dispose()


def _fingerprint(url: str) -> dict[str, tuple[frozenset, frozenset]]:
    """{table: (column-names, index-names)} — excludes Alembic's own version table."""
    insp = inspect(create_engine(url))
    out: dict[str, tuple[frozenset, frozenset]] = {}
    for t in insp.get_table_names():
        if t == "alembic_version":
            continue
        cols = frozenset(c["name"] for c in insp.get_columns(t))
        idx = frozenset(i["name"] for i in insp.get_indexes(t))
        out[t] = (cols, idx)
    return out


def test_alembic_baseline_matches_bootstrap():
    """`alembic upgrade head` on a fresh DB == production_loader._get_session bootstrap
    (create_all + Fix-7 dedup indexes), table-for-table and column/index-for-index."""
    from app.db_migrate import upgrade_head
    from scripts.loaders.production_loader import _get_session

    _drop_create("cf_alembic_test_a")
    _drop_create("cf_alembic_test_b")
    try:
        url_a = f"{_PG_BASE}/cf_alembic_test_a"
        url_b = f"{_PG_BASE}/cf_alembic_test_b"
        _ensure_state_schemas(url_a)
        _ensure_state_schemas(url_b)
        upgrade_head(url_a)
        _get_session(url_b).close()
        assert _fingerprint(url_a) == _fingerprint(url_b)
    finally:
        _drop("cf_alembic_test_a")
        _drop("cf_alembic_test_b")


def test_migrate_is_idempotent():
    """Running migrate twice leaves the DB at head with no error (second run is a no-op)."""
    from app.db_migrate import current_revision, upgrade_head

    _drop_create("cf_alembic_test_idem")
    try:
        url = f"{_PG_BASE}/cf_alembic_test_idem"
        _ensure_state_schemas(url)
        assert current_revision(url) is None
        upgrade_head(url)
        rev = current_revision(url)
        assert rev == "0002_dedup_legacy_transactions"  # latest head (baseline -> dedup)
        upgrade_head(url)  # no-op
        assert current_revision(url) == rev
    finally:
        _drop("cf_alembic_test_idem")
