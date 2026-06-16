"""Tests for the schema-drift / no-migration fix (finding #1, 2026-06-14).

`create_all` only creates *missing tables*; it never adds a column to a
pre-existing one.  The additive-column shims (unified + resolve) close that gap so
columns added to a model after a table already exists in a deployed DB are applied
idempotently rather than breaking inserts with UndefinedColumn.
"""

from __future__ import annotations

from sqlalchemy import create_engine, inspect, text

from app.core.unified_database import (
    _UNIFIED_ADDITIVE_COLUMNS,
    ensure_unified_additive_columns,
)
from app.resolve.run import _ADDITIVE_COLUMNS, _ensure_additive_columns


def _cols(engine, table: str) -> set[str]:
    return {c["name"] for c in inspect(engine).get_columns(table)}


def test_unified_shim_adds_missing_report_columns():
    """A stale unified_reports (created before wave-2) gains the at-filing cols."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        # Pre-wave-2 shape: table exists WITHOUT the new columns.
        conn.execute(text("CREATE TABLE unified_reports (id INTEGER PRIMARY KEY)"))

    ensure_unified_additive_columns(engine)

    cols = _cols(engine, "unified_reports")
    assert "committee_name_at_filing" in cols
    assert "treasurer_name_at_filing" in cols


def test_unified_shim_is_idempotent():
    """Re-running the shim on an already-migrated table is a no-op (no error)."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE unified_reports (id INTEGER PRIMARY KEY)"))
    ensure_unified_additive_columns(engine)
    # Second call must not raise (duplicate-column ALTER would error).
    ensure_unified_additive_columns(engine)
    assert "committee_name_at_filing" in _cols(engine, "unified_reports")


def test_unified_shim_noop_when_table_absent():
    """No target table -> no-op (does not create the table or raise)."""
    engine = create_engine("sqlite://")
    ensure_unified_additive_columns(engine)
    assert "unified_reports" not in set(inspect(engine).get_table_names())


def test_unified_additive_columns_registry():
    """The two wave-2 report columns are registered for migration."""
    registered = {(t, c) for t, c, _ddl in _UNIFIED_ADDITIVE_COLUMNS}
    assert ("unified_reports", "committee_name_at_filing") in registered
    assert ("unified_reports", "treasurer_name_at_filing") in registered


def test_resolve_shim_adds_canonical_entity_employer():
    """A stale canonical_entity (created before wave-3) gains the employer col."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE canonical_entity (id INTEGER PRIMARY KEY)"))

    _ensure_additive_columns(engine)

    assert "employer" in _cols(engine, "canonical_entity")
    # idempotent
    _ensure_additive_columns(engine)


def test_resolve_additive_registry_includes_employer():
    registered = {(t, c) for t, c, _ddl in _ADDITIVE_COLUMNS}
    assert ("canonical_entity", "employer") in registered
