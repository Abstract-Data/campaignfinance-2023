"""Unit tests for ``write_frame``'s ``conflict_where`` predicate parameter.

These tests use an in-memory sqlite DB.  sqlite supports partial unique indexes
and ``ON CONFLICT (...) WHERE ... DO NOTHING``, but ONLY when the matching
partial index has been created first — hence the fixture below explicitly runs
the DDL before the test executes.  Without the index, sqlite raises:
    "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint".

All tests here are sqlite-safe (no Postgres required).
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlmodel import Session, SQLModel

import app.core.models  # noqa: F401 — register all SQLModel table classes


@pytest.fixture()
def session_with_dedup_indexes():
    """sqlite engine with tables + the partial unique index for unified_transactions."""
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uix_transactions_state_type_sourceid
                ON unified_transactions (state_id, transaction_type, transaction_id)
                WHERE transaction_id IS NOT NULL
                """
            )
        )
        conn.commit()
    with Session(engine) as session:
        yield session


def test_conflict_where_dedups_on_repeat(session_with_dedup_indexes):
    """Writing the same row twice with conflict_where must result in exactly one row."""
    import polars as pl

    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedTransaction

    session = session_with_dedup_indexes
    rows = pl.DataFrame(
        [
            {
                "state_id": 1,
                "transaction_type": "CONTRIBUTION",
                "transaction_id": "X1",
                "amount": 1,
            }
        ]
    )
    kw = dict(
        conflict_cols=["state_id", "transaction_type", "transaction_id"],
        update_cols=[],
        conflict_where="transaction_id IS NOT NULL",
    )
    common.write_frame(session, UnifiedTransaction, rows, **kw)
    common.write_frame(session, UnifiedTransaction, rows, **kw)  # identical repeat
    n = session.execute(text("SELECT count(*) FROM unified_transactions")).scalar()
    assert n == 1, f"expected 1 row after double-write, got {n}"


def test_conflict_where_null_excluded_from_dedup(session_with_dedup_indexes):
    """Rows with transaction_id=NULL are NOT covered by the partial unique index.
    Two NULL-id rows should both be written (the WHERE predicate excludes them).
    """
    import polars as pl

    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedTransaction

    session = session_with_dedup_indexes
    null_row = pl.DataFrame(
        [
            {
                "state_id": 1,
                "transaction_type": "CONTRIBUTION",
                "transaction_id": None,
                "amount": 2,
            }
        ]
    )
    kw = dict(
        conflict_cols=["state_id", "transaction_type", "transaction_id"],
        update_cols=[],
        conflict_where="transaction_id IS NOT NULL",
    )
    common.write_frame(session, UnifiedTransaction, null_row, **kw)
    common.write_frame(session, UnifiedTransaction, null_row, **kw)
    n = session.execute(text("SELECT count(*) FROM unified_transactions")).scalar()
    # Both rows land because the partial index only covers non-NULL transaction_id
    assert n == 2, f"expected 2 NULL-id rows (index does not cover NULLs), got {n}"


def test_conflict_where_in_batch_dup_absorbed(session_with_dedup_indexes):
    """A batch that contains the same row twice must produce only one row."""
    import polars as pl

    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedTransaction

    session = session_with_dedup_indexes
    rows = pl.DataFrame(
        [
            {
                "state_id": 1,
                "transaction_type": "CONTRIBUTION",
                "transaction_id": "BATCH_DUP",
                "amount": 3,
            },
            {
                "state_id": 1,
                "transaction_type": "CONTRIBUTION",
                "transaction_id": "BATCH_DUP",
                "amount": 3,
            },
        ]
    )
    kw = dict(
        conflict_cols=["state_id", "transaction_type", "transaction_id"],
        update_cols=[],
        conflict_where="transaction_id IS NOT NULL",
    )
    common.write_frame(session, UnifiedTransaction, rows, **kw)
    n = session.execute(text("SELECT count(*) FROM unified_transactions")).scalar()
    assert n == 1, f"expected 1 row from a same-key batch, got {n}"
