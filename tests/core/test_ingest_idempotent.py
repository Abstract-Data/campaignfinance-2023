"""Idempotency contract tests for vectorized ingest write paths.

Each test verifies first-write-wins behaviour: writing the same rows twice
results in the same row count as writing them once.

Fixture approach: in-memory sqlite DB with all SQLModel tables created and the
dedup unique indexes applied (required for partial-index ON CONFLICT and for
the transaction_persons composite unique).  SQLite does not enforce FK
constraints by default, so rows referencing other tables use dummy integer IDs.
"""

from __future__ import annotations

import polars as pl
import pytest
from sqlalchemy import create_engine, func, select, text
from sqlmodel import Session, SQLModel

import app.core.models  # noqa: F401 — register all SQLModel table classes
from app.core.unified_database import UnifiedDatabaseManager


@pytest.fixture()
def dedup_session():
    """sqlite engine with all tables and dedup unique indexes applied."""
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with engine.connect() as conn:
        for ddl in UnifiedDatabaseManager._DEDUP_INDEXES:
            conn.execute(text(ddl))
        conn.commit()
    with Session(engine) as session:
        yield session


def _count(session: Session, model) -> int:
    return session.execute(select(func.count()).select_from(model)).scalar()


def test_children_idempotent(dedup_session: Session) -> None:
    """Writing the same subtype-child and transaction_person rows twice must
    not increase the row count (first-write-wins via Bucket A ON CONFLICT DO NOTHING).

    Partial tables covered: unified_contributions, unified_expenditures,
    unified_loans, unified_transaction_persons.
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import (
        UnifiedContribution,
        UnifiedExpenditure,
        UnifiedLoan,
        UnifiedTransactionPerson,
    )

    s = dedup_session

    contrib_rows = pl.DataFrame(
        [{"transaction_id": 1, "contributor_entity_id": 10, "recipient_entity_id": 20}]
    )
    expend_rows = pl.DataFrame(
        [{"transaction_id": 2, "payer_entity_id": 10, "payee_entity_id": 30}]
    )
    loan_rows = pl.DataFrame(
        [{"transaction_id": 3, "lender_entity_id": 10, "borrower_entity_id": 40}]
    )
    txn_person_rows = pl.DataFrame([{"transaction_id": 1, "person_id": 100, "role": "CONTRIBUTOR"}])

    def _write_all() -> None:
        common.write_frame(
            s,
            UnifiedContribution,
            contrib_rows,
            conflict_cols=["transaction_id"],
            update_cols=[],
        )
        common.write_frame(
            s,
            UnifiedExpenditure,
            expend_rows,
            conflict_cols=["transaction_id"],
            update_cols=[],
        )
        common.write_frame(
            s,
            UnifiedLoan,
            loan_rows,
            conflict_cols=["transaction_id"],
            update_cols=[],
        )
        common.write_frame(
            s,
            UnifiedTransactionPerson,
            txn_person_rows,
            conflict_cols=["transaction_id", "person_id", "role"],
            update_cols=[],
        )

    _write_all()

    before = {
        "contributions": _count(s, UnifiedContribution),
        "expenditures": _count(s, UnifiedExpenditure),
        "loans": _count(s, UnifiedLoan),
        "transaction_persons": _count(s, UnifiedTransactionPerson),
    }

    _write_all()  # identical second write

    after = {
        "contributions": _count(s, UnifiedContribution),
        "expenditures": _count(s, UnifiedExpenditure),
        "loans": _count(s, UnifiedLoan),
        "transaction_persons": _count(s, UnifiedTransactionPerson),
    }

    assert after == before, f"Row counts changed on re-write: {before} → {after}"


def test_transactions_idempotent(dedup_session: Session) -> None:
    """Writing the same unified_transactions rows twice must not increase row count
    (Bucket B: ON CONFLICT (state_id, transaction_type, transaction_id) WHERE
    transaction_id IS NOT NULL DO NOTHING, via uix_transactions_state_type_sourceid).
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedTransaction

    s = dedup_session
    rows = pl.DataFrame(
        [
            {
                "state_id": 1,
                "transaction_type": "CONTRIBUTION",
                "transaction_id": "TXN-001",
                "amount": 500,
            },
            {
                "state_id": 1,
                "transaction_type": "EXPENDITURE",
                "transaction_id": "TXN-002",
                "amount": 250,
            },
        ]
    )
    kw = dict(
        conflict_cols=["state_id", "transaction_type", "transaction_id"],
        update_cols=[],
        conflict_where="transaction_id IS NOT NULL",
    )
    common.write_frame(s, UnifiedTransaction, rows, **kw)
    before = _count(s, UnifiedTransaction)

    common.write_frame(s, UnifiedTransaction, rows, **kw)  # identical second write
    after = _count(s, UnifiedTransaction)

    assert after == before, f"Transaction row count changed on re-write: {before} → {after}"


def test_entities_idempotent(dedup_session: Session) -> None:
    """Writing the same unified_entities rows twice must not increase row count
    (Bucket B: ON CONFLICT (entity_type, normalized_name, state_id) WHERE
    state_id IS NOT NULL DO NOTHING, via uix_entities_type_name_state).
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedEntity

    s = dedup_session
    rows = pl.DataFrame(
        [
            {"entity_type": "PERSON", "normalized_name": "john smith", "state_id": 1},
            {"entity_type": "ORGANIZATION", "normalized_name": "acme pac", "state_id": 1},
        ]
    )
    kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )
    common.write_frame(s, UnifiedEntity, rows, **kw)
    before = _count(s, UnifiedEntity)

    common.write_frame(s, UnifiedEntity, rows, **kw)  # identical second write
    after = _count(s, UnifiedEntity)

    assert after == before, f"Entity row count changed on re-write: {before} → {after}"


def test_entities_null_state_excluded_from_dedup(dedup_session: Session) -> None:
    """Entities with state_id=NULL are NOT covered by the partial unique index
    (WHERE state_id IS NOT NULL), so two NULL-state entity rows with the same
    type+name both land — the index does not constrain them.
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedEntity

    s = dedup_session
    null_state_row = pl.DataFrame(
        [{"entity_type": "PERSON", "normalized_name": "stateless donor", "state_id": None}]
    )
    kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )
    common.write_frame(s, UnifiedEntity, null_state_row, **kw)
    common.write_frame(s, UnifiedEntity, null_state_row, **kw)
    n = _count(s, UnifiedEntity)
    assert n == 2, (
        f"expected 2 NULL-state entity rows (partial index does not cover NULLs), got {n}"
    )


def test_entities_batch_dup_absorbed(dedup_session: Session) -> None:
    """A batch containing the same entity key twice must produce only one row."""
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedEntity

    s = dedup_session
    rows = pl.DataFrame(
        [
            {"entity_type": "PERSON", "normalized_name": "batch dup person", "state_id": 1},
            {"entity_type": "PERSON", "normalized_name": "batch dup person", "state_id": 1},
        ]
    )
    kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )
    common.write_frame(s, UnifiedEntity, rows, **kw)
    n = _count(s, UnifiedEntity)
    assert n == 1, f"expected 1 entity from a same-key batch, got {n}"
