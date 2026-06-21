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


@pytest.fixture()
def dedup_engine_session():
    """Yields (engine, session) for tests that need both (e.g. id_maps key-frame reads)."""
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with engine.connect() as conn:
        for ddl in UnifiedDatabaseManager._DEDUP_INDEXES:
            conn.execute(text(ddl))
        conn.commit()
    with Session(engine) as session:
        yield engine, session


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


def test_persons_idempotent(dedup_engine_session: tuple) -> None:
    """Writing the same unified_persons rows twice (with Bucket C anti-join pre-filter)
    must not increase the row count (first-write-wins via person_key_frame + inline
    join_nulls=True anti-join).

    Simulates the pipeline: write once, then read person_key_frame from DB, filter the
    same candidate rows through the anti-join, write the filtered (empty) result, and
    assert the count is unchanged.
    """
    from app.core.ingest_vectorized import common
    from app.core.ingest_vectorized.id_maps import person_key_frame
    from app.core.models.tables import UnifiedPerson

    engine, s = dedup_engine_session
    state_id = 1

    person_rows = pl.DataFrame(
        {
            "first_name": ["Alice", None],
            "last_name": ["Smith", None],
            "middle_name": [None, None],
            "suffix": [None, None],
            "organization": [None, "Acme PAC"],
            "employer": [None, None],
            "occupation": [None, None],
            "job_title": [None, None],
            "person_type": ["INDIVIDUAL", "ORGANIZATION"],
            "dedup_addr_key": [None, None],
            "state_id": [state_id, state_id],
            "_pk_org": [None, "acme pac"],
            "_pk_fn": ["alice", None],
            "_pk_ln": ["smith", None],
            "_pk_addr": [None, None],
        },
        schema={
            "first_name": pl.Utf8,
            "last_name": pl.Utf8,
            "middle_name": pl.Utf8,
            "suffix": pl.Utf8,
            "organization": pl.Utf8,
            "employer": pl.Utf8,
            "occupation": pl.Utf8,
            "job_title": pl.Utf8,
            "person_type": pl.Utf8,
            "dedup_addr_key": pl.Utf8,
            "state_id": pl.Int64,
            "_pk_org": pl.Utf8,
            "_pk_fn": pl.Utf8,
            "_pk_ln": pl.Utf8,
            "_pk_addr": pl.Utf8,
        },
    )

    _person_write_cols = [
        "first_name",
        "last_name",
        "middle_name",
        "suffix",
        "organization",
        "employer",
        "occupation",
        "job_title",
        "person_type",
        "dedup_addr_key",
        "state_id",
    ]

    def _write_with_anti_join() -> None:
        existing = person_key_frame(engine, state_id)
        new_rows = person_rows.join(
            existing.select("_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"),
            on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
            how="anti",
            join_nulls=True,
        )
        out = new_rows.select(_person_write_cols)
        common.write_frame(s, UnifiedPerson, out, conflict_cols=None)

    _write_with_anti_join()
    before = _count(s, UnifiedPerson)
    assert before == 2

    _write_with_anti_join()  # identical second write — anti-join should filter all rows
    after = _count(s, UnifiedPerson)
    assert after == before, f"Person row count changed on re-write: {before} → {after}"


def test_addresses_idempotent(dedup_engine_session: tuple) -> None:
    """Writing the same unified_addresses rows twice (with Bucket C anti-join pre-filter)
    must not increase the row count (first-write-wins via address_key_frame +
    filter_new_rows).

    Simulates the pipeline: write once, then use address_key_frame + filter_new_rows to
    drop already-present rows, write the filtered (empty) result, assert count unchanged.
    """
    from app.core.ingest_vectorized import common
    from app.core.ingest_vectorized.id_maps import address_key_frame
    from app.core.models.tables import UnifiedAddress

    engine, s = dedup_engine_session

    addr_rows = pl.DataFrame(
        [
            {
                "street_1": "123 Main St",
                "street_2": None,
                "city": "Austin",
                "state": "TX",
                "zip_code": "78701",
                "country": None,
                "county": None,
            },
            {
                "street_1": "456 Oak Ave",
                "street_2": None,
                "city": "Dallas",
                "state": "TX",
                "zip_code": "75201",
                "country": None,
                "county": None,
            },
        ]
    )

    def _write_with_filter() -> None:
        existing = address_key_frame(engine)
        new_rows = common.filter_new_rows(
            addr_rows,
            existing,
            key_cols=["street_1", "city", "state", "zip_code"],
            normalize_lower=["street_1", "city", "state"],
        )
        common.write_frame(s, UnifiedAddress, new_rows, conflict_cols=None)

    _write_with_filter()
    before = _count(s, UnifiedAddress)
    assert before == 2

    _write_with_filter()  # identical second write — filter_new_rows should return 0 rows
    after = _count(s, UnifiedAddress)
    assert after == before, f"Address row count changed on re-write: {before} → {after}"
