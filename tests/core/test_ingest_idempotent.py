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


def test_campaigns_idempotent(dedup_session: Session) -> None:
    """Writing the same unified_campaigns rows twice must not increase row count
    (Bucket D: ON CONFLICT (normalized_name, primary_committee_id, election_year, state_id)
    WHERE primary_committee_id IS NOT NULL DO NOTHING, via uix_campaigns_identity).

    Requires dedup_session which applies _DEDUP_INDEXES (includes the partial index).
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedCampaign

    s = dedup_session
    rows = pl.DataFrame(
        [
            {
                "normalized_name": "smith for governor",
                "primary_committee_id": "C001",
                "election_year": 2024,
                "state_id": 1,
                "name": "Smith for Governor",
                "office_sought": "GOVERNOR",
                "district": None,
                "candidate_person_id": None,
            },
            {
                "normalized_name": "jones for senate",
                "primary_committee_id": "C002",
                "election_year": 2024,
                "state_id": 1,
                "name": "Jones for Senate",
                "office_sought": "SENATE",
                "district": None,
                "candidate_person_id": None,
            },
        ]
    )
    kw = dict(
        conflict_cols=["normalized_name", "primary_committee_id", "election_year", "state_id"],
        update_cols=[],
        conflict_where="primary_committee_id IS NOT NULL",
    )
    common.write_frame(s, UnifiedCampaign, rows, **kw)
    before = _count(s, UnifiedCampaign)

    common.write_frame(s, UnifiedCampaign, rows, **kw)  # identical second write
    after = _count(s, UnifiedCampaign)

    assert after == before, f"Campaign row count changed on re-write: {before} → {after}"


def test_campaigns_null_committee_excluded_from_dedup(dedup_session: Session) -> None:
    """Campaigns with primary_committee_id=NULL are NOT covered by the partial unique index
    (WHERE primary_committee_id IS NOT NULL), so two NULL-committee rows with the same
    name+year+state both land — the index does not constrain them.
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedCampaign

    s = dedup_session
    null_committee_row = pl.DataFrame(
        [
            {
                "normalized_name": "unknown campaign",
                "primary_committee_id": None,
                "election_year": 2024,
                "state_id": 1,
                "name": "Unknown Campaign",
                "office_sought": None,
                "district": None,
                "candidate_person_id": None,
            }
        ]
    )
    kw = dict(
        conflict_cols=["normalized_name", "primary_committee_id", "election_year", "state_id"],
        update_cols=[],
        conflict_where="primary_committee_id IS NOT NULL",
    )
    common.write_frame(s, UnifiedCampaign, null_committee_row, **kw)
    common.write_frame(s, UnifiedCampaign, null_committee_row, **kw)
    n = _count(s, UnifiedCampaign)
    assert n == 2, (
        f"expected 2 NULL-committee campaign rows (partial index does not cover NULLs), got {n}"
    )


def test_committee_persons_idempotent(dedup_session: Session) -> None:
    """Writing the same unified_committee_persons rows twice must not increase row count
    (Bucket A: ON CONFLICT (committee_id, person_id, role) DO NOTHING,
    via uix_committee_person_role).

    SQLite FK constraints are off by default, so dummy integer IDs are fine.
    """
    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedCommitteePerson

    s = dedup_session
    rows = pl.DataFrame(
        [
            {
                "committee_id": "C001",
                "person_id": 1,
                "role": "TREASURER",
                "entity_id": None,
                "state_id": 1,
                "start_date": None,
                "end_date": None,
                "is_active": True,
                "notes": None,
                "last_modified_by": None,
                "change_reason": None,
            },
            {
                "committee_id": "C001",
                "person_id": 2,
                "role": "CHAIR",
                "entity_id": None,
                "state_id": 1,
                "start_date": None,
                "end_date": None,
                "is_active": True,
                "notes": None,
                "last_modified_by": None,
                "change_reason": None,
            },
        ]
    )
    kw = dict(
        conflict_cols=["committee_id", "person_id", "role"],
        update_cols=[],
    )
    common.write_frame(s, UnifiedCommitteePerson, rows, **kw)
    before = _count(s, UnifiedCommitteePerson)

    common.write_frame(s, UnifiedCommitteePerson, rows, **kw)  # identical second write
    after = _count(s, UnifiedCommitteePerson)

    assert after == before, f"CommitteePerson row count changed on re-write: {before} → {after}"


def test_guarantors_idempotent(dedup_engine_session: tuple) -> None:
    """Writing the same loan_guarantors rows twice (with Bucket C anti-join pre-filter)
    must not increase the row count (first-write-wins).

    loan_id and debt_id are mutually exclusive (one is always NULL), so this path
    uses an inline anti-join with join_nulls=True rather than filter_new_rows (which
    does not expose a join_nulls parameter).

    Simulates the pipeline: write once, then read guarantor_key_frame from DB, filter
    the same candidate rows through the inline anti-join, write the filtered (empty)
    result, and assert the count is unchanged.
    """
    from app.core.ingest_vectorized import common
    from app.core.ingest_vectorized.id_maps import guarantor_key_frame
    from app.core.models import LoanGuarantor

    engine, s = dedup_engine_session

    _KEY_COLS = ["_k_loan", "_k_debt", "_k_last", "_k_first", "_k_org"]

    def _norm_keys(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.col("loan_id").alias("_k_loan"),
            pl.col("debt_id").alias("_k_debt"),
            pl.col("last_name").str.to_lowercase().alias("_k_last"),
            pl.col("first_name").str.to_lowercase().alias("_k_first"),
            pl.col("organization").str.to_lowercase().alias("_k_org"),
        )

    guarantor_rows = pl.DataFrame(
        [
            {
                "loan_id": 1,
                "debt_id": None,
                "entity_id": None,
                "position": 1,
                "person_type": "INDIVIDUAL",
                "organization": None,
                "last_name": "Smith",
                "first_name": "John",
                "suffix": None,
                "prefix": None,
                "city": "Austin",
                "state_code": "TX",
                "county": None,
                "country": None,
                "postal_code": "78701",
                "region": None,
            },
            {
                "loan_id": None,
                "debt_id": 2,
                "entity_id": None,
                "position": 1,
                "person_type": "INDIVIDUAL",
                "organization": None,
                "last_name": "Jones",
                "first_name": "Jane",
                "suffix": None,
                "prefix": None,
                "city": "Dallas",
                "state_code": "TX",
                "county": None,
                "country": None,
                "postal_code": "75201",
                "region": None,
            },
        ],
        schema={
            "loan_id": pl.Int64,
            "debt_id": pl.Int64,
            "entity_id": pl.Int64,
            "position": pl.Int64,
            "person_type": pl.Utf8,
            "organization": pl.Utf8,
            "last_name": pl.Utf8,
            "first_name": pl.Utf8,
            "suffix": pl.Utf8,
            "prefix": pl.Utf8,
            "city": pl.Utf8,
            "state_code": pl.Utf8,
            "county": pl.Utf8,
            "country": pl.Utf8,
            "postal_code": pl.Utf8,
            "region": pl.Utf8,
        },
    )

    def _write_with_anti_join() -> None:
        existing = guarantor_key_frame(engine)
        existing_norm = _norm_keys(existing).select(_KEY_COLS).unique()
        rows_normed = _norm_keys(guarantor_rows).unique(subset=_KEY_COLS, keep="first")
        # join_nulls=True required: loan_id or debt_id is always NULL per row
        new_rows = rows_normed.join(existing_norm, on=_KEY_COLS, how="anti", join_nulls=True).drop(
            _KEY_COLS
        )
        if new_rows.height:
            common.write_frame(s, LoanGuarantor, new_rows, conflict_cols=None)

    _write_with_anti_join()
    before = _count(s, LoanGuarantor)
    assert before == 2

    _write_with_anti_join()  # identical second write — anti-join should filter all rows
    after = _count(s, LoanGuarantor)
    assert after == before, f"Guarantor row count changed on re-write: {before} → {after}"


def test_full_pipeline_idempotent(dedup_engine_session: tuple) -> None:
    """End-to-end first-write-wins contract across all four idempotency buckets.

    Writes a multi-record fixture that covers every mechanism introduced by the
    upsert-all-records plan, then takes a snapshot of all unified_* tables and the
    ingest_errors count.  A second identical write must leave the snapshot and error
    count unchanged.

    Buckets exercised:
    - Bucket A (ON CONFLICT DO NOTHING, full key):
        unified_contributions, unified_expenditures,
        unified_transaction_persons, unified_committee_persons
    - Bucket B (ON CONFLICT ... WHERE pred DO NOTHING, partial index):
        unified_transactions (WHERE transaction_id IS NOT NULL),
        unified_entities (WHERE state_id IS NOT NULL),
        unified_campaigns (WHERE primary_committee_id IS NOT NULL)
    - Bucket C (filter_new_rows / anti-join, functional dedup key):
        unified_persons (person_key_frame anti-join),
        unified_addresses (address_key_frame + filter_new_rows)
    """
    from app.core.ingest_equivalence import diff_snapshots, snapshot_unified
    from app.core.ingest_vectorized import common
    from app.core.ingest_vectorized.id_maps import address_key_frame, person_key_frame
    from app.core.models.tables import (
        IngestError,
        UnifiedAddress,
        UnifiedCampaign,
        UnifiedCommitteePerson,
        UnifiedContribution,
        UnifiedEntity,
        UnifiedExpenditure,
        UnifiedPerson,
        UnifiedTransaction,
        UnifiedTransactionPerson,
    )

    engine, s = dedup_engine_session
    state_id = 1

    # ── Bucket B: transactions ────────────────────────────────────────────────
    txn_rows = pl.DataFrame(
        [
            {
                "state_id": state_id,
                "transaction_type": "CONTRIBUTION",
                "transaction_id": "E2E-TXN-001",
                "amount": 1000,
            },
            {
                "state_id": state_id,
                "transaction_type": "EXPENDITURE",
                "transaction_id": "E2E-TXN-002",
                "amount": 500,
            },
        ]
    )
    txn_kw = dict(
        conflict_cols=["state_id", "transaction_type", "transaction_id"],
        update_cols=[],
        conflict_where="transaction_id IS NOT NULL",
    )

    # ── Bucket B: entities ────────────────────────────────────────────────────
    entity_rows = pl.DataFrame(
        [
            {
                "entity_type": "PERSON",
                "normalized_name": "e2e alice smith",
                "state_id": state_id,
            },
            {
                "entity_type": "ORGANIZATION",
                "normalized_name": "e2e donors pac",
                "state_id": state_id,
            },
        ]
    )
    entity_kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )

    # ── Bucket C: persons (anti-join via person_key_frame) ────────────────────
    _PERSON_KEY_COLS = ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]
    _PERSON_WRITE_COLS = [
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
    person_rows = pl.DataFrame(
        {
            "first_name": ["Alice", None],
            "last_name": ["Smith", None],
            "middle_name": [None, None],
            "suffix": [None, None],
            "organization": [None, "E2E Donors PAC"],
            "employer": [None, None],
            "occupation": [None, None],
            "job_title": [None, None],
            "person_type": ["INDIVIDUAL", "ORGANIZATION"],
            "dedup_addr_key": [None, None],
            "state_id": [state_id, state_id],
            "_pk_org": [None, "e2e donors pac"],
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

    # ── Bucket C: addresses (filter_new_rows via address_key_frame) ───────────
    addr_rows = pl.DataFrame(
        [
            {
                "street_1": "100 Congress Ave",
                "street_2": None,
                "city": "Austin",
                "state": "TX",
                "zip_code": "78701",
                "country": None,
                "county": None,
            },
            {
                "street_1": "200 Commerce St",
                "street_2": None,
                "city": "Dallas",
                "state": "TX",
                "zip_code": "75201",
                "country": None,
                "county": None,
            },
        ]
    )

    # ── Bucket A: contributions ───────────────────────────────────────────────
    contrib_rows = pl.DataFrame(
        [{"transaction_id": 101, "contributor_entity_id": 1, "recipient_entity_id": 2}]
    )
    contrib_kw = dict(conflict_cols=["transaction_id"], update_cols=[])

    # ── Bucket A: expenditures ────────────────────────────────────────────────
    expend_rows = pl.DataFrame(
        [{"transaction_id": 102, "payer_entity_id": 2, "payee_entity_id": 3}]
    )
    expend_kw = dict(conflict_cols=["transaction_id"], update_cols=[])

    # ── Bucket A: transaction_persons ─────────────────────────────────────────
    txn_person_rows = pl.DataFrame(
        [{"transaction_id": 101, "person_id": 10, "role": "CONTRIBUTOR"}]
    )
    txn_person_kw = dict(
        conflict_cols=["transaction_id", "person_id", "role"],
        update_cols=[],
    )

    # ── Bucket D: campaigns (partial index dedup) ─────────────────────────────
    campaign_rows = pl.DataFrame(
        [
            {
                "normalized_name": "e2e smith for governor",
                "primary_committee_id": "E2E-C001",
                "election_year": 2024,
                "state_id": state_id,
                "name": "E2E Smith for Governor",
                "office_sought": "GOVERNOR",
                "district": None,
                "candidate_person_id": None,
            },
            {
                "normalized_name": "e2e jones for senate",
                "primary_committee_id": "E2E-C002",
                "election_year": 2024,
                "state_id": state_id,
                "name": "E2E Jones for Senate",
                "office_sought": "SENATE",
                "district": None,
                "candidate_person_id": None,
            },
        ]
    )
    campaign_kw = dict(
        conflict_cols=[
            "normalized_name",
            "primary_committee_id",
            "election_year",
            "state_id",
        ],
        update_cols=[],
        conflict_where="primary_committee_id IS NOT NULL",
    )

    # ── Bucket A: committee_persons ───────────────────────────────────────────
    committee_person_rows = pl.DataFrame(
        [
            {
                "committee_id": "E2E-C001",
                "person_id": 10,
                "role": "TREASURER",
                "entity_id": None,
                "state_id": state_id,
                "start_date": None,
                "end_date": None,
                "is_active": True,
                "notes": None,
                "last_modified_by": None,
                "change_reason": None,
            },
        ]
    )
    committee_person_kw = dict(
        conflict_cols=["committee_id", "person_id", "role"],
        update_cols=[],
    )

    def _write_all_families() -> None:
        """One full pass through every family write path."""
        # Bucket B
        common.write_frame(s, UnifiedTransaction, txn_rows, **txn_kw)
        common.write_frame(s, UnifiedEntity, entity_rows, **entity_kw)

        # Bucket C — persons
        existing_persons = person_key_frame(engine, state_id)
        new_persons = person_rows.join(
            existing_persons.select(_PERSON_KEY_COLS),
            on=_PERSON_KEY_COLS,
            how="anti",
            join_nulls=True,
        )
        if new_persons.height:
            common.write_frame(
                s,
                UnifiedPerson,
                new_persons.select(_PERSON_WRITE_COLS),
                conflict_cols=None,
            )

        # Bucket C — addresses
        existing_addrs = address_key_frame(engine)
        new_addrs = common.filter_new_rows(
            addr_rows,
            existing_addrs,
            key_cols=["street_1", "city", "state", "zip_code"],
            normalize_lower=["street_1", "city", "state"],
        )
        if new_addrs.height:
            common.write_frame(s, UnifiedAddress, new_addrs, conflict_cols=None)

        # Bucket A
        common.write_frame(s, UnifiedContribution, contrib_rows, **contrib_kw)
        common.write_frame(s, UnifiedExpenditure, expend_rows, **expend_kw)
        common.write_frame(s, UnifiedTransactionPerson, txn_person_rows, **txn_person_kw)
        common.write_frame(s, UnifiedCommitteePerson, committee_person_rows, **committee_person_kw)

        # Bucket D
        common.write_frame(s, UnifiedCampaign, campaign_rows, **campaign_kw)

    # ── First pass ────────────────────────────────────────────────────────────
    _write_all_families()

    before_snap = snapshot_unified(engine)
    before_errors = _count(s, IngestError)

    # Sanity: fixture must have produced rows in core tables.
    assert before_snap.get("unified_transactions"), "no transactions in snapshot after first write"
    assert before_snap.get("unified_entities"), "no entities in snapshot after first write"
    assert before_snap.get("unified_persons"), "no persons in snapshot after first write"
    assert before_snap.get("unified_addresses"), "no addresses in snapshot after first write"
    assert before_snap.get("unified_campaigns"), "no campaigns in snapshot after first write"

    # ── Second (identical) pass ───────────────────────────────────────────────
    _write_all_families()

    after_snap = snapshot_unified(engine)
    after_errors = _count(s, IngestError)

    # ingest_errors must not grow (write_frame never logs ingest errors; this
    # asserts the contract does not silently degrade into error rows).
    assert after_errors == before_errors, (
        f"ingest_errors count changed on re-write: {before_errors} → {after_errors}"
    )

    diffs = diff_snapshots(before_snap, after_snap)
    assert diffs == [], (
        "Snapshot changed after identical re-ingest — first-write-wins contract violated:\n"
        + "\n".join(diffs)
    )
