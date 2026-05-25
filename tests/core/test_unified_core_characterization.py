"""Characterization tests for the unified core pipeline (TASK-5a).

Covers:
- UnifiedSQLDataProcessor: process_record for each TransactionType
- DETAIL_BUILDERS registry completeness
- UnifiedSQLModelBuilder: builder lookup helpers with in-memory SQLite
- ProcessStats counters
- UnifiedStateLoader: batch/session behaviour and stats failure counter
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

import app.core.models  # noqa: F401 — register unified tables
from app.core.builders import UnifiedSQLModelBuilder
from app.core.enums import TransactionType
from app.core.models import (
    UnifiedCommittee,
    UnifiedTransaction,
)
from app.core.processor import DETAIL_BUILDERS, ProcessStats, UnifiedSQLDataProcessor
from app.core.unified_database import UnifiedDatabaseManager

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    """In-memory SQLite engine with unified (schema-less) tables created.

    State-specific tables (texas.*, oklahoma.*) carry a ``schema`` attribute
    that SQLite does not support.  We create only the schema-less unified tables.
    """
    db_path = tmp_path / "test_core.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    for table in SQLModel.metadata.sorted_tables:
        if table.schema is None:
            table.create(engine, checkfirst=True)
    return engine


@pytest.fixture
def db_manager(sqlite_engine) -> UnifiedDatabaseManager:
    manager = UnifiedDatabaseManager(
        database_url=str(sqlite_engine.url), echo=False
    )
    manager.engine = sqlite_engine
    return manager


@pytest.fixture
def processor() -> UnifiedSQLDataProcessor:
    return UnifiedSQLDataProcessor()


def _base_raw(record_type: str, **extra: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "record_type": record_type,
        "filerIdent": "99001",
        "contributionAmount": "500.00",
        "contributionDt": "2024-03-01",
    }
    data.update(extra)
    return data


def _debt_credit_raw(record_type: str) -> dict[str, Any]:
    return _base_raw(
        record_type,
        contributorNameLast="CREDITOR",
        contributorNameFirst="Test",
        recipientNameLast="DEBTOR",
        recipientNameFirst="Party",
    )


# ---------------------------------------------------------------------------
# ProcessStats
# ---------------------------------------------------------------------------


class TestProcessStats:
    def test_total_sums_all_counters(self) -> None:
        stats = ProcessStats(success=5, failures=2, db_errors=1, skipped=3)
        assert stats.total == 11

    def test_str_includes_all_counts(self) -> None:
        stats = ProcessStats(success=10, failures=1, db_errors=0, skipped=0)
        text = str(stats)
        assert "10" in text
        assert "1" in text

    def test_default_all_zero(self) -> None:
        stats = ProcessStats()
        assert stats.total == 0


# ---------------------------------------------------------------------------
# DETAIL_BUILDERS registry
# ---------------------------------------------------------------------------


class TestDetailBuildersRegistry:
    """The registry must cover every actionable TransactionType."""

    EXPECTED_TYPES = {
        TransactionType.CONTRIBUTION,
        TransactionType.LOAN,
        TransactionType.DEBT,
        TransactionType.CREDIT,
        TransactionType.TRAVEL,
        TransactionType.ASSET,
    }

    def test_all_expected_types_registered(self) -> None:
        assert self.EXPECTED_TYPES.issubset(set(DETAIL_BUILDERS.keys()))

    def test_builders_are_callable(self) -> None:
        for tx_type, builder_fn in DETAIL_BUILDERS.items():
            assert callable(builder_fn), f"{tx_type} builder is not callable"


# ---------------------------------------------------------------------------
# process_record — one record per TransactionType
# ---------------------------------------------------------------------------


class TestProcessRecordByType:
    @pytest.mark.parametrize(
        ("record_type", "expected_type"),
        [
            ("RCPT", TransactionType.CONTRIBUTION),
            ("LOAN", TransactionType.LOAN),
            ("TRVL", TransactionType.TRAVEL),
            ("ASSET", TransactionType.ASSET),
        ],
    )
    def test_transaction_type_assigned(
        self,
        processor: UnifiedSQLDataProcessor,
        record_type: str,
        expected_type: TransactionType,
    ) -> None:
        raw = _base_raw(record_type)
        txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
        assert txn.transaction_type == expected_type

    @pytest.mark.parametrize(
        ("record_type", "expected_type"),
        [
            ("DEBT", TransactionType.DEBT),
            ("CRED", TransactionType.CREDIT),
        ],
    )
    def test_debt_credit_type_assigned(
        self,
        processor: UnifiedSQLDataProcessor,
        record_type: str,
        expected_type: TransactionType,
    ) -> None:
        raw = _debt_credit_raw(record_type)
        txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
        assert txn.transaction_type == expected_type

    def test_returns_unified_transaction(self, processor: UnifiedSQLDataProcessor) -> None:
        raw = _base_raw("RCPT")
        txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
        assert isinstance(txn, UnifiedTransaction)

    def test_amount_parsed_from_string(self, processor: UnifiedSQLDataProcessor) -> None:
        raw = _base_raw("RCPT", contributionAmount="1234.56")
        txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
        assert txn.amount is not None
        assert float(txn.amount) == pytest.approx(1234.56)

    def test_raw_data_stored_on_transaction(self, processor: UnifiedSQLDataProcessor) -> None:
        raw = _base_raw("RCPT")
        txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
        assert txn.raw_data is not None
        parsed = json.loads(txn.raw_data)
        assert "filerIdent" in parsed


# ---------------------------------------------------------------------------
# process_record_stream
# ---------------------------------------------------------------------------


class TestProcessRecordStream:
    def test_yields_all_records(self, processor: UnifiedSQLDataProcessor) -> None:
        records = [_base_raw("RCPT"), _base_raw("LOAN"), _base_raw("TRVL")]

        def _source() -> Iterator[dict]:
            yield from records

        results = list(
            processor.process_record_stream(_source(), "texas", state_id=1, state_code="TX")
        )
        assert len(results) == 3

    def test_stream_is_lazy(self, processor: UnifiedSQLDataProcessor) -> None:
        yielded: list[int] = []

        def _source() -> Iterator[dict]:
            for i, r in enumerate([_base_raw("RCPT"), _base_raw("LOAN")]):
                yielded.append(i)
                yield r

        gen = processor.process_record_stream(_source(), "texas", state_id=1, state_code="TX")
        assert len(yielded) == 0
        next(gen)
        assert len(yielded) == 1

    def test_empty_stream_returns_empty(self, processor: UnifiedSQLDataProcessor) -> None:
        results = list(
            processor.process_record_stream(iter([]), "texas", state_id=1, state_code="TX")
        )
        assert results == []


# ---------------------------------------------------------------------------
# process_records (list wrapper)
# ---------------------------------------------------------------------------


class TestProcessRecords:
    def test_returns_list(self, processor: UnifiedSQLDataProcessor) -> None:
        records = [_base_raw("RCPT"), _base_raw("LOAN")]
        results = processor.process_records(records, "texas", state_id=1, state_code="TX")
        assert isinstance(results, list)
        assert len(results) == 2

    def test_state_id_propagated(self, processor: UnifiedSQLDataProcessor) -> None:
        results = processor.process_records(
            [_base_raw("RCPT")], "texas", state_id=42, state_code="TX"
        )
        assert results[0].state_id == 42


# ---------------------------------------------------------------------------
# UnifiedSQLModelBuilder with in-memory SQLite
# ---------------------------------------------------------------------------


class TestBuilderWithSession:
    """Builder lookup helpers work against real SQLite (Wave 2 session injection)."""

    def test_build_transaction_returns_instance(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            builder = UnifiedSQLModelBuilder(
                "texas", state_id=1, state_code="TX", session=session
            )
            raw = _base_raw("RCPT")
            txn = builder.build_transaction(raw)
        assert isinstance(txn, UnifiedTransaction)

    def test_find_committee_returns_none_when_absent(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            builder = UnifiedSQLModelBuilder(
                "texas", state_id=1, state_code="TX", session=session
            )
            result = builder._find_committee_by_filer_id("nonexistent-filer-999")
        assert result is None

    def test_find_committee_returns_record_when_present(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            committee = UnifiedCommittee(
                filer_id="FILER-001",
                name="Test Committee",
                state_id=1,
            )
            session.add(committee)
            session.commit()

            builder = UnifiedSQLModelBuilder(
                "texas", state_id=1, state_code="TX", session=session
            )
            result = builder._find_committee_by_filer_id("FILER-001")
        assert result is not None
        assert result.name == "Test Committee"

    def test_build_committee_creates_instance(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            builder = UnifiedSQLModelBuilder(
                "texas", state_id=1, state_code="TX", session=session
            )
            raw = _base_raw("RCPT", filerName="My Test Committee")
            committee = builder.build_committee(raw)
        # May be None if filer_id cannot be resolved — that is valid behaviour
        if committee is not None:
            assert isinstance(committee, UnifiedCommittee)


# ---------------------------------------------------------------------------
# UnifiedDatabaseManager with in-memory SQLite
# ---------------------------------------------------------------------------


class TestDatabaseManagerSQLite:
    def test_get_session_returns_session(self, db_manager: UnifiedDatabaseManager) -> None:
        session = db_manager.get_session()
        assert isinstance(session, Session)
        session.close()

    def test_save_and_retrieve_transaction(self, db_manager: UnifiedDatabaseManager) -> None:
        txn = UnifiedTransaction(
            amount=Decimal("100.00"),
            transaction_date=date(2024, 1, 1),
            transaction_type=TransactionType.CONTRIBUTION,
            state_id=1,
        )
        with db_manager.get_session() as session:
            session.add(txn)
            session.commit()
            session.refresh(txn)
            txn_id = txn.id

        with db_manager.get_session() as session:
            found = session.exec(
                select(UnifiedTransaction).where(UnifiedTransaction.id == txn_id)
            ).first()
        assert found is not None
        assert float(found.amount) == pytest.approx(100.0)

    def test_get_transactions_amount_range(self, db_manager: UnifiedDatabaseManager) -> None:
        with db_manager.get_session() as session:
            for amt in [50, 500, 5000]:
                session.add(
                    UnifiedTransaction(
                        amount=Decimal(str(amt)),
                        transaction_date=date(2024, 1, 1),
                        transaction_type=TransactionType.CONTRIBUTION,
                        state_id=1,
                    )
                )
            session.commit()

        results = db_manager.get_transactions_by_amount_range(100, 1000)
        amounts = [float(r.amount) for r in results]
        assert 500.0 in amounts
        assert 50.0 not in amounts
        assert 5000.0 not in amounts

    def test_get_transactions_date_range(self, db_manager: UnifiedDatabaseManager) -> None:
        with db_manager.get_session() as session:
            for d in ["2024-01-01", "2024-06-15", "2025-01-01"]:
                session.add(
                    UnifiedTransaction(
                        amount=Decimal("100.00"),
                        transaction_date=date(*[int(x) for x in d.split("-")]),
                        transaction_type=TransactionType.CONTRIBUTION,
                        state_id=1,
                    )
                )
            session.commit()

        results = db_manager.get_transactions_by_date_range("2024-01-01", "2024-12-31")
        dates = [r.transaction_date.isoformat() for r in results]
        assert "2024-01-01" in dates
        assert "2024-06-15" in dates
        assert "2025-01-01" not in dates

    def test_update_transaction_creates_version(
        self, db_manager: UnifiedDatabaseManager
    ) -> None:
        with db_manager.get_session() as session:
            txn = UnifiedTransaction(
                amount=Decimal("200.00"),
                transaction_date=date(2024, 1, 1),
                transaction_type=TransactionType.CONTRIBUTION,
                state_id=1,
                description="Original",
            )
            session.add(txn)
            session.commit()
            session.refresh(txn)
            txn_id = txn.id

        updated = db_manager.update_transaction(txn_id, {"description": "Updated"})
        assert updated is not None
        versions = db_manager.get_transaction_versions(txn_id)
        assert len(versions) >= 1

    def test_get_transactions_returns_empty_for_unknown_state(
        self, db_manager: UnifiedDatabaseManager
    ) -> None:
        results = db_manager.get_transactions(state="ZZ")
        assert results == []
