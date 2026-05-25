"""Tests for analytics and query methods on UnifiedDatabaseManager (TASK-5a).

All tests run against in-memory SQLite — no Postgres required.
Exercises: get_transactions, get_summary_statistics, get_cross_state_analysis,
get_transactions_by_amount_range, get_transactions_by_date_range,
get_committee_by_name, get_person_by_name, add_person_to_committee.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

import app.core.models  # noqa: F401 — register unified tables
from app.core.enums import CommitteeRole, TransactionType
from app.core.models import (
    State,
    UnifiedCommittee,
    UnifiedPerson,
    UnifiedTransaction,
)
from app.core.unified_database import UnifiedDatabaseManager

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    """SQLite engine with only the schema-less unified tables created.

    State-specific models use PostgreSQL schemas (oklahoma.*, texas.*) that
    SQLite does not understand; we skip those by filtering on ``table.schema``.
    """
    db_path = tmp_path / "analytics.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    for table in SQLModel.metadata.sorted_tables:
        if table.schema is None:
            table.create(engine, checkfirst=True)
    return engine


@pytest.fixture
def analytics_db(sqlite_engine) -> UnifiedDatabaseManager:
    manager = UnifiedDatabaseManager(
        database_url=str(sqlite_engine.url), echo=False
    )
    manager.engine = sqlite_engine
    return manager


def _make_txn(
    amount: float,
    tx_type: TransactionType = TransactionType.CONTRIBUTION,
    tx_date: date = date(2024, 6, 1),
    state_id: int = 1,
) -> UnifiedTransaction:
    return UnifiedTransaction(
        amount=Decimal(str(amount)),
        transaction_type=tx_type,
        transaction_date=tx_date,
        state_id=state_id,
    )


@pytest.fixture
def seeded_db(analytics_db: UnifiedDatabaseManager, sqlite_engine) -> UnifiedDatabaseManager:
    """Seed the DB with a small deterministic dataset."""
    with Session(sqlite_engine) as session:
        state_tx = State(name="texas", code="TX")
        state_ok = State(name="oklahoma", code="OK")
        session.add(state_tx)
        session.add(state_ok)
        session.flush()

        txns = [
            UnifiedTransaction(
                amount=Decimal("100.00"),
                transaction_type=TransactionType.CONTRIBUTION,
                transaction_date=date(2024, 1, 15),
                state_id=state_tx.id,
            ),
            UnifiedTransaction(
                amount=Decimal("2500.00"),
                transaction_type=TransactionType.CONTRIBUTION,
                transaction_date=date(2024, 3, 10),
                state_id=state_tx.id,
            ),
            UnifiedTransaction(
                amount=Decimal("750.00"),
                transaction_type=TransactionType.EXPENDITURE,
                transaction_date=date(2024, 4, 5),
                state_id=state_ok.id,
            ),
            UnifiedTransaction(
                amount=Decimal("50000.00"),
                transaction_type=TransactionType.LOAN,
                transaction_date=date(2024, 6, 20),
                state_id=state_ok.id,
            ),
        ]
        for txn in txns:
            session.add(txn)
        session.commit()

    return analytics_db


# ---------------------------------------------------------------------------
# get_transactions — basic filtering
# ---------------------------------------------------------------------------


class TestGetTransactions:
    def test_returns_all_when_no_filter(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions(load_relationships=False)
        assert len(results) == 4

    def test_filter_by_transaction_type(self, seeded_db: UnifiedDatabaseManager) -> None:
        contributions = seeded_db.get_transactions(
            transaction_type=TransactionType.CONTRIBUTION, load_relationships=False
        )
        assert len(contributions) == 2
        assert all(t.transaction_type == TransactionType.CONTRIBUTION for t in contributions)

    def test_limit_respected(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions(limit=2, load_relationships=False)
        assert len(results) <= 2

    def test_empty_db_returns_empty_list(self, analytics_db: UnifiedDatabaseManager) -> None:
        results = analytics_db.get_transactions(load_relationships=False)
        assert results == []


# ---------------------------------------------------------------------------
# get_transactions_by_amount_range
# ---------------------------------------------------------------------------


class TestAmountRangeQuery:
    def test_selects_records_within_range(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions_by_amount_range(500, 3000)
        amounts = {float(r.amount) for r in results}
        assert 2500.0 in amounts
        assert 750.0 in amounts
        assert 100.0 not in amounts
        assert 50000.0 not in amounts

    def test_boundary_values_included(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions_by_amount_range(100, 100)
        assert len(results) == 1
        assert float(results[0].amount) == pytest.approx(100.0)

    def test_no_results_outside_range(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions_by_amount_range(99999, 100000)
        assert results == []


# ---------------------------------------------------------------------------
# get_transactions_by_date_range
# ---------------------------------------------------------------------------


class TestDateRangeQuery:
    def test_selects_transactions_in_range(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions_by_date_range("2024-01-01", "2024-03-31")
        dates = {r.transaction_date.isoformat() for r in results}
        assert "2024-01-15" in dates
        assert "2024-03-10" in dates
        assert "2024-04-05" not in dates

    def test_boundary_dates_inclusive(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions_by_date_range("2024-01-15", "2024-01-15")
        assert len(results) == 1
        assert results[0].transaction_date == date(2024, 1, 15)

    def test_empty_range_returns_empty(self, seeded_db: UnifiedDatabaseManager) -> None:
        results = seeded_db.get_transactions_by_date_range("2020-01-01", "2020-12-31")
        assert results == []


# ---------------------------------------------------------------------------
# get_summary_statistics / get_cross_state_analysis
#
# NOTE: Both methods contain a pre-existing bug — they reference
# ``tx.contributor`` which does not exist on ``UnifiedTransaction``
# (the correct path is ``tx.contribution.contributor``).  This causes an
# ``AttributeError`` when iterating over any seeded data.  The tests below
# characterize the broken behaviour rather than the intended behaviour so
# that the bug is visible and can be tracked until the source is fixed.
# ---------------------------------------------------------------------------


class TestSummaryStatistics:
    def test_empty_db_returns_zero_total(self, analytics_db: UnifiedDatabaseManager) -> None:
        """With no data the loop never runs, so the bug is not triggered."""
        stats = analytics_db.get_summary_statistics()
        assert stats["total_transactions"] == 0
        assert stats["total_amount"] == pytest.approx(0.0)

    def test_with_data_raises_attribute_error(
        self, seeded_db: UnifiedDatabaseManager
    ) -> None:
        """Pre-existing bug: tx.contributor does not exist on UnifiedTransaction.
        Remove this test once the source is fixed to use tx.contribution.contributor."""
        with pytest.raises(AttributeError, match="contributor"):
            seeded_db.get_summary_statistics()


class TestCrossStateAnalysis:
    def test_empty_db_returns_zeroes(self, analytics_db: UnifiedDatabaseManager) -> None:
        """With no data the loop never runs; result keys are present and zeroed."""
        analysis = analytics_db.get_cross_state_analysis()
        assert analysis["total_transactions"] == 0

    def test_with_data_raises_attribute_error(
        self, seeded_db: UnifiedDatabaseManager
    ) -> None:
        """Pre-existing bug mirrors get_summary_statistics — tx.contributor undefined."""
        with pytest.raises(AttributeError, match="contributor"):
            seeded_db.get_cross_state_analysis()


# ---------------------------------------------------------------------------
# get_committee_by_name
# ---------------------------------------------------------------------------


class TestGetCommitteeByName:
    def test_returns_committee_when_present(
        self, analytics_db: UnifiedDatabaseManager, sqlite_engine
    ) -> None:
        with Session(sqlite_engine) as session:
            session.add(UnifiedCommittee(name="Alpha PAC", filer_id="F001", state_id=1))
            session.commit()

        results = analytics_db.get_committee_by_name("Alpha PAC")
        assert len(results) == 1
        assert results[0].filer_id == "F001"

    def test_returns_empty_for_unknown_name(
        self, analytics_db: UnifiedDatabaseManager
    ) -> None:
        results = analytics_db.get_committee_by_name("Nonexistent Committee XYZ")
        assert results == []


# ---------------------------------------------------------------------------
# get_person_by_name
# ---------------------------------------------------------------------------


class TestGetPersonByName:
    def test_returns_person_when_present(
        self, analytics_db: UnifiedDatabaseManager, sqlite_engine
    ) -> None:
        with Session(sqlite_engine) as session:
            person = UnifiedPerson(first_name="Jane", last_name="Doe", state_id=1)
            session.add(person)
            session.commit()

        results = analytics_db.get_person_by_name("Jane", "Doe")
        assert len(results) == 1

    def test_returns_empty_for_unknown_person(
        self, analytics_db: UnifiedDatabaseManager
    ) -> None:
        results = analytics_db.get_person_by_name("Nobody", "Exists")
        assert results == []


# ---------------------------------------------------------------------------
# add_person_to_committee (batch-session injection path)
# ---------------------------------------------------------------------------


class TestAddPersonToCommittee:
    def test_adds_person_to_committee_with_injected_session(
        self, analytics_db: UnifiedDatabaseManager, sqlite_engine
    ) -> None:
        from app.core.models import UnifiedCommitteePerson

        with Session(sqlite_engine) as session:
            committee = UnifiedCommittee(name="Beta Committee", filer_id="F002", state_id=1)
            person = UnifiedPerson(first_name="Bob", last_name="Builder", state_id=1)
            session.add(committee)
            session.add(person)
            session.flush()

            # committee_id is str (= filer_id); person_id is int
            analytics_db.add_person_to_committee(
                person.id,
                committee.filer_id,  # type: ignore[arg-type]  # FK is str in DB
                role=CommitteeRole.TREASURER,
                session=session,
            )
            session.commit()

            link = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.committee_id == committee.filer_id
                )
            ).first()
        assert link is not None
