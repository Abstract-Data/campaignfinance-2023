"""Phase 0 reconciliation gate — ``reconcile_report_totals`` contract tests.

Seeds in-memory SQLite with ``UnifiedReport`` / ``UnifiedTransaction`` rows
(no ``tmp/texas`` or production Postgres required).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlmodel import Session, SQLModel, create_engine

from app.core.source_models import UnifiedReport, reconcile_report_totals
from app.core.enums import TransactionType
from app.core.models import UnifiedTransaction
from tests.resolve.conftest import (
    StubState,
    create_stub_tables,
    stub_metadata,
)


@pytest.fixture(name="recon_engine")
def recon_engine_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    # Use app ``UnifiedTransaction`` schema (not the stub table of the same name).
    create_stub_tables(engine, tables=[StubState.__table__])
    SQLModel.metadata.create_all(
        engine,
        tables=[UnifiedReport.__table__, UnifiedTransaction.__table__],
    )
    yield engine
    SQLModel.metadata.drop_all(
        engine,
        tables=[UnifiedReport.__table__, UnifiedTransaction.__table__],
    )
    stub_metadata.drop_all(engine, tables=[StubState.__table__])


def _add_report(
    session: Session,
    *,
    state_id: int,
    report_ident: str,
    total_contributions: Decimal | None,
    total_expenditures: Decimal | None = None,
) -> UnifiedReport:
    report = UnifiedReport(
        state_id=state_id,
        report_ident=report_ident,
        total_contributions=total_contributions,
        total_expenditures=total_expenditures,
    )
    session.add(report)
    session.commit()
    session.refresh(report)
    return report


def _add_transaction(
    session: Session,
    *,
    report_id: int,
    state_id: int,
    amount: Decimal,
    transaction_type: TransactionType,
) -> None:
    session.add(
        UnifiedTransaction(
            report_id=report_id,
            state_id=state_id,
            amount=amount,
            transaction_type=transaction_type,
        )
    )
    session.commit()


def test_reconcile_report_totals_returns_expected_keys(recon_engine) -> None:
    with Session(recon_engine) as session:
        state = StubState(code="TX")
        session.add(state)
        session.commit()
        session.refresh(state)

        matched = _add_report(
            session,
            state_id=state.id,
            report_ident="RPT-MATCH",
            total_contributions=Decimal("1000.00"),
        )
        _add_transaction(
            session,
            report_id=matched.id,
            state_id=state.id,
            amount=Decimal("600.00"),
            transaction_type=TransactionType.CONTRIBUTION,
        )
        _add_transaction(
            session,
            report_id=matched.id,
            state_id=state.id,
            amount=Decimal("400.00"),
            transaction_type=TransactionType.CONTRIBUTION,
        )

        mismatched = _add_report(
            session,
            state_id=state.id,
            report_ident="RPT-MISMATCH",
            total_contributions=Decimal("1000.00"),
        )
        _add_transaction(
            session,
            report_id=mismatched.id,
            state_id=state.id,
            amount=Decimal("500.00"),
            transaction_type=TransactionType.CONTRIBUTION,
        )

        skipped = _add_report(
            session,
            state_id=state.id,
            report_ident="RPT-SKIP",
            total_contributions=None,
            total_expenditures=None,
        )
        _add_transaction(
            session,
            report_id=skipped.id,
            state_id=state.id,
            amount=Decimal("100.00"),
            transaction_type=TransactionType.CONTRIBUTION,
        )

        result = reconcile_report_totals(session, tolerance=Decimal("1.00"))

    assert set(result.keys()) == {"checked", "matched", "mismatched", "skipped"}
    assert result["checked"] == 3
    assert result["matched"] == 1
    assert result["mismatched"] == 1
    assert result["skipped"] == 1


def test_reconcile_report_totals_respects_tolerance(recon_engine) -> None:
    """A declared total within tolerance counts as matched."""
    with Session(recon_engine) as session:
        state = StubState(code="TX")
        session.add(state)
        session.commit()
        session.refresh(state)

        report = _add_report(
            session,
            state_id=state.id,
            report_ident="RPT-TOL",
            total_contributions=Decimal("1000.00"),
        )
        _add_transaction(
            session,
            report_id=report.id,
            state_id=state.id,
            amount=Decimal("999.50"),
            transaction_type=TransactionType.CONTRIBUTION,
        )

        result = reconcile_report_totals(session, tolerance=Decimal("1.00"))

    assert result["checked"] == 1
    assert result["matched"] == 1
    assert result["mismatched"] == 0
