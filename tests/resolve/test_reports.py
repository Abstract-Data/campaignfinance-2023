"""Task 0a — UnifiedReport model + CVR1 ingestion + report_id link tests."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlmodel import Field, Session, SQLModel, create_engine, select

from app.core.source_models.reports import UnifiedReport
from app.core.source_models.reports_ingest import build_report, link_transactions_to_reports


class _State(SQLModel, table=True):
    __tablename__ = "states"
    __table_args__ = {"extend_existing": True}

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(max_length=2, default="TX")


class _FileOrigin(SQLModel, table=True):
    __tablename__ = "file_origins"
    __table_args__ = {"extend_existing": True}

    id: str = Field(primary_key=True, max_length=64)
    state_id: int | None = Field(default=None, foreign_key="states.id")
    filename: str = Field(default="cover.parquet", max_length=500)
    created_at: datetime = Field(default_factory=datetime.utcnow)


class _UnifiedCommittee(SQLModel, table=True):
    __tablename__ = "unified_committees"
    __table_args__ = {"extend_existing": True}

    filer_id: str = Field(primary_key=True, max_length=100)
    name: str | None = None


class _UnifiedTransaction(SQLModel, table=True):
    __tablename__ = "unified_transactions"
    __table_args__ = {"extend_existing": True}

    id: int | None = Field(default=None, primary_key=True)
    state_id: int | None = Field(default=None, foreign_key="states.id")
    committee_id: str | None = Field(default=None, foreign_key="unified_committees.filer_id")
    report_ident: str | None = Field(default=None, max_length=20, index=True)
    report_id: int | None = Field(default=None, index=True)


SAMPLE_CVR1 = {
    "recordType": "CVR1",
    "formTypeCd": "GPAC",
    "reportInfoIdent": "12345678901",
    "receivedDt": "20240115",
    "infoOnlyFlag": "N",
    "filerIdent": "00012345",
    "filerTypeCd": "GPAC",
    "filerName": "Example PAC",
    "filedDt": "20240115",
    "periodStartDt": "20240101",
    "periodEndDt": "20240131",
    "totalContribAmount": "15000.00",
    "unitemizedContribAmount": "500.00",
    "totalExpendAmount": "8500.00",
    "unitemizedExpendAmount": "250.00",
    "loanBalanceAmount": "0.00",
    "contribsMaintainedAmount": "6500.00",
}


def test_build_report_returns_unified_report_instance() -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)
    assert isinstance(report, UnifiedReport)


def test_build_report_maps_identifiers() -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)

    assert report.report_ident == "12345678901"
    assert report.committee_id == "00012345"
    assert report.state_id == 43
    assert report.form_type == "GPAC"
    assert report.uuid is not None


def test_build_report_parses_period_dates() -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)

    assert report.period_start == date(2024, 1, 1)
    assert report.period_end == date(2024, 1, 31)
    assert report.filed_date == date(2024, 1, 15)


def test_build_report_parses_decimal_totals() -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)

    assert report.total_contributions == Decimal("15000.00")
    assert report.total_unitemized_contributions == Decimal("500.00")
    assert report.total_expenditures == Decimal("8500.00")
    assert report.total_unitemized_expenditures == Decimal("250.00")
    assert report.loan_balance == Decimal("0.00")
    assert report.contributions_maintained == Decimal("6500.00")


def test_build_report_defaults() -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)

    assert report.is_final is False
    assert report.created_at is not None
    assert report.updated_at is not None


def test_build_report_file_origin_id_passed_through() -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id="abc123")
    assert report.file_origin_id == "abc123"


@pytest.fixture(name="reports_session")
def reports_session_fixture():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(
        engine,
        tables=[
            _State.__table__,
            _FileOrigin.__table__,
            _UnifiedCommittee.__table__,
            _UnifiedTransaction.__table__,
            UnifiedReport.__table__,
        ],
    )
    with Session(engine) as session:
        session.add(_State(id=43, code="TX"))
        session.add(_UnifiedCommittee(filer_id="00012345", name="Example PAC"))
        session.commit()
        yield session


def test_link_transactions_to_reports_returns_count(reports_session: Session) -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)
    reports_session.add(report)
    reports_session.commit()
    reports_session.refresh(report)

    tx1 = _UnifiedTransaction(state_id=43, committee_id="00012345", report_ident="12345678901")
    tx2 = _UnifiedTransaction(state_id=43, committee_id="00012345", report_ident="12345678901")
    reports_session.add(tx1)
    reports_session.add(tx2)
    reports_session.commit()

    count = link_transactions_to_reports(reports_session)
    assert count == 2


def test_link_transactions_to_reports_sets_report_id(reports_session: Session) -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)
    reports_session.add(report)
    reports_session.commit()
    reports_session.refresh(report)

    tx = _UnifiedTransaction(state_id=43, committee_id="00012345", report_ident="12345678901")
    reports_session.add(tx)
    reports_session.commit()
    reports_session.refresh(tx)

    link_transactions_to_reports(reports_session)
    reports_session.refresh(tx)

    assert tx.report_id == report.id


def test_link_transactions_to_reports_skips_already_linked(reports_session: Session) -> None:
    report1 = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)
    reports_session.add(report1)
    sample2 = dict(SAMPLE_CVR1, reportInfoIdent="99999999999")
    report2 = build_report(sample2, state_id=43, file_origin_id=None)
    reports_session.add(report2)
    reports_session.commit()
    reports_session.refresh(report1)
    reports_session.refresh(report2)

    tx_linked = _UnifiedTransaction(
        state_id=43,
        committee_id="00012345",
        report_ident="12345678901",
        report_id=report1.id,
    )
    tx_unlinked = _UnifiedTransaction(
        state_id=43, committee_id="00012345", report_ident="12345678901"
    )
    reports_session.add(tx_linked)
    reports_session.add(tx_unlinked)
    reports_session.commit()

    count = link_transactions_to_reports(reports_session)
    assert count == 1


def test_unified_reports_table_creates_cleanly(reports_session: Session) -> None:
    report = build_report(SAMPLE_CVR1, state_id=43, file_origin_id=None)
    reports_session.add(report)
    reports_session.commit()

    stored = reports_session.exec(select(UnifiedReport)).one()
    assert stored.report_ident == "12345678901"
    assert stored.committee_id == "00012345"
    assert stored.total_contributions == Decimal("15000.00")
