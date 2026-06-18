"""Tests for Task 2a — at-filing columns, backfill, and treasurer helper.

Covers:
- build_report() populates committee_name_at_filing and treasurer_name_at_filing
  for both INDIVIDUAL and ENTITY treasurer types.
- backfill_report_at_filing() correctly updates NULL at-filing columns from
  stored raw_data JSON on SQLite.
- treasurer_for_report() returns None cleanly when no matching treasurer exists
  (avoids the heavyweight TECTreasurer address validator in unit tests).
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlmodel import Session, create_engine

import app.core.models  # noqa: F401 — ensure unified tables are registered
from app.core.source_models.reports import UnifiedReport
from app.core.source_models.reports_ingest import (
    backfill_report_at_filing,
    build_report,
    treasurer_for_report,
)
from tests.resolve.conftest import (
    StubFileOrigin,
    StubState,
    StubUnifiedCommittee,
    create_resolve_tables,
)

# ---------------------------------------------------------------------------
# Shared raw-dict fixtures
# ---------------------------------------------------------------------------

BASE_CVR1: dict = {
    "recordType": "CVR1",
    "formTypeCd": "GPAC",
    "reportInfoIdent": "11111111111",
    "filerIdent": "00012345",
    "filerTypeCd": "GPAC",
    "filerName": "Acme Political Action Committee",
    "filedDt": "20240315",
    "periodStartDt": "20240101",
    "periodEndDt": "20240331",
    "totalContribAmount": "1000.00",
    "unitemizedContribAmount": "0.00",
    "totalExpendAmount": "500.00",
    "unitemizedExpendAmount": "0.00",
    "loanBalanceAmount": "0.00",
    "contribsMaintainedAmount": "500.00",
    "cashOnHandAmount": "500.00",
}

INDIVIDUAL_CVR1: dict = {
    **BASE_CVR1,
    "treasPersentTypeCd": "INDIVIDUAL",
    "treasNameFirst": "Jane",
    "treasNameLast": "Doe",
    "treasNameOrganization": "",
}

ENTITY_CVR1: dict = {
    **BASE_CVR1,
    "reportInfoIdent": "22222222222",
    "treasPersentTypeCd": "ENTITY",
    "treasNameFirst": "",
    "treasNameLast": "",
    "treasNameOrganization": "Finance Corp LLC",
}

MISSING_TREAS_CVR1: dict = {
    **BASE_CVR1,
    "reportInfoIdent": "33333333333",
    # No treas* keys at all
}


# ---------------------------------------------------------------------------
# build_report() at-filing column tests
# ---------------------------------------------------------------------------


class TestBuildReportAtFilingColumns:
    """Verify build_report() populates at-filing snapshot columns."""

    def test_committee_name_populated_from_filer_name(self) -> None:
        report = build_report(BASE_CVR1, state_id=43)
        assert report.committee_name_at_filing == "Acme Political Action Committee"

    def test_filer_name_missing_yields_none(self) -> None:
        raw = dict(BASE_CVR1, reportInfoIdent="44444444444")
        raw.pop("filerName", None)
        report = build_report(raw, state_id=43)
        assert report.committee_name_at_filing is None

    def test_filer_name_blank_yields_none(self) -> None:
        raw = dict(BASE_CVR1, reportInfoIdent="55555555555", filerName="   ")
        report = build_report(raw, state_id=43)
        assert report.committee_name_at_filing is None

    def test_individual_treasurer_joined_first_last(self) -> None:
        report = build_report(INDIVIDUAL_CVR1, state_id=43)
        assert report.treasurer_name_at_filing == "Jane Doe"

    def test_entity_treasurer_uses_organization(self) -> None:
        report = build_report(ENTITY_CVR1, state_id=43)
        assert report.treasurer_name_at_filing == "Finance Corp LLC"

    def test_individual_only_last_name(self) -> None:
        raw = dict(
            INDIVIDUAL_CVR1,
            reportInfoIdent="66666666666",
            treasNameFirst="",
            treasNameLast="Smith",
        )
        report = build_report(raw, state_id=43)
        assert report.treasurer_name_at_filing == "Smith"

    def test_individual_only_first_name(self) -> None:
        raw = dict(
            INDIVIDUAL_CVR1,
            reportInfoIdent="77777777777",
            treasNameFirst="Alice",
            treasNameLast="",
        )
        report = build_report(raw, state_id=43)
        assert report.treasurer_name_at_filing == "Alice"

    def test_no_treasurer_fields_yields_none(self) -> None:
        report = build_report(MISSING_TREAS_CVR1, state_id=43)
        assert report.treasurer_name_at_filing is None

    def test_unknown_treas_type_treated_as_individual(self) -> None:
        """Unrecognised treasPersentTypeCd falls through to the INDIVIDUAL branch."""
        raw = dict(
            INDIVIDUAL_CVR1,
            reportInfoIdent="88888888888",
            treasPersentTypeCd="UNKNOWN",
            treasNameFirst="Bob",
            treasNameLast="Jones",
        )
        report = build_report(raw, state_id=43)
        assert report.treasurer_name_at_filing == "Bob Jones"


# ---------------------------------------------------------------------------
# backfill_report_at_filing() — SQLite in-memory engine
# ---------------------------------------------------------------------------


@pytest.fixture(name="backfill_session")
def backfill_session_fixture():
    """Fresh SQLite in-memory engine with only schema-less unified tables."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    create_resolve_tables(
        engine,
        stub_tables=[
            StubState.__table__,
            StubFileOrigin.__table__,
            StubUnifiedCommittee.__table__,
        ],
        app_tables=[UnifiedReport.__table__],
    )
    with Session(engine) as session:
        session.add(StubState(id=43, code="TX"))
        session.add(StubUnifiedCommittee(filer_id="00012345", name="Acme PAC"))
        session.commit()
        yield session




class TestBackfillReportAtFiling:
    """backfill_report_at_filing() is a no-op stub since Wave 2b removed raw_data."""

    def test_backfill_returns_zero(self, backfill_session: Session) -> None:
        """backfill_report_at_filing() is now a no-op; always returns 0."""
        count = backfill_report_at_filing(backfill_session)
        assert count == 0

    def test_backfill_returns_int(self, backfill_session: Session) -> None:
        """Return type is int."""
        count = backfill_report_at_filing(backfill_session)
        assert isinstance(count, int)


# ---------------------------------------------------------------------------
# treasurer_for_report() — light test (no-match path)
#
# TECTreasurer rows require a valid address via the Pydantic model-validator,
# making them heavyweight to construct in a pure unit test.  We verify the
# helper returns None cleanly when no treasurer is linked, which exercises the
# query path without needing a fully populated texas schema.
# ---------------------------------------------------------------------------


class TestTreasurerForReport:
    """treasurer_for_report() returns None when no treasurer match exists."""

    def test_returns_none_when_no_committee_id(self, backfill_session: Session) -> None:
        report = UnifiedReport(
            state_id=43,
            report_ident="97777777777",
            committee_id=None,
            filed_date=date(2024, 3, 15),
        )
        result = treasurer_for_report(backfill_session, report)
        assert result is None

    def test_returns_none_when_no_filed_date(self, backfill_session: Session) -> None:
        report = UnifiedReport(
            state_id=43,
            report_ident="97777777778",
            committee_id="00012345",
            filed_date=None,
        )
        result = treasurer_for_report(backfill_session, report)
        assert result is None

    def test_returns_none_when_non_integer_committee_id(self, backfill_session: Session) -> None:
        """committee_id that cannot be cast to int returns None gracefully."""
        report = UnifiedReport(
            state_id=43,
            report_ident="97777777779",
            committee_id="not-an-int",
            filed_date=date(2024, 3, 15),
        )
        result = treasurer_for_report(backfill_session, report)
        assert result is None
