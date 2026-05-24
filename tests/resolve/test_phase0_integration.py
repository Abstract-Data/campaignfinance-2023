"""Task 0z — Phase 0 integration tests.

Verifies that all Phase-0 source models are registered in
``SQLModel.metadata`` and that ``RECORD_TYPE_BUILDERS`` covers the expected
record types.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.core.source_models import (
    RECORD_TYPE_BUILDERS,
    CommitteePurpose,
    ExpenditureCategory,
    SpacLink,
    UnifiedNotice,
    UnifiedPledge,
    UnifiedReport,
    build_report,
    link_transactions_to_reports,
)
from app.core.source_models.lookups_ingest import (
    build_committee_purpose,
    build_expenditure_category,
)
from app.core.source_models.notices_ingest import build_notice
from app.core.source_models.pledges_ingest import build_pledge
from app.core.source_models.spac_ingest import build_spac_link
from tests.resolve.conftest import (
    StubState,
    StubUnifiedTransaction,
    create_resolve_tables,
    drop_resolve_tables,
)


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------


EXPECTED_REGISTRY_KEYS = {"CVR1", "CVR2", "CVR3", "EXCAT", "SPAC"}


def test_record_type_builders_has_expected_keys():
    missing = EXPECTED_REGISTRY_KEYS - set(RECORD_TYPE_BUILDERS)
    assert not missing, f"Missing registry keys: {missing}"


def test_record_type_builders_values_are_callable():
    for key, builder in RECORD_TYPE_BUILDERS.items():
        assert callable(builder), f"Builder for {key!r} is not callable"


# ---------------------------------------------------------------------------
# SQLModel.metadata table registration
# ---------------------------------------------------------------------------

EXPECTED_TABLES = {
    "unified_reports",
    "unified_pledges",
    "expenditure_categories",
    "committee_purposes",
    "spac_links",
    "unified_notices",
}


def test_all_phase0_tables_registered_in_metadata():
    registered = set(SQLModel.metadata.tables)
    missing = EXPECTED_TABLES - registered
    assert not missing, f"Tables missing from SQLModel.metadata: {missing}"


# ---------------------------------------------------------------------------
# Builder smoke tests (no DB — just instantiation)
# ---------------------------------------------------------------------------


def test_cvr1_builder_returns_unified_report():
    raw = {
        "recordType": "CVR1",
        "reportInfoIdent": "99999",
        "filerIdent": "00099999",
        "rptTypeCd": "JAN8",
        "rptBeginDt": "20240101",
        "rptEndDt": "20240131",
        "filed_date": "20240201",
    }
    report = RECORD_TYPE_BUILDERS["CVR1"](raw, state_id=1)
    assert isinstance(report, UnifiedReport)


def test_cvr2_builder_returns_unified_notice():
    raw = {
        "recordType": "CVR2",
        "filerIdent": "00099999",
        "reportInfoIdent": "99999",
        "receivedDt": "20240201",
    }
    notice = RECORD_TYPE_BUILDERS["CVR2"](raw, state_id=1)
    assert isinstance(notice, UnifiedNotice)


def test_cvr3_builder_returns_committee_purpose():
    raw = {
        "recordType": "CVR3",
        "filerIdent": "00099999",
        "reportInfoIdent": "99999",
        "officeSoughtDesc": "Governor",
        "countyDesc": "Travis",
        "districtDesc": "5",
        "officeHeldDesc": "None",
    }
    purpose = RECORD_TYPE_BUILDERS["CVR3"](raw, state_id=1)
    assert isinstance(purpose, CommitteePurpose)


def test_excat_builder_returns_expenditure_category():
    raw = {"expendCategoryCodeValue": "FOOD", "expendCategoryCodeLabel": "Food and beverage"}
    category = RECORD_TYPE_BUILDERS["EXCAT"](raw, state_id=1)
    assert isinstance(category, ExpenditureCategory)


def test_spac_builder_returns_spac_link():
    raw = {
        "spacFilerIdent": "SPAC001",
        "candidateFilerIdent": "CAND001",
        "candidateNameFirst": "John",
        "candidateNameLast": "Doe",
        "supportOppositCd": "SUPPORT",
    }
    link = RECORD_TYPE_BUILDERS["SPAC"](raw, state_id=1)
    assert isinstance(link, SpacLink)


# ---------------------------------------------------------------------------
# Integration fixture — engine with all Phase-0 tables created
# ---------------------------------------------------------------------------


@pytest.fixture(name="integ_engine")
def integ_engine_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    create_resolve_tables(engine)
    yield engine
    drop_resolve_tables(engine)


def test_load_cvr1_and_link_to_transactions(integ_engine):
    """End-to-end: insert a CVR1 report, insert transactions, verify linking."""
    with Session(integ_engine) as session:
        state = StubState(code="TX")
        session.add(state)
        session.commit()
        session.refresh(state)

        # Insert a report
        raw_cvr1 = {
            "reportInfoIdent": "REPT001",
            "filerIdent": None,
            "rptTypeCd": "JAN8",
            "rptBeginDt": "20240101",
            "rptEndDt": "20240131",
        }
        report = build_report(raw_cvr1, state_id=state.id)
        session.add(report)
        session.commit()

        # Insert a transaction that references this report
        txn = StubUnifiedTransaction(report_ident="REPT001", state_id=state.id)
        session.add(txn)
        session.commit()

        # Link transactions to reports
        linked = link_transactions_to_reports(session)
        assert linked >= 1

        # Verify the transaction has report_id set
        refreshed = session.exec(
            select(StubUnifiedTransaction).where(StubUnifiedTransaction.id == txn.id)
        ).one()
        assert refreshed.report_id == report.id


def test_all_builders_produce_insertable_rows(integ_engine):
    """Verify each builder produces a row that can be persisted."""
    with Session(integ_engine) as session:
        state = StubState(code="TX")
        session.add(state)
        session.commit()
        session.refresh(state)

        # ExpenditureCategory
        cat = build_expenditure_category(
            {"expendCategoryCodeValue": "TEST", "expendCategoryCodeLabel": "Test Category"}
        )
        session.add(cat)

        # CommitteePurpose
        purpose = build_committee_purpose(
            {
                "filerIdent": "00011111",
                "reportInfoIdent": "11111",
                "officeSoughtDesc": "Senator",
                "countyDesc": "Harris",
                "districtDesc": "1",
                "officeHeldDesc": "",
            },
            state_id=state.id,
        )
        session.add(purpose)

        # UnifiedNotice
        notice = build_notice(
            {"filerIdent": "00012345", "reportInfoIdent": "12345", "receivedDt": "20240115"},
            state_id=state.id,
        )
        session.add(notice)

        # UnifiedReport
        report = build_report(
            {"reportInfoIdent": "REPT999", "filerIdent": None, "rptTypeCd": "JAN8"},
            state_id=state.id,
        )
        session.add(report)
        session.commit()

        # Verify they are all in the DB
        assert session.exec(select(ExpenditureCategory)).first() is not None
        assert session.exec(select(CommitteePurpose)).first() is not None
        assert session.exec(select(UnifiedNotice)).first() is not None
        assert session.exec(select(UnifiedReport)).first() is not None
