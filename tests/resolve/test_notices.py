"""Tests for UnifiedNotice source model and CVR2 ingestion (task 0e)."""

from __future__ import annotations

import json
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlmodel import Session, select

from app.core.source_models.notices import UnifiedNotice
from app.core.source_models.notices_ingest import build_notice
from tests.resolve.conftest import (
    StubState,
    StubUnifiedCommittee,
    create_resolve_tables,
)

SAMPLE_CVR2 = {
    "recordType": "CVR2",
    "formTypeCd": "COH",
    "reportInfoIdent": 12345678901,
    "receivedDt": "20240315",
    "infoOnlyFlag": "N",
    "filerIdent": "00012345",
    "filerTypeCd": "COH",
    "filerName": "JANE DOE FOR STATE SENATE",
    "committeeActivityId": 98765432101,
    "notifierCommactPersentKindCd": "NOTIFIER",
    "notifierPersentTypeCd": "ENTITY",
    "notifierNameOrganization": "TEXAS SECRETARY OF STATE",
    "notifierStreetCity": "AUSTIN",
    "notifierStreetStateCd": "TX",
}


def test_build_notice_maps_cvr2_fields() -> None:
    notice = build_notice(SAMPLE_CVR2, state_id=43)

    assert isinstance(notice, UnifiedNotice)
    assert notice.committee_id == "00012345"
    assert notice.report_ident == "12345678901"
    assert notice.state_id == 43
    assert notice.notice_date == date(2024, 3, 15)
    assert notice.notice_from == "TEXAS SECRETARY OF STATE"
    assert notice.uuid
    assert notice.created_at is not None
    assert notice.updated_at is not None

    raw = json.loads(notice.raw_data or "{}")
    assert raw["recordType"] == "CVR2"
    assert raw["formTypeCd"] == "COH"
    assert raw["filerName"] == "JANE DOE FOR STATE SENATE"
    assert "filerIdent" not in raw
    assert "reportInfoIdent" not in raw
    assert "receivedDt" not in raw
    assert "notifierNameOrganization" not in raw


@pytest.fixture(name="notice_session")
def notice_session_fixture():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    create_resolve_tables(
        engine,
        stub_tables=[StubState.__table__, StubUnifiedCommittee.__table__],
        app_tables=[UnifiedNotice.__table__],
    )
    with Session(engine) as session:
        session.add(StubState(id=43, code="TX"))
        session.add(
            StubUnifiedCommittee(filer_id="00012345", name="JANE DOE FOR STATE SENATE")
        )
        session.commit()
        yield session


def test_unified_notices_table_creates_via_metadata_create_all(
    notice_session: Session,
) -> None:
    notice_session.add(build_notice(SAMPLE_CVR2, state_id=43))
    notice_session.commit()

    stored = notice_session.exec(select(UnifiedNotice)).one()
    assert stored.committee_id == "00012345"
    assert stored.notice_date == date(2024, 3, 15)
    assert stored.notice_from == "TEXAS SECRETARY OF STATE"
