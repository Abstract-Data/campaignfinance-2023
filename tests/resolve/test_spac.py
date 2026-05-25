"""Tests for SpacLink source model and SPAC ingestion."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlmodel import Session

from app.core.source_models.spac import SpacLink
from app.core.source_models.spac_ingest import build_spac_link
from tests.resolve.conftest import (
    StubState,
    StubUnifiedCommittee,
    create_resolve_tables,
)


SAMPLE_SPAC_WITH_SUPPORTED_ID = {
    "recordType": "SPAC",
    "spacFilerIdent": "00049300",
    "spacFilerName": "CITIZENS FOR GOOD GOVERNMENT SPAC",
    "spacPositionCd": "SUPPORT",
    "candidateFilerIdent": "00012345",
    "candidateFilerName": "JANE DOE",
}

SAMPLE_SPAC_NAME_ONLY = {
    "recordType": "SPAC",
    "spacFilerIdent": "00049300",
    "spacFilerName": "CITIZENS FOR GOOD GOVERNMENT SPAC",
    "spacPositionCd": "OPPOSE",
    "candidateFilerName": "JOHN SMITH",
}

SAMPLE_SPAC_MEASURE = {
    "recordType": "SPAC",
    "spacFilerIdent": "00049300",
    "spacFilerName": "BOND SUPPORT COMMITTEE",
    "spacPositionCd": "SUPPORT",
    "ctaSeekOfficeDescr": "MUNICIPAL BOND PROPOSITION A",
}


def test_build_spac_link_maps_candidate_with_supported_filer_id() -> None:
    link = build_spac_link(SAMPLE_SPAC_WITH_SUPPORTED_ID, state_id=44)

    assert isinstance(link, SpacLink)
    assert link.spac_filer_id == "00049300"
    assert link.supported_filer_id == "00012345"
    assert link.supported_name == "JANE DOE"
    assert link.support_type == "candidate"
    assert link.position == "support"
    assert link.state_id == 44
    assert json.loads(link.raw_data or "{}") == SAMPLE_SPAC_WITH_SUPPORTED_ID


def test_build_spac_link_leaves_supported_filer_id_none_when_absent() -> None:
    link = build_spac_link(SAMPLE_SPAC_NAME_ONLY, state_id=44)

    assert link.supported_filer_id is None
    assert link.supported_name == "JOHN SMITH"
    assert link.support_type == "candidate"
    assert link.position == "oppose"


def test_build_spac_link_maps_measure_record() -> None:
    link = build_spac_link(SAMPLE_SPAC_MEASURE, state_id=44)

    assert link.supported_filer_id is None
    assert link.supported_name == "MUNICIPAL BOND PROPOSITION A"
    assert link.support_type == "measure"
    assert link.position == "support"


def test_spac_links_table_creates_via_metadata_create_all() -> None:
    engine = create_engine("sqlite:///:memory:")
    create_resolve_tables(
        engine,
        stub_tables=[StubState.__table__, StubUnifiedCommittee.__table__],
        app_tables=[SpacLink.__table__],
    )

    with Session(engine) as session:
        session.add(StubState(id=44, code="TX", name="Texas"))
        session.add(StubUnifiedCommittee(filer_id="00049300", name="SPAC"))
        session.add(
            SpacLink(
                spac_filer_id="00049300",
                supported_name="JANE DOE",
                support_type="candidate",
                position="support",
                state_id=44,
                raw_data=json.dumps(SAMPLE_SPAC_NAME_ONLY),
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        session.commit()

        stored = session.get(SpacLink, 1)
        assert stored is not None
        assert stored.spac_filer_id == "00049300"
