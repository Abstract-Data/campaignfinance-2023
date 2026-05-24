"""Tests for resolution-layer SQLModels (task-1b, schema-only).

Verifies all six models — MatchRun, EntityCrosswalk, AddressCrosswalk,
CampaignCrosswalk, MatchDecision, MergeReview — register in metadata,
create cleanly in SQLite, and satisfy column-level constraints.
"""

from __future__ import annotations

import pytest
from sqlalchemy import inspect as sa_inspect
from sqlmodel import SQLModel, create_engine

from app.resolve.models.resolution import (
    AddressCrosswalk,
    CampaignCrosswalk,
    ConfidenceBand,
    DecisionBand,
    DecisionOutcome,
    EntityCrosswalk,
    MatchDecision,
    MatchMethod,
    MatchRun,
    MergeReview,
    PassType,
    ReviewStatus,
    RunStatus,
    SourceType,
)

RESOLUTION_TABLES = {
    "match_run",
    "entity_crosswalk",
    "address_crosswalk",
    "campaign_crosswalk",
    "match_decision",
    "merge_review",
}

_RESOLUTION_MODELS = [
    MatchRun,
    EntityCrosswalk,
    AddressCrosswalk,
    CampaignCrosswalk,
    MatchDecision,
    MergeReview,
]


@pytest.fixture(scope="module")
def engine():
    """SQLite in-memory engine scoped to this module."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    tables = [m.__table__ for m in _RESOLUTION_MODELS]
    SQLModel.metadata.create_all(eng, tables=tables)
    yield eng
    eng.dispose()


# ---------------------------------------------------------------------------
# Step 1 / Step 4 — table registration and DDL
# ---------------------------------------------------------------------------


def test_all_six_tables_registered_in_metadata():
    """All six resolution tables are registered in SQLModel.metadata."""
    registered = set(SQLModel.metadata.tables.keys())
    missing = RESOLUTION_TABLES - registered
    assert not missing, f"Tables not registered in metadata: {missing}"


def test_all_six_tables_created_by_create_all(engine):
    """create_all emits DDL for all six resolution tables."""
    inspector = sa_inspect(engine)
    created = set(inspector.get_table_names())
    missing = RESOLUTION_TABLES - created
    assert not missing, f"Tables not created by create_all: {missing}"


# ---------------------------------------------------------------------------
# Step 5 — column constraints
# ---------------------------------------------------------------------------


def test_source_id_is_string_column_on_all_crosswalks(engine):
    """source_id must be a VARCHAR/TEXT column on every crosswalk table."""
    inspector = sa_inspect(engine)
    for table_name in ("entity_crosswalk", "address_crosswalk", "campaign_crosswalk"):
        cols = {c["name"]: c for c in inspector.get_columns(table_name)}
        assert "source_id" in cols, f"source_id missing from {table_name}"
        col_type = str(cols["source_id"]["type"]).upper()
        assert any(t in col_type for t in ("VARCHAR", "TEXT", "CHAR")), (
            f"source_id on {table_name} is not a string type: {col_type}"
        )


def test_match_score_is_nullable_on_all_crosswalks(engine):
    """match_score must be nullable on every crosswalk table."""
    inspector = sa_inspect(engine)
    for table_name in ("entity_crosswalk", "address_crosswalk", "campaign_crosswalk"):
        cols = {c["name"]: c for c in inspector.get_columns(table_name)}
        assert "match_score" in cols, f"match_score missing from {table_name}"
        assert cols["match_score"]["nullable"], (
            f"match_score on {table_name} must be nullable"
        )


def test_pass_type_enum_rejects_invalid_value():
    """PassType Python enum rejects values not in its definition."""
    with pytest.raises(ValueError):
        PassType("invalid_pass_type")


def test_run_status_enum_rejects_invalid_value():
    """RunStatus Python enum rejects values not in its definition."""
    with pytest.raises(ValueError):
        RunStatus("bad_status")


def test_confidence_band_enum_rejects_invalid_value():
    """ConfidenceBand Python enum rejects invalid values."""
    with pytest.raises(ValueError):
        ConfidenceBand("not_a_band")


def test_match_method_enum_rejects_invalid_value():
    """MatchMethod Python enum rejects invalid values."""
    with pytest.raises(ValueError):
        MatchMethod("not_a_method")


def test_review_status_enum_rejects_invalid_value():
    """ReviewStatus Python enum rejects invalid values."""
    with pytest.raises(ValueError):
        ReviewStatus("bad_status")


# ---------------------------------------------------------------------------
# Model-level behaviour
# ---------------------------------------------------------------------------


def test_match_run_counters_default_to_zero():
    """All MatchRun integer counters default to 0."""
    run = MatchRun(state_code="TX", pass_type=PassType.entity)
    assert run.records_in == 0
    assert run.pairs_compared == 0
    assert run.auto_merges == 0
    assert run.queued == 0
    assert run.rejected == 0
    assert run.canonical_out == 0


def test_match_run_status_defaults_to_running():
    """MatchRun.status defaults to RunStatus.running."""
    run = MatchRun(state_code="TX", pass_type=PassType.entity)
    assert run.status == RunStatus.running


def test_entity_crosswalk_accepts_string_filer_id():
    """source_id on EntityCrosswalk accepts committee filer_id strings (numeric strings)."""
    cw = EntityCrosswalk(
        source_type=SourceType.unified_committee,
        source_id="00123456",
        canonical_entity_id=42,
        match_method=MatchMethod.exact,
        confidence_band=ConfidenceBand.auto,
    )
    assert cw.source_id == "00123456"
    assert isinstance(cw.source_id, str)


def test_match_score_nullable_for_exact_method():
    """match_score is None for exact/deterministic matches (no probabilistic score)."""
    cw = EntityCrosswalk(
        source_type=SourceType.unified_person,
        source_id="999",
        canonical_entity_id=1,
        match_method=MatchMethod.exact,
        confidence_band=ConfidenceBand.auto,
        match_score=None,
    )
    assert cw.match_score is None


def test_merge_review_defaults_to_pending():
    """MergeReview.status defaults to ReviewStatus.pending."""
    review = MergeReview(
        source_a_type=SourceType.unified_person,
        source_a_id="1",
        source_b_type=SourceType.unified_person,
        source_b_id="2",
    )
    assert review.status == ReviewStatus.pending


def test_match_decision_captures_pairwise_data():
    """MatchDecision stores both source_a and source_b identifiers."""
    decision = MatchDecision(
        source_a_type=SourceType.unified_person,
        source_a_id="100",
        source_b_type=SourceType.unified_person,
        source_b_id="200",
        score=0.97,
        method=MatchMethod.probabilistic,
        band=DecisionBand.auto,
        outcome=DecisionOutcome.merged,
    )
    assert decision.source_a_id == "100"
    assert decision.source_b_id == "200"
    assert decision.score == pytest.approx(0.97)
    assert decision.outcome == DecisionOutcome.merged


def test_address_crosswalk_same_shape_as_entity():
    """AddressCrosswalk shares the crosswalk shape with a canonical_address_id column."""
    cw = AddressCrosswalk(
        source_type=SourceType.unified_entity,
        source_id="addr_99",
        canonical_address_id=7,
        match_method=MatchMethod.deterministic_rule,
        confidence_band=ConfidenceBand.auto,
    )
    assert cw.canonical_address_id == 7
    assert cw.match_score is None


def test_campaign_crosswalk_same_shape_as_entity():
    """CampaignCrosswalk shares the crosswalk shape with a canonical_campaign_id column."""
    cw = CampaignCrosswalk(
        source_type=SourceType.unified_committee,
        source_id="CAMP-001",
        canonical_campaign_id=5,
        match_method=MatchMethod.exact,
        confidence_band=ConfidenceBand.auto,
    )
    assert cw.canonical_campaign_id == 5
    assert isinstance(cw.source_id, str)
