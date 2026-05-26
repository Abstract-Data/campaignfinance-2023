"""Phase 2 end-to-end integration tests.

Wires all seven Phase 2 stages together and verifies the pipeline
contract on a small seeded fixture:

  Stage 1  build_resolution_input      (task-1c)
  Stage 2  run_blocking_stage          (task-1e)
  Stage 3  run_fastpath_stage          (task-1f)
  Stage 4  run_score_stage             (task-2a)
  Stage 5  run_classify_stage          (task-2b)
  Stage 6  run_cluster_stage           (task-2c)
  Stage 7  run_survivorship_stage      (task-1g + 2d)

TDD steps per task-2z brief:

- Step 2: E2E test — ``match_run.status=completed``, ``scored_pairs`` rows,
  ``merge_review`` rows for the medium band, ``clusters`` rows.
- Step 6: Idempotency — two runs produce identical crosswalk structures.
"""

from __future__ import annotations

import json
from collections import defaultdict

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

import app.resolve.models  # noqa: F401 — central ORM registry
from app.core.models import (
    State,
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.resolve.blocking import CandidatePair
from app.resolve.models.canonical import (
    CanonicalAddress,
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalNameHistory,
)
from app.resolve.models.resolution import (
    AddressCrosswalk,
    CampaignCrosswalk,
    EntityCrosswalk,
    MatchDecision,
    MatchMethod,
    MatchRun,
    MergeReview,
    RunStatus,
)
from app.resolve.run import ResolutionRun
from app.resolve.stages import (
    run_blocking_stage,
    run_classify_stage,
    run_cluster_stage,
    run_fastpath_stage,
    run_score_stage,
    run_survivorship_stage,
    stage1_build_resolution_input,
)
from app.resolve.stages.cluster import ClusterAssignment
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.stages.score import ScoredPair
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Full Phase 2 stage list (stages 1→2→3→4→5→6→7)
# ---------------------------------------------------------------------------

PHASE2_STAGES = [
    stage1_build_resolution_input,  # stage 1 — standardize
    run_blocking_stage,  # stage 2 — candidate pair blocking
    run_fastpath_stage,  # stage 3 — deterministic fast-path
    run_score_stage,  # stage 4 — Splink probabilistic scoring
    run_classify_stage,  # stage 5 — band classification
    run_cluster_stage,  # stage 6 — connected components clustering
    run_survivorship_stage,  # stage 7 — survivorship + canonical publish
]

# Test config: uses an unreachable auto_threshold so every pair Splink scores
# above 0 lands in the medium (review) band — guaranteeing merge_review rows
# in the integration fixture regardless of exact Splink probabilities.
_TEST_CONFIG = {
    "state_code": "TX",
    "pass_type": "entity",
    "auto_threshold": 1.01,  # unreachable → no auto-merges from Splink
    "review_threshold": 0.0,  # floor → all non-zero-scored pairs go to review
    "max_cluster_size": 200,
    "seed": 42,
}

# Low auto_threshold so fixture near-duplicates Splink scores above the auto band
# and Stage 7 writes entity_crosswalk rows with match_method=probabilistic.
_PROBABILISTIC_XWALK_CONFIG = {
    "state_code": "TX",
    "pass_type": "entity",
    "auto_threshold": 1e-6,
    "review_threshold": 0.0,
    "max_cluster_size": 200,
    "seed": 42,
}

# ---------------------------------------------------------------------------
# Tables required for Phase 2 tests
# ---------------------------------------------------------------------------

_TABLES_TO_CREATE = [
    # Unified source layer
    State.__table__,
    UnifiedAddress.__table__,
    UnifiedPerson.__table__,
    UnifiedCommittee.__table__,
    UnifiedEntity.__table__,
    # Resolution pipeline — run tracking
    MatchRun.__table__,
    # Resolution pipeline — stage intermediates
    ResolutionInput.__table__,
    CandidatePair.__table__,
    MergeEdge.__table__,
    ScoredPair.__table__,
    MatchDecision.__table__,
    MergeReview.__table__,
    ClusterAssignment.__table__,
    # Canonical layer
    CanonicalAddress.__table__,
    CanonicalEntity.__table__,
    CanonicalCampaign.__table__,
    CanonicalNameHistory.__table__,
    # Crosswalk tables
    EntityCrosswalk.__table__,
    AddressCrosswalk.__table__,
    CampaignCrosswalk.__table__,
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """In-memory SQLite engine with Phase 2 tables created."""
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng, tables=_TABLES_TO_CREATE)
    return eng


def _seed_source_data(session: Session, state_code: str = "TX") -> dict:
    """Insert representative unified source records for Phase 2 testing.

    Fixture has enough diversity for Splink EM training (≥4 person records)
    and phonetic-variant / address-variant near-duplicates that Splink will
    score > 0, driving rows into the ``scored_pairs`` and ``merge_review``
    tables when the test config has a low review_threshold.

    Records:
      - State TX
      - UnifiedAddress A1 (123 Main St, Austin)
      - UnifiedAddress A2 (124 Main St, Austin) — near-duplicate address
      - P1: John Smith   @ A1  ─┐ identical → fastpath merges
      - P2: John Smith   @ A1  ─┘
      - P3: Jon Smith    @ A1    — phonetic variant; near-duplicate
      - P4: John Smyth   @ A2   — typo variant; near-duplicate
      - P5: Jane Doe     @ A2   — clearly different
      - C1: Texas Democratic Party — committee singleton
    """
    state = State(code=state_code, name="Texas")
    session.add(state)
    session.flush()

    addr1 = UnifiedAddress(street_1="123 Main St", city="Austin", state="TX", zip_code="78701")
    addr2 = UnifiedAddress(street_1="124 Main St", city="Austin", state="TX", zip_code="78701")
    session.add_all([addr1, addr2])
    session.flush()

    p1 = UnifiedPerson(first_name="John", last_name="Smith", state_id=state.id, address_id=addr1.id)
    p2 = UnifiedPerson(first_name="John", last_name="Smith", state_id=state.id, address_id=addr1.id)
    p3 = UnifiedPerson(first_name="Jon", last_name="Smith", state_id=state.id, address_id=addr1.id)
    p4 = UnifiedPerson(first_name="John", last_name="Smyth", state_id=state.id, address_id=addr2.id)
    p5 = UnifiedPerson(first_name="Jane", last_name="Doe", state_id=state.id, address_id=addr2.id)
    session.add_all([p1, p2, p3, p4, p5])
    session.flush()

    committee = UnifiedCommittee(
        filer_id="CMTE001", name="Texas Democratic Party", state_id=state.id
    )
    session.add(committee)
    session.commit()

    return {
        "state": state,
        "persons": [p1, p2, p3, p4, p5],
        "committees": [committee],
        "source_count": 6,  # 5 persons + 1 committee
    }


@pytest.fixture()
def seeded_engine(engine):
    """engine with representative source data pre-seeded."""
    with Session(engine) as session:
        seed = _seed_source_data(session)
    return engine, seed


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run_phase2_pipeline(engine, config: dict | None = None) -> ResolutionRun:
    """Execute the full Phase 2 pipeline on *engine* and return the run object."""
    cfg = config if config is not None else _TEST_CONFIG.copy()
    run = ResolutionRun(state_code=cfg.get("state_code", "TX"), config=cfg)
    with Session(engine) as session:
        run.run(session, PHASE2_STAGES)
    return run


def _crosswalk_cluster_structure(
    session: Session,
    run_id: int,
) -> frozenset[frozenset[tuple[str, str]]]:
    """Return cluster structure for *run_id* as frozenset of frozensets.

    Each inner frozenset contains ``(source_type_str, source_id)`` tuples for
    one canonical entity cluster.
    """
    xwalks = session.exec(select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)).all()
    clusters: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for xw in xwalks:
        st = xw.source_type.value if hasattr(xw.source_type, "value") else str(xw.source_type)
        clusters[xw.canonical_entity_id].append((st, xw.source_id))
    return frozenset(frozenset(members) for members in clusters.values())


# ---------------------------------------------------------------------------
# E2E tests
# ---------------------------------------------------------------------------


class TestPhase2EndToEnd:
    """Verify the 7-stage pipeline completes and satisfies Phase 2 invariants."""

    def test_pipeline_status_is_completed(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run is not None
        assert match_run.status == RunStatus.completed

    def test_match_run_has_finished_at_set(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.finished_at is not None

    def test_records_in_count_equals_source_count(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.records_in == seed["source_count"]

    def test_scored_pairs_rows_created(self, seeded_engine):
        """Stage 4 must produce at least one scored_pairs row."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == run.run_id)).all()

        assert len(rows) >= 1, "scored_pairs must have at least one row after Stage 4"

    def test_scored_pairs_have_valid_scores(self, seeded_engine):
        """All scored_pairs scores must be in [0.0, 1.0]."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            rows = session.exec(select(ScoredPair).where(ScoredPair.run_id == run.run_id)).all()

        for row in rows:
            assert 0.0 <= row.score <= 1.0, f"score {row.score} out of [0, 1]"

    def test_match_decisions_created(self, seeded_engine):
        """Stage 5 must write at least one match_decision row."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            decisions = session.exec(
                select(MatchDecision).where(MatchDecision.run_id == run.run_id)
            ).all()

        assert len(decisions) >= 1

    def test_merge_review_rows_for_medium_band(self, seeded_engine):
        """With wide review band, at least one scored pair lands in merge_review."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            reviews = session.exec(
                select(MergeReview).where(MergeReview.run_id == run.run_id)
            ).all()

        assert len(reviews) >= 1, (
            "merge_review must have rows: at least one scored pair should fall "
            "in the medium band with review_threshold=0.0 and auto_threshold=1.01"
        )

    def test_clusters_rows_created(self, seeded_engine):
        """Stage 6 must populate the clusters staging table."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            clusters = session.exec(
                select(ClusterAssignment).where(ClusterAssignment.run_id == run.run_id)
            ).all()

        assert len(clusters) >= 1, "clusters staging table must have rows after Stage 6"

    def test_canonical_entities_created(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            entities = session.exec(
                select(CanonicalEntity).where(CanonicalEntity.last_run_id == run.run_id)
            ).all()

        assert len(entities) >= 1

    def test_every_source_record_has_exactly_one_crosswalk_row(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            xwalks = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run.run_id)
            ).all()

        assert len(xwalks) == seed["source_count"]

    def test_crosswalk_reflects_probabilistic_match_method_and_score(self, seeded_engine):
        """Stage 7 must propagate Splink match_method and match_score onto crosswalk."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine, config=_PROBABILISTIC_XWALK_CONFIG)

        with Session(engine) as session:
            xwalks = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run.run_id)
            ).all()

        probabilistic_rows = [
            xw for xw in xwalks if xw.match_method == MatchMethod.probabilistic
        ]
        assert len(probabilistic_rows) >= 1, (
            "entity_crosswalk must include rows merged via the probabilistic path "
            "when Splink auto-merges are enabled"
        )
        for xw in probabilistic_rows:
            assert xw.match_score is not None, (
                "probabilistic crosswalk rows must carry the Splink match_score"
            )
            assert 0.0 <= xw.match_score <= 1.0, (
                f"match_score {xw.match_score} out of [0, 1]"
            )

        exact_rows = [xw for xw in xwalks if xw.match_method == MatchMethod.exact]
        assert len(exact_rows) >= 1, "singleton or fastpath rows should remain exact"
        assert all(xw.match_score is None for xw in exact_rows)

    def test_canonical_count_does_not_exceed_source_count(self, seeded_engine):
        """canonical_out ≤ source count — merges reduce, never inflate."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.canonical_out <= seed["source_count"]

    def test_no_auto_published_cluster_exceeds_max_cluster_size(self, seeded_engine):
        """No cluster in ClusterAssignment with held_for_review=False exceeds cap."""
        engine, seed = seeded_engine
        max_size = _TEST_CONFIG["max_cluster_size"]
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            rows = session.exec(
                select(ClusterAssignment).where(
                    ClusterAssignment.run_id == run.run_id,
                    ClusterAssignment.held_for_review == False,  # noqa: E712
                )
            ).all()

        cluster_sizes: dict[str, int] = defaultdict(int)
        for row in rows:
            cluster_sizes[row.cluster_id] += 1

        for cluster_id, size in cluster_sizes.items():
            assert (
                size <= max_size
            ), f"Auto-published cluster {cluster_id} has size {size} > cap {max_size}"

    def test_config_json_contains_thresholds(self, seeded_engine):
        """match_run.config_json must record auto_threshold and review_threshold."""
        engine, seed = seeded_engine
        run = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        cfg = json.loads(match_run.config_json)
        assert "auto_threshold" in cfg
        assert "review_threshold" in cfg
        assert "max_cluster_size" in cfg

    def test_pipeline_on_empty_state_completes_cleanly(self, engine):
        """No source data → run still completes with zero records."""
        with Session(engine) as session:
            empty_state = State(code="OK", name="Oklahoma")
            session.add(empty_state)
            session.commit()

        config = {**_TEST_CONFIG, "state_code": "OK"}
        run = _run_phase2_pipeline(engine, config=config)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.status == RunStatus.completed
        assert match_run.records_in == 0
        assert match_run.canonical_out == 0


# ---------------------------------------------------------------------------
# Idempotency tests
# ---------------------------------------------------------------------------


class TestPhase2Idempotency:
    """Two runs on the same source data produce identical cluster structures."""

    def test_two_runs_produce_identical_cluster_structure(self, seeded_engine):
        engine, seed = seeded_engine
        run1 = _run_phase2_pipeline(engine)
        run2 = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            structure1 = _crosswalk_cluster_structure(session, run1.run_id)
            structure2 = _crosswalk_cluster_structure(session, run2.run_id)

        assert structure1 == structure2

    def test_second_run_status_is_completed(self, seeded_engine):
        engine, seed = seeded_engine
        _run_phase2_pipeline(engine)
        run2 = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            match_run2 = session.get(MatchRun, run2.run_id)

        assert match_run2.status == RunStatus.completed

    def test_canonical_entity_count_stable_across_runs(self, seeded_engine):
        """Two runs must not accumulate duplicate live canonical rows."""
        engine, seed = seeded_engine
        _run_phase2_pipeline(engine)
        run2 = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            live_count = len(session.exec(select(CanonicalEntity)).all())
            mr2 = session.get(MatchRun, run2.run_id)

        assert live_count == mr2.canonical_out
        assert live_count <= seed["source_count"]

    def test_canonical_out_is_identical_across_runs(self, seeded_engine):
        engine, seed = seeded_engine
        run1 = _run_phase2_pipeline(engine)
        run2 = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            mr1 = session.get(MatchRun, run1.run_id)
            mr2 = session.get(MatchRun, run2.run_id)

        assert mr1.canonical_out == mr2.canonical_out

    def test_cluster_member_counts_match_across_runs(self, seeded_engine):
        """Each cluster in run 1 has a same-size counterpart in run 2."""
        engine, seed = seeded_engine
        run1 = _run_phase2_pipeline(engine)
        run2 = _run_phase2_pipeline(engine)

        with Session(engine) as session:
            s1 = _crosswalk_cluster_structure(session, run1.run_id)
            s2 = _crosswalk_cluster_structure(session, run2.run_id)

        sizes1 = sorted(len(c) for c in s1)
        sizes2 = sorted(len(c) for c in s2)
        assert sizes1 == sizes2


# ---------------------------------------------------------------------------
# Phase 2 config acceptance tests
# ---------------------------------------------------------------------------


class TestPhase2Config:
    """Verify starting thresholds behave correctly end-to-end."""

    def test_production_thresholds_complete_pipeline(self, seeded_engine):
        """Default production thresholds (0.99/0.80) must not crash the pipeline."""
        engine, seed = seeded_engine
        prod_config = {
            "state_code": "TX",
            "pass_type": "entity",
            "auto_threshold": 0.99,
            "review_threshold": 0.80,
            "max_cluster_size": 200,
            "seed": 42,
        }
        run = _run_phase2_pipeline(engine, config=prod_config)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.status == RunStatus.completed

    def test_mega_cluster_cap_routes_large_clusters_to_review(self, engine):
        """A max_cluster_size of 2 routes any 3+ member cluster to merge_review."""
        with Session(engine) as session:
            state = State(code="TX", name="Texas")
            session.add(state)
            session.flush()

            addr = UnifiedAddress(street_1="1 A St", city="Austin", state="TX", zip_code="78701")
            session.add(addr)
            session.flush()

            # Three identically-named persons → fastpath merges into one 3-member cluster
            for i in range(3):
                session.add(
                    UnifiedPerson(
                        first_name="Bob", last_name="Jones", state_id=state.id, address_id=addr.id
                    )
                )
            session.commit()

        config = {
            "state_code": "TX",
            "pass_type": "entity",
            "auto_threshold": 0.99,
            "review_threshold": 0.80,
            "max_cluster_size": 2,  # cap at 2; a 3-member cluster is held
            "seed": 42,
        }
        run = _run_phase2_pipeline(engine, config=config)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)
            held_rows = session.exec(
                select(ClusterAssignment).where(
                    ClusterAssignment.run_id == run.run_id,
                    ClusterAssignment.held_for_review == True,  # noqa: E712
                )
            ).all()

        assert match_run.status == RunStatus.completed
        assert len(held_rows) >= 1, "mega-cluster guard must hold ≥1 cluster when cap exceeded"
