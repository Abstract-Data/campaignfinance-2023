"""Phase 1 end-to-end integration tests.

Wires all four Phase 1 stages together and verifies the pipeline
contract on a small seeded fixture:

  Stage 1  build_resolution_input      (task-1c)
  Stage 2  run_blocking_stage          (task-1e)
  Stage 3  run_fastpath_stage          (task-1f)
  Stage 7  run_survivorship_stage      (task-1g)

TDD steps per task-1z brief:

- Step 2: E2E test — ``match_run.status=completed``, canonical rows
  created, one crosswalk row per source record.
- Step 4: Idempotency — two runs on the same fixture produce identical
  cluster structures (same source→canonical groupings).
"""

from __future__ import annotations

from collections import defaultdict

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

import app.resolve.models  # noqa: F401 — central ORM registry (UnifiedReport)

# Import unified source models so their tables can be referenced explicitly.
# We do NOT call SQLModel.metadata.create_all() without a table list because
# the global metadata spans the entire project.  Instead, _TABLES_TO_CREATE
# below enumerates exactly the tables needed for these tests.
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
    MatchRun,
    MergeReview,
    RunStatus,
)
from app.resolve.run import ResolutionRun
from app.resolve.stages import (
    run_blocking_stage,
    run_fastpath_stage,
    run_survivorship_stage,
    stage1_build_resolution_input,
)
from app.resolve.stages.cluster import ClusterAssignment
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Ordered Phase 1 stage list
# ---------------------------------------------------------------------------

PHASE1_STAGES = [
    stage1_build_resolution_input,  # stage 1 — standardize
    run_blocking_stage,  # stage 2 — blocking
    run_fastpath_stage,  # stage 3 — fast-path
    run_survivorship_stage,  # stage 7 — survivorship + publish
]

# ---------------------------------------------------------------------------
# Tables required for Phase 1 tests (explicit to avoid FK resolution errors
# from models in other modules not imported here, e.g. unified_reports).
# Order follows FK dependency: FK targets before dependents.
# ---------------------------------------------------------------------------

_TABLES_TO_CREATE = [
    # Unified source layer (Stage 1 queries these)
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
    MatchDecision.__table__,
    MergeReview.__table__,
    # Canonical layer (CanonicalAddress first — FK target for CanonicalEntity)
    CanonicalAddress.__table__,
    CanonicalEntity.__table__,
    CanonicalCampaign.__table__,
    CanonicalNameHistory.__table__,
    # Crosswalk tables (FK into match_run and canonical tables)
    EntityCrosswalk.__table__,
    AddressCrosswalk.__table__,
    CampaignCrosswalk.__table__,
    # Phase 2 staging table — queried by run_survivorship_stage; empty here
    # triggers the Phase 1 trivial-clustering fallback path.
    ClusterAssignment.__table__,
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """In-memory SQLite engine with Phase 1 tables created.

    Uses an explicit table list to avoid FK resolution errors for models
    defined in other modules (e.g. unified_reports).  StaticPool ensures
    every Session shares the same underlying connection so data committed
    in one session is visible in the next.
    """
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng, tables=_TABLES_TO_CREATE)
    return eng


def _seed_source_data(session: Session, state_code: str = "TX") -> dict:
    """Insert minimal unified source records.  Returns seed metadata.

    Fixture:
      - State TX
      - UnifiedAddress A1 (parseable US address)
      - UnifiedPerson P1 (John Smith, addr=A1)   ─┐ identical name+address
      - UnifiedPerson P2 (John Smith, addr=A1)   ─┘ → fastpath merges them
      - UnifiedPerson P3 (Jane Doe, no address)    → singleton
      - UnifiedCommittee C1                        → singleton

    Expected pipeline outcome (assuming address parses):
      3 canonical entities from 4 source records.
    Even if the address standardizer returns "unparsed", the pipeline
    should complete with 4 canonical entities (all singletons) — still a
    valid completed run with every source record crosswalked.
    """
    state = State(code=state_code, name="Texas")
    session.add(state)
    session.flush()

    addr = UnifiedAddress(
        street_1="123 Main St",
        city="Austin",
        state="TX",
        zip_code="78701",
    )
    session.add(addr)
    session.flush()

    p1 = UnifiedPerson(first_name="John", last_name="Smith", state_id=state.id, address_id=addr.id)
    p2 = UnifiedPerson(first_name="John", last_name="Smith", state_id=state.id, address_id=addr.id)
    p3 = UnifiedPerson(first_name="Jane", last_name="Doe", state_id=state.id)
    session.add_all([p1, p2, p3])
    session.flush()

    committee = UnifiedCommittee(
        filer_id="CMTE001", name="Texas Democratic Party", state_id=state.id
    )
    session.add(committee)
    session.commit()

    return {
        "state": state,
        "persons": [p1, p2, p3],
        "committees": [committee],
        "source_count": 4,  # 3 persons + 1 committee
    }


@pytest.fixture()
def seeded_engine(engine):
    """engine with minimal source data pre-seeded."""
    with Session(engine) as session:
        seed = _seed_source_data(session)
    return engine, seed


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _run_pipeline(engine, state_code: str = "TX") -> ResolutionRun:
    """Execute the full Phase 1 pipeline on *engine* and return the run object."""
    config = {"state_code": state_code, "pass_type": "entity"}
    run = ResolutionRun(state_code=state_code, config=config)
    with Session(engine) as session:
        run.run(session, PHASE1_STAGES)
    return run


def _crosswalk_cluster_structure(
    session: Session,
    run_id: int,
) -> frozenset[frozenset[tuple[str, str]]]:
    """Return the cluster structure for *run_id* as a frozenset of frozensets.

    Each inner frozenset contains ``(source_type_str, source_id)`` tuples for
    one canonical entity cluster.  Two runs are idempotent when their cluster
    structures are equal.
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


class TestPhase1EndToEnd:
    """Verify the pipeline completes and satisfies structural invariants."""

    def test_pipeline_status_is_completed(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run is not None
        assert match_run.status == RunStatus.completed

    def test_every_source_record_has_exactly_one_crosswalk_row(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            xwalks = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run.run_id)
            ).all()

        assert len(xwalks) == seed["source_count"]

    def test_canonical_entity_rows_are_created(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            entities = session.exec(
                select(CanonicalEntity).where(CanonicalEntity.last_run_id == run.run_id)
            ).all()

        assert len(entities) >= 1

    def test_canonical_out_count_is_positive(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.canonical_out >= 1

    def test_records_in_count_equals_source_count(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.records_in == seed["source_count"]

    def test_canonical_count_does_not_exceed_source_count(self, seeded_engine):
        """canonical_out ≤ source count (merges reduce or hold; never inflate)."""
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.canonical_out <= seed["source_count"]

    def test_match_run_has_finished_at_set(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.finished_at is not None

    def test_resolution_input_rows_created_for_run(self, seeded_engine):
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            inputs = session.exec(
                select(ResolutionInput).where(ResolutionInput.run_id == run.run_id)
            ).all()

        assert len(inputs) == seed["source_count"]

    def test_candidate_pairs_created_after_blocking(self, seeded_engine):
        """Stage 2 persists candidate pairs when duplicate names block together."""
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            pairs = session.exec(
                select(CandidatePair).where(CandidatePair.run_id == run.run_id)
            ).all()

        assert len(pairs) >= 1

    def test_match_decisions_created_after_fastpath(self, seeded_engine):
        """Stage 3 writes at least one decision when duplicates merge."""
        engine, seed = seeded_engine
        run = _run_pipeline(engine)

        with Session(engine) as session:
            decisions = session.exec(
                select(MatchDecision).where(MatchDecision.run_id == run.run_id)
            ).all()

        assert len(decisions) >= 1

    def test_pipeline_on_empty_state_completes_cleanly(self, engine):
        """No source data for a state → run still completes (zero records)."""
        with Session(engine) as session:
            empty_state = State(code="OK", name="Oklahoma")
            session.add(empty_state)
            session.commit()

        run = _run_pipeline(engine, state_code="OK")

        with Session(engine) as session:
            match_run = session.get(MatchRun, run.run_id)

        assert match_run.status == RunStatus.completed
        assert match_run.canonical_out == 0
        assert match_run.records_in == 0


# ---------------------------------------------------------------------------
# Idempotency tests
# ---------------------------------------------------------------------------


class TestPhase1Idempotency:
    """Two runs on the same source data produce identical cluster structures."""

    def test_two_runs_produce_identical_cluster_structure(self, seeded_engine):
        engine, seed = seeded_engine
        run1 = _run_pipeline(engine)
        run2 = _run_pipeline(engine)

        with Session(engine) as session:
            structure1 = _crosswalk_cluster_structure(session, run1.run_id)
            structure2 = _crosswalk_cluster_structure(session, run2.run_id)

        assert structure1 == structure2

    def test_second_run_status_is_completed(self, seeded_engine):
        engine, seed = seeded_engine
        _run_pipeline(engine)
        run2 = _run_pipeline(engine)

        with Session(engine) as session:
            match_run2 = session.get(MatchRun, run2.run_id)

        assert match_run2.status == RunStatus.completed

    def test_each_run_produces_independent_crosswalk_rows(self, seeded_engine):
        """Crosswalk rows are partitioned by run_id; runs are fully independent."""
        engine, seed = seeded_engine
        run1 = _run_pipeline(engine)
        run2 = _run_pipeline(engine)

        with Session(engine) as session:
            run1_xwalks = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run1.run_id)
            ).all()
            run2_xwalks = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2.run_id)
            ).all()

        assert len(run1_xwalks) == seed["source_count"]
        assert len(run2_xwalks) == seed["source_count"]

    def test_canonical_out_is_identical_across_runs(self, seeded_engine):
        engine, seed = seeded_engine
        run1 = _run_pipeline(engine)
        run2 = _run_pipeline(engine)

        with Session(engine) as session:
            mr1 = session.get(MatchRun, run1.run_id)
            mr2 = session.get(MatchRun, run2.run_id)

        assert mr1.canonical_out == mr2.canonical_out

    def test_canonical_entity_count_stable_across_runs(self, seeded_engine):
        """Two pipeline runs must not accumulate duplicate live canonical rows."""
        engine, seed = seeded_engine
        _run_pipeline(engine)
        run2 = _run_pipeline(engine)

        with Session(engine) as session:
            live_count = len(session.exec(select(CanonicalEntity)).all())
            mr2 = session.get(MatchRun, run2.run_id)

        assert live_count == mr2.canonical_out
        assert live_count <= seed["source_count"]

    def test_cluster_member_counts_match_across_runs(self, seeded_engine):
        """Each cluster in run 1 has a same-size counterpart in run 2."""
        engine, seed = seeded_engine
        run1 = _run_pipeline(engine)
        run2 = _run_pipeline(engine)

        with Session(engine) as session:
            s1 = _crosswalk_cluster_structure(session, run1.run_id)
            s2 = _crosswalk_cluster_structure(session, run2.run_id)

        sizes1 = sorted(len(c) for c in s1)
        sizes2 = sorted(len(c) for c in s2)
        assert sizes1 == sizes2
