"""Reversibility tests: unmerge a resolution run.

TDD steps from task-3c-reversibility.md:

- Step 1/2: Core reversibility test (seeded fixture → run 1 → snapshot →
  run 2 → unmerge run 2 → assert cluster structure matches snapshot).
- Step 4: can_unmerge guards; transactionality; decided prior-run
  merge_review rows survive reversal; no-prior-run path.

Task: 3c | Branch: resolve/phase-3/task-3c-reversibility
"""

from __future__ import annotations

from collections import defaultdict
from unittest.mock import patch

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
    MatchRun,
    MergeReview,
    ReviewStatus,
    SourceType,
)
from app.resolve.reverse import RunReversal, can_unmerge, unmerge_run
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
# Stage list (Phase 1 only)
# ---------------------------------------------------------------------------

PHASE1_STAGES = [
    stage1_build_resolution_input,
    run_blocking_stage,
    run_fastpath_stage,
    run_survivorship_stage,
]

# ---------------------------------------------------------------------------
# Tables — explicit list to avoid FK resolution errors from other modules.
# Order follows FK dependency: FK targets before dependents.
# ---------------------------------------------------------------------------

_TABLES_TO_CREATE = [
    State.__table__,
    UnifiedAddress.__table__,
    UnifiedPerson.__table__,
    UnifiedCommittee.__table__,
    UnifiedEntity.__table__,
    MatchRun.__table__,
    ResolutionInput.__table__,
    CandidatePair.__table__,
    MergeEdge.__table__,
    MatchDecision.__table__,
    MergeReview.__table__,
    CanonicalAddress.__table__,
    CanonicalEntity.__table__,
    CanonicalCampaign.__table__,
    CanonicalNameHistory.__table__,
    EntityCrosswalk.__table__,
    AddressCrosswalk.__table__,
    CampaignCrosswalk.__table__,
    ClusterAssignment.__table__,
]


# ---------------------------------------------------------------------------
# Engine fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """In-memory SQLite engine with Phase 1 tables.

    StaticPool ensures every Session shares the same underlying connection so
    data committed in one session is visible in the next.
    """
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng, tables=_TABLES_TO_CREATE)
    return eng


# ---------------------------------------------------------------------------
# Seed helper
# ---------------------------------------------------------------------------


def _seed_source_data(session: Session, state_code: str = "TX") -> dict:
    """Insert minimal unified source records and return seed metadata.

    Fixture:
      - State TX
      - UnifiedAddress A1 (parseable US address)
      - UnifiedPerson P1 (John Smith, addr=A1)  ─┐ identical name+address
      - UnifiedPerson P2 (John Smith, addr=A1)  ─┘ → fastpath merges them
      - UnifiedPerson P3 (Jane Doe, no address)   → singleton
      - UnifiedCommittee C1                        → singleton

    Expected: 3 canonical entities from 4 source records (or 4 if address
    parse fails — both are valid completed-run outcomes).
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
        "source_count": 4,
    }


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _run_pipeline(engine, state_code: str = "TX") -> ResolutionRun:
    """Execute the full Phase 1 pipeline on *engine* and return the run object."""
    config = {"state_code": state_code, "pass_type": "entity"}
    run = ResolutionRun(state_code=state_code, config=config)
    with Session(engine) as session:
        run.run(session, PHASE1_STAGES)
    return run


# ---------------------------------------------------------------------------
# Snapshot helper — ID-agnostic cluster structure capture
# ---------------------------------------------------------------------------


def _canonical_state_snapshot(session: Session, run_id: int) -> frozenset:
    """Capture the cluster structure for *run_id* as a comparable frozenset.

    Returns a frozenset where each element is a ``(members, canonical_name)``
    tuple:

    - *members* — frozenset of ``(source_type_str, source_id)`` pairs in one
      cluster.
    - *canonical_name* — the ``CanonicalEntity.canonical_name`` for that
      cluster.

    This representation is stable across re-runs because it does not depend on
    integer primary keys (``id``, ``canonical_entity_id``) which change when
    survivorship rebuilds the canonical layer.
    """
    xwalks = session.exec(select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)).all()

    # Map canonical_entity_id → list of (source_type, source_id)
    clusters: dict[int, list[tuple[str, str]]] = defaultdict(list)
    for xw in xwalks:
        st = xw.source_type.value if hasattr(xw.source_type, "value") else str(xw.source_type)
        clusters[xw.canonical_entity_id].append((st, xw.source_id))

    # Fetch live canonical entities (IDs may differ after rebuild)
    entity_ids = list(clusters.keys())
    if not entity_ids:
        return frozenset()

    entities: dict[int, CanonicalEntity] = {
        e.id: e
        for e in session.exec(
            select(CanonicalEntity).where(CanonicalEntity.id.in_(entity_ids))
        ).all()
    }

    return frozenset(
        (frozenset(members), entities[eid].canonical_name)
        for eid, members in clusters.items()
        if eid in entities
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def seeded_engine(engine):
    """Engine with source data pre-seeded (one pipeline run not yet executed)."""
    with Session(engine) as session:
        _seed_source_data(session)
    return engine


@pytest.fixture()
def seeded_engine_two_runs(seeded_engine):
    """Engine with source data seeded and two completed pipeline runs."""
    engine = seeded_engine
    run1 = _run_pipeline(engine)
    run2 = _run_pipeline(engine)
    return engine, run1.run_id, run2.run_id


# ---------------------------------------------------------------------------
# Tests — can_unmerge guard
# ---------------------------------------------------------------------------


class TestCanUnmergeGuard:
    """Verify the can_unmerge guard returns correct (bool, reason) tuples."""

    def test_returns_true_for_latest_completed_run(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs
        with Session(engine) as session:
            ok, reason = can_unmerge(session, run2_id)
        assert ok is True
        assert reason == ""

    def test_rejects_non_latest_completed_run(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs
        with Session(engine) as session:
            ok, reason = can_unmerge(session, run1_id)
        assert ok is False
        assert str(run2_id) in reason

    def test_rejects_nonexistent_run(self, engine):
        with Session(engine) as session:
            ok, reason = can_unmerge(session, 99999)
        assert ok is False
        assert "not found" in reason

    def test_rejects_already_reverted_run(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs
        with Session(engine) as session:
            unmerge_run(session, run2_id)
        # run2 is now reverted; attempting it again must fail
        with Session(engine) as session:
            ok, reason = can_unmerge(session, run2_id)
        assert ok is False
        assert "reverted" in reason

    def test_after_reverting_run2_run1_becomes_latest(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs
        with Session(engine) as session:
            unmerge_run(session, run2_id)
        with Session(engine) as session:
            ok, _ = can_unmerge(session, run1_id)
        assert ok is True

    def test_rejects_running_status(self, seeded_engine):
        """Finding 5: can_unmerge returns False for status=running."""
        engine = seeded_engine
        run = _run_pipeline(engine)

        from sqlalchemy import text as _text

        with Session(engine) as session:
            session.execute(
                _text("UPDATE match_run SET status = :status WHERE id = :rid"),
                {"status": "running", "rid": run.run_id},
            )
            session.commit()
            ok, reason = can_unmerge(session, run.run_id)
        assert ok is False
        assert "running" in reason

    def test_rejects_failed_status(self, seeded_engine):
        """Finding 5: can_unmerge returns False for status=failed."""
        engine = seeded_engine
        run = _run_pipeline(engine)

        from sqlalchemy import text as _text

        with Session(engine) as session:
            session.execute(
                _text("UPDATE match_run SET status = :status WHERE id = :rid"),
                {"status": "failed", "rid": run.run_id},
            )
            session.commit()
            ok, reason = can_unmerge(session, run.run_id)
        assert ok is False
        assert "failed" in reason


# ---------------------------------------------------------------------------
# Core reversibility test (Step 1 / Step 3)
# ---------------------------------------------------------------------------


class TestUnmergeRestoresCanonicalState:
    """Merge → second run → unmerge restores canonical state exactly."""

    def test_reversibility_restores_cluster_structure(self, seeded_engine_two_runs):
        """After unmerge_run(run2), entity_crosswalk + canonical_entity for
        run1 are structurally identical to the snapshot taken right after run1.

        'Structurally identical' means the same cluster groupings (same source
        records together) and same canonical names — independent of integer IDs
        which change when survivorship rebuilds the layer.
        """
        engine, run1_id, run2_id = seeded_engine_two_runs

        # Snapshot state immediately after run 1
        with Session(engine) as session:
            snap_before = _canonical_state_snapshot(session, run1_id)

        assert len(snap_before) >= 1, "run 1 must produce at least one cluster"

        # Unmerge run 2
        with Session(engine) as session:
            reversal = unmerge_run(session, run2_id)

        assert isinstance(reversal, RunReversal)

        # Snapshot state after reversal (reads run1's newly rebuilt crosswalk)
        with Session(engine) as session:
            snap_after = _canonical_state_snapshot(session, run1_id)

        assert (
            snap_after == snap_before
        ), "cluster structure after unmerging run2 must match snapshot after run1"

    def test_unmerge_removes_run2_crosswalk_rows(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            before = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)
            ).all()
        assert len(before) > 0

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            after = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)
            ).all()
        assert len(after) == 0

    def test_unmerge_removes_run2_match_decisions(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            decisions = session.exec(
                select(MatchDecision).where(MatchDecision.run_id == run2_id)
            ).all()
        assert len(decisions) == 0

    def test_unmerge_marks_run_as_reverted(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        # Read status via raw SQL to avoid enum coercion on "reverted"
        from sqlalchemy import text as _text

        with Session(engine) as session:
            row = session.execute(
                _text("SELECT status FROM match_run WHERE id = :rid"),
                {"rid": run2_id},
            ).first()
        assert row is not None
        assert row[0] == "reverted"

    def test_run1_remains_completed_after_run2_reverted(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            run1 = session.get(MatchRun, run1_id)
        assert run1 is not None
        assert run1.status.value == "completed"

    def test_canonical_entity_count_restored_after_reversal(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            count_after_run1 = len(session.exec(select(CanonicalEntity)).all())

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            count_after_reversal = len(session.exec(select(CanonicalEntity)).all())

        assert count_after_reversal == count_after_run1

    def test_reversal_summary_reports_correct_counts(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            run2_ec_before = len(
                session.exec(select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)).all()
            )

        with Session(engine) as session:
            reversal = unmerge_run(session, run2_id)

        assert reversal.run_id == run2_id
        assert reversal.entity_crosswalk_removed == run2_ec_before
        assert reversal.canonical_rows_rebuilt >= 0


# ---------------------------------------------------------------------------
# Step 4 additional tests
# ---------------------------------------------------------------------------


class TestCanUnmergeNonLatest:
    """can_unmerge refuses a non-latest run."""

    def test_cannot_unmerge_run1_when_run2_exists(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs
        with Session(engine) as session:
            ok, reason = can_unmerge(session, run1_id)
        assert not ok
        assert str(run2_id) in reason, "error should mention the blocking run"

    def test_unmerge_raises_value_error_when_can_unmerge_blocks(self, seeded_engine_two_runs):
        """Finding 6: unmerge_run raises ValueError when can_unmerge blocks."""
        engine, run1_id, run2_id = seeded_engine_two_runs
        with Session(engine) as session:
            with pytest.raises(ValueError, match=f"Cannot unmerge run_id={run1_id}"):
                unmerge_run(session, run1_id)


class TestReversalTransactionality:
    """Reversal is transactional: a mid-reversal failure leaves no partial changes."""

    def test_failed_reversal_rolls_back_deletions(self, seeded_engine_two_runs):
        """If an error occurs before survivorship commits, entity_crosswalk rows
        for run2 must be intact after the caller rolls back the session.
        """
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            xwalk_count_before = len(
                session.exec(select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)).all()
            )
        assert xwalk_count_before > 0

        # Patch run_survivorship_stage (imported at module level in reverse.py)
        # so it raises before issuing any commit.  This simulates a failure
        # after the ORM deletes but before the transaction is committed.
        with patch(
            "app.resolve.reverse.run_survivorship_stage",
            side_effect=RuntimeError("simulated mid-reversal failure"),
        ):
            with Session(engine) as session:
                try:
                    unmerge_run(session, run2_id)
                except RuntimeError:
                    session.rollback()

        # After rollback, run2's crosswalk rows must still exist
        with Session(engine) as session:
            xwalk_count_after = len(
                session.exec(select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)).all()
            )
        assert (
            xwalk_count_after == xwalk_count_before
        ), "entity_crosswalk rows must be intact after a rolled-back reversal"

    def test_failed_reversal_run_status_unchanged(self, seeded_engine_two_runs):
        """Run status must remain 'completed' if the reversal fails and rolls back."""
        engine, run1_id, run2_id = seeded_engine_two_runs

        from sqlalchemy import text as _text

        with patch(
            "app.resolve.reverse.run_survivorship_stage",
            side_effect=RuntimeError("simulated failure"),
        ):
            with Session(engine) as session:
                try:
                    unmerge_run(session, run2_id)
                except RuntimeError:
                    session.rollback()

        with Session(engine) as session:
            row = session.execute(
                _text("SELECT status FROM match_run WHERE id = :rid"),
                {"rid": run2_id},
            ).first()
        assert row[0] == "completed"


class TestDecidedPriorRunMergeReviewRowsSurvive:
    """Decided merge_review rows from prior runs are not deleted by reversal."""

    def test_approved_review_from_run1_survives_run2_reversal(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        # Create an approved merge_review row attributed to run1
        with Session(engine) as session:
            prior_review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id="prior-a",
                source_b_type=SourceType.unified_person,
                source_b_id="prior-b",
                status=ReviewStatus.approved,
            )
            session.add(prior_review)
            session.commit()
            prior_review_id = prior_review.id

        # Unmerge run2 — must not touch run1's review row
        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            surviving = session.get(MergeReview, prior_review_id)
        assert surviving is not None, "approved prior-run merge_review must survive reversal"
        assert surviving.status == ReviewStatus.approved

    def test_rejected_review_from_run1_survives_run2_reversal(self, seeded_engine_two_runs):
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            prior_review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id="prior-x",
                source_b_type=SourceType.unified_person,
                source_b_id="prior-y",
                status=ReviewStatus.rejected,
            )
            session.add(prior_review)
            session.commit()
            prior_review_id = prior_review.id

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            surviving = session.get(MergeReview, prior_review_id)
        assert surviving is not None
        assert surviving.status == ReviewStatus.rejected

    def test_run2_merge_review_rows_are_deleted(self, seeded_engine_two_runs):
        """merge_review rows surfaced by run2 must be removed on reversal."""
        engine, run1_id, run2_id = seeded_engine_two_runs

        with Session(engine) as session:
            run2_review = MergeReview(
                run_id=run2_id,
                source_a_type=SourceType.unified_person,
                source_a_id="r2-a",
                source_b_type=SourceType.unified_person,
                source_b_id="r2-b",
                status=ReviewStatus.pending,
            )
            session.add(run2_review)
            session.commit()
            run2_review_id = run2_review.id

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            gone = session.get(MergeReview, run2_review_id)
        assert gone is None, "pending merge_review from the reverted run must be deleted"


class TestNoPriorRunPath:
    """When there is no prior run, canonical tables are left empty."""

    def test_canonical_empty_after_reverting_only_run(self, seeded_engine):
        engine = seeded_engine
        run1 = _run_pipeline(engine)

        with Session(engine) as session:
            reversal = unmerge_run(session, run1.run_id)

        assert reversal.canonical_rows_rebuilt == 0

        with Session(engine) as session:
            entities = session.exec(select(CanonicalEntity)).all()
        assert len(entities) == 0

    def test_reversal_run_marked_reverted_with_no_prior_run(self, seeded_engine):
        engine = seeded_engine
        run1 = _run_pipeline(engine)

        with Session(engine) as session:
            unmerge_run(session, run1.run_id)

        from sqlalchemy import text as _text

        with Session(engine) as session:
            row = session.execute(
                _text("SELECT status FROM match_run WHERE id = :rid"),
                {"rid": run1.run_id},
            ).first()
        assert row[0] == "reverted"

    def test_cannot_unmerge_after_reverting_only_run(self, seeded_engine):
        engine = seeded_engine
        run1 = _run_pipeline(engine)

        with Session(engine) as session:
            unmerge_run(session, run1.run_id)

        with Session(engine) as session:
            ok, reason = can_unmerge(session, run1.run_id)
        assert not ok
        assert "reverted" in reason
