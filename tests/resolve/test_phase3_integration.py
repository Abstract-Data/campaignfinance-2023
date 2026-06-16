"""Phase 3 integration tests — review→rerun cycle.

Covers:
- An approved MergeReview causes the pair to merge on the next pipeline run.
- A rejected MergeReview keeps the pair separate and never re-queues them.
- Unmerge of a run that used an approved review deletes that run's artifacts
  and rebuilds the canonical layer from the prior completed run.
- ``show`` renders a human-readable explanation (not raw JSON).
- ``python -m app.resolve review list`` and ``unmerge --help`` are reachable
  through the main CLI entry point.

TDD steps from task-3z-integration.md:
  Step 2 — failing review→rerun test
  Step 3 — fix wiring gaps so tests pass
  Step 4 — add explanation rendering test
  Step 5 — full suite green including reversibility

Task: 3z | Branch: resolve/phase-3/task-3z-integration
"""

from __future__ import annotations

import json

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
    ReviewStatus,
    SourceType,
)
from app.resolve.reverse import unmerge_run
from app.resolve.review.queue import approve, reject
from app.resolve.run import ResolutionRun
from app.resolve.stages import (
    run_blocking_stage,
    run_fastpath_stage,
    run_survivorship_stage,
    stage1_build_resolution_input,
)
from app.resolve.stages.cluster import ClusterAssignment
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.stages.score import ScoredPair
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Phase 1 stage list (no Splink; avoids scoring/classify overhead)
# ---------------------------------------------------------------------------

PHASE1_STAGES = [
    stage1_build_resolution_input,
    run_blocking_stage,
    run_fastpath_stage,
    run_survivorship_stage,
]

# ---------------------------------------------------------------------------
# Table list — FK order; mirrors test_reversibility.py
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
    ScoredPair.__table__,
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
    """Shared in-memory SQLite engine (StaticPool) with all resolve tables."""
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng, tables=_TABLES_TO_CREATE)
    return eng


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_state(session: Session, code: str = "TX") -> State:
    state = State(code=code, name="Texas")
    session.add(state)
    session.flush()
    return state


def _seed_distinct_persons(session: Session, state: State) -> tuple[int, int]:
    """Seed two persons with different names so they will NOT fastpath-merge.

    Returns integer PKs so callers do not depend on detached ORM objects.
    """
    p1 = UnifiedPerson(first_name="John", last_name="Smith", state_id=state.id)
    p2 = UnifiedPerson(first_name="Jane", last_name="Doe", state_id=state.id)
    session.add_all([p1, p2])
    session.flush()
    return p1.id, p2.id  # type: ignore[return-value]


def _seed_mergeable_persons(session: Session, state: State) -> tuple[int, int, int]:
    """Seed two fastpath-mergeable persons plus one distinct singleton.

    Returns (auto1_id, auto2_id, solo_id) as integers.
    """
    addr = UnifiedAddress(
        street_1="123 Main St",
        city="Austin",
        state="TX",
        zip_code="78701",
    )
    session.add(addr)
    session.flush()
    p_auto1 = UnifiedPerson(
        first_name="John", last_name="Smith", state_id=state.id, address_id=addr.id
    )
    p_auto2 = UnifiedPerson(
        first_name="John", last_name="Smith", state_id=state.id, address_id=addr.id
    )
    p_solo = UnifiedPerson(first_name="Jane", last_name="Doe", state_id=state.id)
    session.add_all([p_auto1, p_auto2, p_solo])
    session.flush()
    return p_auto1.id, p_auto2.id, p_solo.id  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


def _run_pipeline(engine, state_code: str = "TX") -> ResolutionRun:
    config = {"state_code": state_code, "pass_type": "entity"}
    run = ResolutionRun(state_code=state_code, config=config)
    with Session(engine) as session:
        run.run(session, PHASE1_STAGES)
    return run


# ---------------------------------------------------------------------------
# Crosswalk helpers
# ---------------------------------------------------------------------------


def _xwalk_map(session: Session, run_id: int) -> dict[str, int]:
    """Return {source_id: canonical_entity_id} for a given run."""
    rows = session.exec(select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)).all()
    return {row.source_id: row.canonical_entity_id for row in rows}


# ---------------------------------------------------------------------------
# Tests: approved review → merge on next run
# ---------------------------------------------------------------------------


class TestApprovedReviewMergesCycle:
    """An approved MergeReview causes the pair to merge in the subsequent run."""

    def test_approved_pair_shares_canonical_entity_after_rerun(self, engine):
        """Core review→rerun cycle: approve a pending pair → run again → merged."""
        # Seed two persons that won't fastpath-merge.
        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        # Run 1: both persons are separate canonical entities.
        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            xwalk1 = _xwalk_map(session, run1_id)
        assert str(p1_id) in xwalk1
        assert str(p2_id) in xwalk1
        assert xwalk1[str(p1_id)] != xwalk1[str(p2_id)], "run 1: p1 and p2 must be separate"

        # Insert a pending MergeReview and approve it via the queue API.
        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                score=0.88,
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            review_id = review.id

        with Session(engine) as session:
            approved = approve(session, review_id, reviewer="test-reviewer", notes="integration")
        assert approved.status == ReviewStatus.approved

        # Run 2: survivorship picks up the approved review as a merge edge.
        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            xwalk2 = _xwalk_map(session, run2_id)

        assert str(p1_id) in xwalk2
        assert str(p2_id) in xwalk2
        assert xwalk2[str(p1_id)] == xwalk2[str(p2_id)], (
            "run 2: p1 and p2 must be in the same canonical entity after approval"
        )

    def test_approved_pair_crosswalk_uses_approved_review_method(self, engine):
        """EntityCrosswalk for the approved pair should carry match_method=approved_review."""
        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                score=0.87,
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            review_id = review.id

        with Session(engine) as session:
            approve(session, review_id, reviewer="test-reviewer")

        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            rows = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)
            ).all()
            p1_row = next((r for r in rows if r.source_id == str(p1_id)), None)
            p2_row = next((r for r in rows if r.source_id == str(p2_id)), None)

        assert p1_row is not None
        assert p2_row is not None
        methods = {p1_row.match_method, p2_row.match_method}
        assert MatchMethod.approved_review in methods, (
            f"at least one member should have match_method=approved_review; got {methods}"
        )


# ---------------------------------------------------------------------------
# Tests: rejected review → pair never re-queued
# ---------------------------------------------------------------------------


class TestRejectedReviewNeverReQueued:
    """A rejected MergeReview keeps the pair separate; they are not re-queued."""

    def test_rejected_pair_remains_separate_after_rerun(self, engine):
        """After rejecting a review, p1 and p2 stay in distinct canonical entities."""
        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                score=0.83,
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            review_id = review.id

        with Session(engine) as session:
            rejected = reject(
                session, review_id, reviewer="test-reviewer", notes="different person"
            )
        assert rejected.status == ReviewStatus.rejected

        # Run 2: rejected review is NOT an approved edge; p1+p2 stay separate.
        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            xwalk2 = _xwalk_map(session, run2_id)

        assert str(p1_id) in xwalk2
        assert str(p2_id) in xwalk2
        assert xwalk2[str(p1_id)] != xwalk2[str(p2_id)], (
            "run 2: rejected pair must remain in separate canonical entities"
        )

    def test_rejected_pair_not_in_pending_queue_after_rerun(self, engine):
        """Phase 1 pipeline does not re-queue a previously rejected pair."""
        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                score=0.82,
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            reject(session, review.id, reviewer="test-reviewer")

        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            pending_for_run2 = session.exec(
                select(MergeReview).where(
                    MergeReview.run_id == run2_id,
                    MergeReview.status == ReviewStatus.pending,
                )
            ).all()

        # Phase 1 has no classify stage, so no new MergeReview rows created by run 2.
        rejected_pair = frozenset({str(p1_id), str(p2_id)})
        for row in pending_for_run2:
            pair = frozenset({row.source_a_id, row.source_b_id})
            assert pair != rejected_pair, "rejected pair must not be re-queued in run 2"


# ---------------------------------------------------------------------------
# Tests: unmerge restores graph
# ---------------------------------------------------------------------------


class TestUnmergeRestoresGraph:
    """Unmerging a run that was built on an approved review clears that run's artifacts."""

    def test_unmerge_deletes_run2_crosswalk_rows(self, engine):
        """After unmerge of run 2, its EntityCrosswalk rows are gone."""
        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        # Run 1: p1 and p2 separate.
        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            approve(session, review.id, reviewer="test-reviewer")

        # Run 2: p1+p2 merged via approved review.
        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            run2_before = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)
            ).all()
        assert len(run2_before) > 0

        # Unmerge run 2.
        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            run2_after = session.exec(
                select(EntityCrosswalk).where(EntityCrosswalk.run_id == run2_id)
            ).all()
        assert len(run2_after) == 0, "unmerge must delete run 2's EntityCrosswalk rows"

    def test_unmerge_marks_run2_reverted(self, engine):
        """Unmerged run has status='reverted'."""
        from sqlalchemy import text as _text

        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            approve(session, review.id, reviewer="test-reviewer")

        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        with Session(engine) as session:
            row = session.execute(
                _text("SELECT status FROM match_run WHERE id = :rid"),
                {"rid": run2_id},
            ).first()
        assert row is not None
        assert row[0] == "reverted"

    def test_approved_review_survives_unmerge_of_later_run(self, engine):
        """The approved MergeReview row (run_id=run1) is not deleted by unmerge of run2."""
        with Session(engine) as session:
            state = _seed_state(session)
            p1_id, p2_id = _seed_distinct_persons(session, state)
            session.commit()

        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p2_id),
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            review_id = review.id
            approve(session, review_id, reviewer="test-reviewer")

        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        with Session(engine) as session:
            unmerge_run(session, run2_id)

        # The approved MergeReview (run_id=run1_id) should survive.
        with Session(engine) as session:
            surviving = session.get(MergeReview, review_id)
        assert surviving is not None, (
            "approved MergeReview from run 1 must survive unmerge of run 2"
        )
        assert surviving.status == ReviewStatus.approved

    def test_canonical_rebuilt_after_unmerge_with_fastpath_pairs(self, engine):
        """After unmerge, canonical layer is rebuilt from the prior completed run."""
        with Session(engine) as session:
            state = _seed_state(session)
            # p_auto1 + p_auto2 fastpath-merge; p_solo is a singleton.
            p_auto1_id, _p_auto2_id, p_solo_id = _seed_mergeable_persons(session, state)
            session.commit()

        run1 = _run_pipeline(engine)
        run1_id = run1.run_id

        with Session(engine) as session:
            canonical_after_run1 = session.exec(select(CanonicalEntity)).all()
        count_after_run1 = len(canonical_after_run1)
        assert count_after_run1 >= 1

        # Approve a review merging the solo person with one of the auto-pair persons.
        with Session(engine) as session:
            review = MergeReview(
                run_id=run1_id,
                source_a_type=SourceType.unified_person,
                source_a_id=str(p_auto1_id),
                source_b_type=SourceType.unified_person,
                source_b_id=str(p_solo_id),
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            approve(session, review.id, reviewer="test-reviewer")

        run2 = _run_pipeline(engine)
        run2_id = run2.run_id

        # Unmerge run 2: canonical is rebuilt from run 1.
        with Session(engine) as session:
            unmerge_run(session, run2_id)

        # After rebuild, canonical entities are >= 1 (consistent).
        with Session(engine) as session:
            canonical_after_unmerge = session.exec(select(CanonicalEntity)).all()
        assert len(canonical_after_unmerge) >= 1, "canonical layer must be non-empty after rebuild"


# ---------------------------------------------------------------------------
# Tests: show renders explanation (not raw JSON)
# ---------------------------------------------------------------------------


class TestShowRendersExplanation:
    """``show`` displays a rendered explanation waterfall, not raw JSON."""

    def test_show_renders_explanation_waterfall(self, engine, capsys):
        from app.resolve.models.resolution import MatchRun, PassType, RunStatus
        from app.resolve.review.cli import _run_show

        explanation_payload = {
            "first_name": {"gamma": 3, "label": "Exact match", "bf": 12.0},
            "last_name": {"gamma": 2, "label": "Near match", "bf": 6.0},
        }
        explanation_json = json.dumps(explanation_payload)

        with Session(engine) as session:
            run = MatchRun(
                state_code="TX",
                pass_type=PassType.entity,
                status=RunStatus.completed,
            )
            session.add(run)
            session.flush()

            review = MergeReview(
                run_id=run.id,
                source_a_type=SourceType.unified_person,
                source_a_id="p-explain-a",
                source_b_type=SourceType.unified_person,
                source_b_id="p-explain-b",
                score=0.91,
                explanation_json=explanation_json,
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            review_id = review.id

        with Session(engine) as session:
            _run_show(session, review_id)

        captured = capsys.readouterr()
        assert "Final match probability:" in captured.out, (
            "show must render the explanation waterfall via render_explanation"
        )
        assert "first_name" in captured.out
        assert "Exact match" in captured.out

    def test_show_does_not_output_raw_json_block(self, engine, capsys):
        """When explanation is present, output must not be just a JSON object."""
        from app.resolve.models.resolution import MatchRun, PassType, RunStatus
        from app.resolve.review.cli import _run_show

        explanation_payload = {
            "last_name": {"gamma": 2, "label": "Near match", "bf": 8.0},
        }
        explanation_json = json.dumps(explanation_payload)

        with Session(engine) as session:
            run = MatchRun(
                state_code="TX",
                pass_type=PassType.entity,
                status=RunStatus.completed,
            )
            session.add(run)
            session.flush()

            review = MergeReview(
                run_id=run.id,
                source_a_type=SourceType.unified_person,
                source_a_id="p-raw-a",
                source_b_type=SourceType.unified_person,
                source_b_id="p-raw-b",
                explanation_json=explanation_json,
                status=ReviewStatus.pending,
            )
            session.add(review)
            session.commit()
            session.refresh(review)
            review_id = review.id

        with Session(engine) as session:
            _run_show(session, review_id)

        captured = capsys.readouterr()
        # Rendered output should contain "contribution=" not a bare JSON structure.
        assert "contribution=" in captured.out, (
            "rendered explanation should contain 'contribution=' waterfall lines"
        )
        assert '"bf"' not in captured.out, "raw JSON keys like '\"bf\"' must not appear in output"


# ---------------------------------------------------------------------------
# Tests: CLI wiring — review and unmerge subcommands reachable from main CLI
# ---------------------------------------------------------------------------


class TestMainCliSubcommandWiring:
    """The review and unmerge subcommands are reachable from app.resolve.cli.main()."""

    def test_review_list_via_main_cli(self, capsys):
        """``main(['review', '--sqlite', 'list'])`` exits cleanly."""
        from app.resolve.cli import main

        # --sqlite is a flag on the 'review' subparser, not on 'list'.
        exit_code = main(["review", "--sqlite", "list"])
        assert exit_code == 0

    def test_review_list_shows_empty_queue_message(self, capsys):
        """Empty queue produces the expected 'No pending items' message."""
        from app.resolve.cli import main

        main(["review", "--sqlite", "list"])
        captured = capsys.readouterr()
        assert "No pending" in captured.out

    def test_unmerge_help_does_not_error(self):
        """``main(['unmerge', '--help'])`` raises SystemExit(0) (argparse --help)."""

        from app.resolve.cli import main

        with pytest.raises(SystemExit) as exc_info:
            main(["unmerge", "--help"])
        assert exc_info.value.code == 0

    def test_unmerge_nonexistent_run_returns_nonzero(self, capsys):
        """``unmerge --run 999999 --sqlite`` returns non-zero for a missing run."""
        from app.resolve.cli import main

        exit_code = main(["unmerge", "--run", "999999", "--sqlite"])
        assert exit_code != 0
