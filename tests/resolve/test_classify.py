"""Task 2b tests for stage 5 probabilistic classification."""

from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.models.resolution import (
    DecisionBand,
    DecisionOutcome,
    MatchDecision,
    MatchMethod,
    MatchRun,
    PassType,
    ReviewStatus,
    RunStatus,
    SourceType,
)
from app.resolve.stages.classify import run_classify_stage
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.models.resolution import MergeReview
from app.resolve.stages.score import ScoredPair


def _make_engine():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(
        engine,
        tables=[
            MatchRun.__table__,
            ScoredPair.__table__,
            MatchDecision.__table__,
            MergeEdge.__table__,
            MergeReview.__table__,
        ],
    )
    return engine


def _seed_run(session: Session, run_id: int = 1) -> None:
    session.add(
        MatchRun(
            id=run_id,
            state_code="TX",
            pass_type=PassType.entity,
            status=RunStatus.running,
        )
    )
    session.commit()


def _add_scored_pair(
    session: Session,
    *,
    run_id: int,
    pair_id: str,
    score: float,
    entity_type: str = "person",
) -> None:
    session.add(
        ScoredPair(
            run_id=run_id,
            source_a_type="unified_person",
            source_a_id=f"{pair_id}-a",
            source_b_type="unified_person",
            source_b_id=f"{pair_id}-b",
            entity_type=entity_type,
            score=score,
            explanation_json=f'{{"pair": "{pair_id}"}}',
        )
    )


def _decision_by_pair(session: Session, run_id: int) -> dict[tuple[str, str], MatchDecision]:
    rows = session.exec(select(MatchDecision).where(MatchDecision.run_id == run_id)).all()
    return {(row.source_a_id, row.source_b_id): row for row in rows}


def test_classify_bands_and_writes_expected_downstream_rows():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=11)
        _add_scored_pair(session, run_id=11, pair_id="auto", score=0.999)
        _add_scored_pair(session, run_id=11, pair_id="review", score=0.90)
        _add_scored_pair(session, run_id=11, pair_id="reject", score=0.50)
        session.commit()

        result = run_classify_stage(session, run_id=11, config={})
        assert result == {"auto_merges": 1, "queued": 1, "rejected": 1}

        decisions = _decision_by_pair(session, run_id=11)
        assert len(decisions) == 3

        auto_decision = decisions[("auto-a", "auto-b")]
        assert auto_decision.band == DecisionBand.auto
        assert auto_decision.outcome == DecisionOutcome.merged
        assert auto_decision.explanation_json == '{"pair": "auto"}'

        review_decision = decisions[("review-a", "review-b")]
        assert review_decision.band == DecisionBand.review
        assert review_decision.outcome == DecisionOutcome.queued

        reject_decision = decisions[("reject-a", "reject-b")]
        assert reject_decision.band == DecisionBand.reject
        assert reject_decision.outcome == DecisionOutcome.rejected

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 11)).all()
        assert len(edges) == 1
        assert edges[0].source_a_id == "auto-a"
        assert edges[0].source_b_id == "auto-b"
        assert edges[0].edge_source == "probabilistic"

        review_rows = session.exec(select(MergeReview).where(MergeReview.run_id == 11)).all()
        assert len(review_rows) == 1
        assert review_rows[0].source_a_id == "review-a"
        assert review_rows[0].source_b_id == "review-b"
        assert review_rows[0].status == ReviewStatus.pending


def test_prior_review_decisions_override_score_and_requeue_rules():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=12)
        _add_scored_pair(session, run_id=12, pair_id="approved", score=0.10)
        _add_scored_pair(session, run_id=12, pair_id="rejected", score=0.95)
        session.add(
            MergeReview(
                run_id=3,
                source_a_type=SourceType.unified_person,
                source_a_id="approved-a",
                source_b_type=SourceType.unified_person,
                source_b_id="approved-b",
                status=ReviewStatus.approved,
            )
        )
        session.add(
            MergeReview(
                run_id=4,
                source_a_type=SourceType.unified_person,
                source_a_id="rejected-a",
                source_b_type=SourceType.unified_person,
                source_b_id="rejected-b",
                status=ReviewStatus.rejected,
            )
        )
        session.commit()

        result = run_classify_stage(session, run_id=12, config={})
        assert result == {"auto_merges": 1, "queued": 0, "rejected": 1}

        decisions = _decision_by_pair(session, run_id=12)
        approved = decisions[("approved-a", "approved-b")]
        assert approved.band == DecisionBand.auto
        assert approved.outcome == DecisionOutcome.merged

        rejected = decisions[("rejected-a", "rejected-b")]
        assert rejected.band == DecisionBand.reject
        assert rejected.outcome == DecisionOutcome.rejected

        edges = session.exec(
            select(MergeEdge).where(MergeEdge.run_id == 12).order_by(MergeEdge.id)
        ).all()
        assert len(edges) == 1
        assert edges[0].source_a_id == "approved-a"
        assert edges[0].source_b_id == "approved-b"
        assert edges[0].edge_source == "approved_review"

        pending_reviews = session.exec(
            select(MergeReview).where(
                MergeReview.run_id == 12,
                MergeReview.status == ReviewStatus.pending,
            )
        ).all()
        assert pending_reviews == []


def test_classify_honors_per_entity_type_threshold_overrides():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=13)
        _add_scored_pair(
            session,
            run_id=13,
            pair_id="person",
            entity_type="person",
            score=0.85,
        )
        _add_scored_pair(
            session,
            run_id=13,
            pair_id="committee",
            entity_type="committee",
            score=0.85,
        )
        session.commit()

        config = {
            "auto_threshold": 0.99,
            "review_threshold": 0.80,
            "threshold_overrides": {
                "person": {"auto_threshold": 0.90, "review_threshold": 0.80},
                "committee": {"auto_threshold": 0.80, "review_threshold": 0.70},
            },
        }
        result = run_classify_stage(session, run_id=13, config=config)
        assert result == {"auto_merges": 1, "queued": 1, "rejected": 0}

        decisions = _decision_by_pair(session, run_id=13)
        assert decisions[("person-a", "person-b")].band == DecisionBand.review
        assert decisions[("committee-a", "committee-b")].band == DecisionBand.auto


def test_classify_rerun_is_idempotent():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=14)
        _add_scored_pair(session, run_id=14, pair_id="auto", score=0.999)
        _add_scored_pair(session, run_id=14, pair_id="review", score=0.90)
        session.commit()

        first = run_classify_stage(session, run_id=14, config={})
        second = run_classify_stage(session, run_id=14, config={})
        assert first == second == {"auto_merges": 1, "queued": 1, "rejected": 0}

        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 14)
        ).all()
        assert len(decisions) == 2

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 14)).all()
        assert len(edges) == 1

        review_rows = session.exec(
            select(MergeReview).where(
                MergeReview.run_id == 14,
                MergeReview.status == ReviewStatus.pending,
            )
        ).all()
        assert len(review_rows) == 1


def test_classify_auto_threshold_boundary_includes_exact_score():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=15)
        _add_scored_pair(session, run_id=15, pair_id="boundary", score=0.99)
        session.commit()

        result = run_classify_stage(session, run_id=15, config={})
        assert result == {"auto_merges": 1, "queued": 0, "rejected": 0}

        decisions = _decision_by_pair(session, run_id=15)
        boundary = decisions[("boundary-a", "boundary-b")]
        assert boundary.band == DecisionBand.auto
        assert boundary.outcome == DecisionOutcome.merged

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 15)).all()
        assert len(edges) == 1
        assert edges[0].edge_source == "probabilistic"


def test_prior_rejection_beats_later_approval_for_same_pair():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=16)
        _add_scored_pair(session, run_id=16, pair_id="conflict", score=0.999)
        session.add(
            MergeReview(
                run_id=1,
                source_a_type=SourceType.unified_person,
                source_a_id="conflict-a",
                source_b_type=SourceType.unified_person,
                source_b_id="conflict-b",
                status=ReviewStatus.approved,
            )
        )
        session.add(
            MergeReview(
                run_id=2,
                source_a_type=SourceType.unified_person,
                source_a_id="conflict-a",
                source_b_type=SourceType.unified_person,
                source_b_id="conflict-b",
                status=ReviewStatus.rejected,
            )
        )
        session.commit()

        result = run_classify_stage(session, run_id=16, config={})
        assert result == {"auto_merges": 0, "queued": 0, "rejected": 1}

        decisions = _decision_by_pair(session, run_id=16)
        conflict = decisions[("conflict-a", "conflict-b")]
        assert conflict.band == DecisionBand.reject
        assert conflict.outcome == DecisionOutcome.rejected

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 16)).all()
        assert edges == []


def test_classify_preserves_fastpath_match_decisions():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session, run_id=17)
        _add_scored_pair(session, run_id=17, pair_id="prob", score=0.999)
        session.add(
            MatchDecision(
                run_id=17,
                source_a_type=SourceType.unified_person,
                source_a_id="exact-a",
                source_b_type=SourceType.unified_person,
                source_b_id="exact-b",
                score=None,
                method=MatchMethod.exact,
                band=DecisionBand.auto,
                outcome=DecisionOutcome.merged,
                explanation_json='{"rule": "email"}',
            )
        )
        session.commit()

        run_classify_stage(session, run_id=17, config={})

        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 17)
        ).all()
        assert len(decisions) == 2

        exact_rows = [
            row for row in decisions if row.method == MatchMethod.exact
        ]
        assert len(exact_rows) == 1
        assert exact_rows[0].source_a_id == "exact-a"
        assert exact_rows[0].explanation_json == '{"rule": "email"}'
