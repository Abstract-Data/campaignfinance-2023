"""Stage 5: classify scored pairs into auto/review/reject bands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlmodel import Session, delete, select

from app.resolve.models.resolution import (
    DecisionBand,
    DecisionOutcome,
    MatchDecision,
    MatchMethod,
    MergeReview,
    ReviewStatus,
    SourceType,
)
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.stages.score import ScoredPair


@dataclass(frozen=True)
class _Thresholds:
    auto_threshold: float
    review_threshold: float


def _ordered_pair(
    source_a_type: str,
    source_a_id: str,
    source_b_type: str,
    source_b_id: str,
) -> tuple[tuple[str, str], tuple[str, str]]:
    left = (source_a_type, source_a_id)
    right = (source_b_type, source_b_id)
    return (left, right) if left <= right else (right, left)


def _thresholds_for_entity(
    config: dict[str, Any],
    entity_type: str,
) -> _Thresholds:
    auto_threshold = float(config.get("auto_threshold", 0.99))
    review_threshold = float(config.get("review_threshold", 0.80))

    threshold_overrides = config.get("threshold_overrides", {})
    if isinstance(threshold_overrides, dict):
        entity_overrides = threshold_overrides.get(entity_type, {})
        if isinstance(entity_overrides, dict):
            auto_threshold = float(entity_overrides.get("auto_threshold", auto_threshold))
            review_threshold = float(entity_overrides.get("review_threshold", review_threshold))

    if review_threshold > auto_threshold:
        raise ValueError("review_threshold cannot exceed auto_threshold")

    return _Thresholds(
        auto_threshold=auto_threshold,
        review_threshold=review_threshold,
    )


def _load_prior_decisions(
    session: Session,
) -> dict[tuple[tuple[str, str], tuple[str, str]], ReviewStatus]:
    prior_by_pair: dict[tuple[tuple[str, str], tuple[str, str]], ReviewStatus] = {}

    rows = session.exec(
        select(MergeReview).where(
            MergeReview.status.in_([ReviewStatus.approved, ReviewStatus.rejected])
        )
    ).all()
    for row in rows:
        key = _ordered_pair(
            row.source_a_type.value,
            row.source_a_id,
            row.source_b_type.value,
            row.source_b_id,
        )
        existing = prior_by_pair.get(key)
        if existing == ReviewStatus.rejected:
            continue
        if row.status == ReviewStatus.rejected:
            prior_by_pair[key] = ReviewStatus.rejected
        elif existing is None:
            prior_by_pair[key] = ReviewStatus.approved

    return prior_by_pair


def _to_source_type(source_type: str) -> SourceType:
    return SourceType(source_type)


def run_classify_stage(
    session: Session,
    run_id: int,
    config: dict[str, Any],
) -> dict[str, int]:
    """Classify scored pairs and persist decisions + downstream queue/edges."""
    session.exec(
        delete(MatchDecision).where(
            MatchDecision.run_id == run_id,
            MatchDecision.method == MatchMethod.probabilistic,
        )
    )
    session.exec(
        delete(MergeEdge).where(
            MergeEdge.run_id == run_id,
            MergeEdge.edge_source.in_(["probabilistic", "approved_review"]),
        )
    )
    session.exec(
        delete(MergeReview).where(
            MergeReview.run_id == run_id,
            MergeReview.status == ReviewStatus.pending,
        )
    )

    prior_decisions = _load_prior_decisions(session)
    scored_pairs = session.exec(
        select(ScoredPair)
        .where(ScoredPair.run_id == run_id)
        .order_by(
            ScoredPair.source_a_type,
            ScoredPair.source_a_id,
            ScoredPair.source_b_type,
            ScoredPair.source_b_id,
            ScoredPair.id,
        )
    ).all()

    auto_merges = 0
    queued = 0
    rejected = 0
    decisions: list[MatchDecision] = []
    edges: list[MergeEdge] = []
    review_rows: list[MergeReview] = []

    for pair in scored_pairs:
        key = _ordered_pair(
            pair.source_a_type,
            pair.source_a_id,
            pair.source_b_type,
            pair.source_b_id,
        )
        prior_status = prior_decisions.get(key)

        band: DecisionBand
        outcome: DecisionOutcome
        edge_source: str | None = None
        decision_method: MatchMethod = MatchMethod.probabilistic

        if prior_status == ReviewStatus.rejected:
            band = DecisionBand.reject
            outcome = DecisionOutcome.rejected
            rejected += 1
        elif prior_status == ReviewStatus.approved:
            band = DecisionBand.auto
            outcome = DecisionOutcome.merged
            edge_source = "approved_review"
            decision_method = MatchMethod.approved_review
            auto_merges += 1
        else:
            thresholds = _thresholds_for_entity(config, pair.entity_type)
            if pair.score >= thresholds.auto_threshold:
                band = DecisionBand.auto
                outcome = DecisionOutcome.merged
                edge_source = "probabilistic"
                auto_merges += 1
            elif pair.score >= thresholds.review_threshold:
                band = DecisionBand.review
                outcome = DecisionOutcome.queued
                queued += 1
            else:
                band = DecisionBand.reject
                outcome = DecisionOutcome.rejected
                rejected += 1

        decisions.append(
            MatchDecision(
                run_id=run_id,
                source_a_type=_to_source_type(pair.source_a_type),
                source_a_id=pair.source_a_id,
                source_b_type=_to_source_type(pair.source_b_type),
                source_b_id=pair.source_b_id,
                score=pair.score,
                method=decision_method,
                band=band,
                outcome=outcome,
                explanation_json=pair.explanation_json,
            )
        )

        if edge_source is not None:
            left, right = key
            edges.append(
                MergeEdge(
                    run_id=run_id,
                    source_a_type=left[0],
                    source_a_id=left[1],
                    source_b_type=right[0],
                    source_b_id=right[1],
                    edge_source=edge_source,
                )
            )
        elif band == DecisionBand.review:
            left, right = key
            review_rows.append(
                MergeReview(
                    run_id=run_id,
                    source_a_type=_to_source_type(left[0]),
                    source_a_id=left[1],
                    source_b_type=_to_source_type(right[0]),
                    source_b_id=right[1],
                    score=pair.score,
                    explanation_json=pair.explanation_json,
                    status=ReviewStatus.pending,
                )
            )

    session.add_all(decisions)
    session.add_all(edges)
    session.add_all(review_rows)
    session.commit()

    return {
        "auto_merges": auto_merges,
        "queued": queued,
        "rejected": rejected,
    }
