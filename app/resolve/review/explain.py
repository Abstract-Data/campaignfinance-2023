"""Render and report human-readable explanations for match decisions."""

from __future__ import annotations

import json
from collections import Counter
from typing import Any

from sqlmodel import Session, select

from app.resolve.models.resolution import DecisionBand, MatchDecision

_NO_EXPLANATION = "No explanation available (missing or malformed explanation_json)."


def _parse_explanation_payload(
    explanation_json: str | dict[str, Any] | None,
) -> dict[str, Any] | None:
    if explanation_json is None:
        return None
    if isinstance(explanation_json, dict):
        return explanation_json
    if isinstance(explanation_json, str):
        try:
            payload = json.loads(explanation_json)
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None
    return None


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number


def explanation_table(explanation_json: str | dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return a waterfall table for per-field explanation contributions."""
    payload = _parse_explanation_payload(explanation_json)
    if not payload:
        return []

    rows: list[dict[str, Any]] = []
    running_total = 0.0

    for field, details in payload.items():
        if not isinstance(details, dict):
            continue

        similarity_level = str(details.get("label") or f"gamma={details.get('gamma', 'n/a')}")
        contribution = _as_float(details.get("bf_tf_adj"))
        if contribution is None:
            contribution = _as_float(details.get("bf"))
        if contribution is None:
            continue

        running_total += contribution
        rows.append(
            {
                "field": field,
                "similarity_level": similarity_level,
                "contribution": contribution,
                "running_total": running_total,
            }
        )

    return rows


def _final_probability(payload: dict[str, Any], rows: list[dict[str, Any]]) -> float | None:
    explicit = _as_float(
        payload.get("match_probability")
        or payload.get("final_match_probability")
        or payload.get("probability")
    )
    if explicit is not None:
        return max(0.0, min(1.0, explicit))

    # Fall back to a neutral-prior approximation using the cumulative Bayes factor.
    bf_product = 1.0
    has_weight = False
    for row in rows:
        weight = _as_float(row.get("contribution"))
        if weight is None or weight <= 0:
            continue
        has_weight = True
        bf_product *= weight

    if not has_weight:
        return None

    return bf_product / (1.0 + bf_product)


def render_explanation(explanation_json: str | dict[str, Any]) -> str:
    """Render explanation payload as plain-text waterfall lines."""
    payload = _parse_explanation_payload(explanation_json)
    if not payload:
        return _NO_EXPLANATION

    rows = explanation_table(payload)
    if not rows:
        return _NO_EXPLANATION

    rendered_rows = [
        (
            f"{row['field']}: level={row['similarity_level']} | "
            f"contribution={row['contribution']:.4f} | "
            f"running_total={row['running_total']:.4f}"
        )
        for row in rows
    ]

    final_probability = _final_probability(payload, rows)
    if final_probability is None:
        rendered_rows.append("Final match probability: unknown")
    else:
        rendered_rows.append(f"Final match probability: {final_probability:.4f}")

    return "\n".join(rendered_rows)


def run_report(session: Session, run_id: int, *, band: str | DecisionBand | None = None) -> str:
    """Render a multi-decision explanation report for one run."""
    all_decisions = list(
        session.exec(
            select(MatchDecision).where(MatchDecision.run_id == run_id).order_by(MatchDecision.id)
        ).all()
    )

    band_counts = Counter(decision.band.value for decision in all_decisions)
    band_auto = band_counts.get(DecisionBand.auto.value, 0)
    band_review = band_counts.get(DecisionBand.review.value, 0)
    band_reject = band_counts.get(DecisionBand.reject.value, 0)

    band_value: str | None = None
    if band is not None:
        band_value = band.value if isinstance(band, DecisionBand) else str(band)
        decisions = [d for d in all_decisions if d.band.value == band_value]
    else:
        decisions = all_decisions

    lines = [
        f"Match explanation report for run {run_id}",
        f"Total decisions: {len(all_decisions)}",
        f"Band counts: auto={band_auto} review={band_review} reject={band_reject}",
    ]
    if band_value is not None:
        lines.append(f"Filtered band: {band_value}")
    lines.append(f"Rendered decisions: {len(decisions)}")

    for decision in decisions:
        lines.append("")
        lines.append(
            "Pair "
            f"{decision.source_a_type.value}:{decision.source_a_id} <-> "
            f"{decision.source_b_type.value}:{decision.source_b_id}"
        )
        lines.append(
            "Decision "
            f"band={decision.band.value} outcome={decision.outcome.value} "
            f"score={decision.score if decision.score is not None else 'n/a'}"
        )
        lines.append(render_explanation(decision.explanation_json))

    return "\n".join(lines)
