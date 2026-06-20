"""Shared constants and row helpers for the Splink score harness."""

from __future__ import annotations

import json
from typing import Any

from app.resolve.standardize.staging import ResolutionInput

FALLBACK_SCORE = 0.0
MIN_RECORDS_FOR_EM = 4
PAIR_STREAM_SIZE = 50_000
PREDICT_STREAM_SIZE = 50_000

__all__ = [
    "FALLBACK_SCORE",
    "MIN_RECORDS_FOR_EM",
    "PAIR_STREAM_SIZE",
    "PREDICT_STREAM_SIZE",
    "build_uid",
    "row_to_dict",
    "scored_row",
]


def build_uid(source_type: str, source_id: str) -> str:
    """Stable unique identifier combining source type and source id."""
    return f"{source_type}:{source_id}"


def row_to_dict(row: ResolutionInput) -> dict[str, Any]:
    """Flatten a ResolutionInput row to a Splink-compatible dict."""
    zip5 = row.zip5 or ""
    first_name = row.first_name or ""
    return {
        "unique_id": build_uid(row.source_type, row.source_id),
        "source_id": row.source_id,
        "source_type": row.source_type,
        "first_name": first_name,
        "first_initial": first_name[:1].lower() if first_name.strip() else "",
        "first_name_phonetic": row.first_name_phonetic or "",
        "last_name": row.last_name or "",
        "last_name_phonetic": row.last_name_phonetic or "",
        "normalized_org": row.normalized_org or "",
        "line_1": row.line_1 or "",
        "city": row.city or "",
        "state": row.state or "",
        "zip5": zip5,
        "zip3": zip5[:3] if len(zip5) >= 3 else "",
        "employer": row.employer,
    }


def scored_row(
    run_id: int,
    a_type: str,
    a_id: str,
    b_type: str,
    b_id: str,
    entity_type: str,
    score: float,
    explanation: dict[str, Any],
) -> dict[str, Any]:
    """Build a plain-dict ``scored_pairs`` row for Core bulk insert."""
    return {
        "run_id": run_id,
        "source_a_type": a_type,
        "source_a_id": a_id,
        "source_b_type": b_type,
        "source_b_id": b_id,
        "entity_type": entity_type,
        "score": score,
        "explanation_json": json.dumps(explanation),
    }
