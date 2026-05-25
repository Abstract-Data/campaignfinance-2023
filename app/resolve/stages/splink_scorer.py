"""Golden-set adapter for Stage 4 Splink scoring.

Hand-labeled CSV pair dicts from ``tests/resolve/golden/`` are seeded into an
in-memory SQLite session and scored via :func:`run_score_stage`.  Scores are
converted to ``match`` / ``no_match`` predictions for the precision/recall
regression harness in ``tests/resolve/test_match_quality.py``.

Task: 2e harness wiring (BLOCK B1 fix)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.blocking import CandidatePair
from app.resolve.models.resolution import MatchRun, PassType, RunStatus
from app.resolve.stages.score import ScoredPair, run_score_stage
from app.resolve.standardize.staging import ResolutionInput

# Conservative cutoff for binary golden-set labels.  Slightly above the classify
# stage ``review_threshold`` (0.80) so hard no_match cases (e.g. suffix-only
# differences) stay below the floor on person pairs.
MATCH_THRESHOLD: float = 0.90

_GOLDEN_RUN_ID = 1
_GOLDEN_SEED = 42

_SCORE_TABLES = [
    MatchRun.__table__,
    ResolutionInput.__table__,
    CandidatePair.__table__,
    ScoredPair.__table__,
]


def _blank(value: str | None) -> str | None:
    """Return None for empty strings so SQLModel stores NULL."""
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def _make_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(engine, tables=_SCORE_TABLES)
    return engine


def _person_input(
    *, run_id: int, source_id: str, side: str, row: dict[str, Any]
) -> ResolutionInput:
    suffix = "_a" if side == "a" else "_b"
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_person",
        source_id=source_id,
        entity_type="person",
        first_name=_blank(row.get(f"first_name{suffix}")),
        middle_name=_blank(row.get(f"middle_name{suffix}")),
        last_name=_blank(row.get(f"last_name{suffix}")),
        suffix=_blank(row.get(f"suffix{suffix}")),
        line_1=_blank(row.get(f"line_1{suffix}")),
        city=_blank(row.get(f"city{suffix}")),
        state=_blank(row.get(f"state{suffix}")),
        zip5=_blank(row.get(f"zip5{suffix}")),
        raw_name=_blank(row.get(f"raw_name{suffix}")),
        parse_status="parsed",
    )


def _organization_input(
    *, run_id: int, source_id: str, side: str, row: dict[str, Any]
) -> ResolutionInput:
    suffix = "_a" if side == "a" else "_b"
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_entity",
        source_id=source_id,
        entity_type="organization",
        is_organization=True,
        normalized_org=_blank(row.get(f"normalized_org{suffix}")),
        line_1=_blank(row.get(f"line_1{suffix}")),
        city=_blank(row.get(f"city{suffix}")),
        state=_blank(row.get(f"state{suffix}")),
        zip5=_blank(row.get(f"zip5{suffix}")),
        raw_name=_blank(row.get(f"raw_name{suffix}")),
        parse_status="parsed",
    )


def _committee_input(
    *, run_id: int, source_id: str, side: str, row: dict[str, Any]
) -> ResolutionInput:
    suffix = "_a" if side == "a" else "_b"
    filer_id = _blank(row.get(f"filer_id{suffix}")) or source_id
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_committee",
        source_id=filer_id,
        entity_type="committee",
        is_organization=True,
        normalized_org=_blank(row.get(f"normalized_org{suffix}")),
        line_1=_blank(row.get(f"line_1{suffix}")),
        city=_blank(row.get(f"city{suffix}")),
        state=_blank(row.get(f"state{suffix}")),
        zip5=_blank(row.get(f"zip5{suffix}")),
        raw_name=_blank(row.get(f"raw_name{suffix}")),
        parse_status="parsed",
    )


def _score_pairs(
    pairs: list[dict[str, Any]],
    *,
    source_type: str,
    build_input: Callable[..., ResolutionInput],
    pair_ids: Callable[[dict[str, Any]], tuple[str, str]],
) -> list[str]:
    """Seed golden pairs into SQLite, run Stage 4, return match predictions."""
    if not pairs:
        return []

    engine = _make_engine()
    with Session(engine) as session:
        session.add(
            MatchRun(
                id=_GOLDEN_RUN_ID,
                state_code="TX",
                pass_type=PassType.entity,
                status=RunStatus.running,
            )
        )

        for row in pairs:
            source_a_id, source_b_id = pair_ids(row)
            session.add(
                build_input(
                    run_id=_GOLDEN_RUN_ID,
                    source_id=source_a_id,
                    side="a",
                    row=row,
                )
            )
            session.add(
                build_input(
                    run_id=_GOLDEN_RUN_ID,
                    source_id=source_b_id,
                    side="b",
                    row=row,
                )
            )
            session.add(
                CandidatePair(
                    run_id=_GOLDEN_RUN_ID,
                    source_a_type=source_type,
                    source_a_id=source_a_id,
                    source_b_type=source_type,
                    source_b_id=source_b_id,
                    rule_name="golden_set",
                )
            )
        session.commit()

        run_score_stage(session, run_id=_GOLDEN_RUN_ID, config={"seed": _GOLDEN_SEED})
        scored_rows = session.exec(
            select(ScoredPair).where(ScoredPair.run_id == _GOLDEN_RUN_ID)
        ).all()

    score_by_pair = {(row.source_a_id, row.source_b_id): row.score for row in scored_rows}

    predictions: list[str] = []
    for row in pairs:
        key = pair_ids(row)
        score = score_by_pair.get(key, 0.0)
        predictions.append("match" if score >= MATCH_THRESHOLD else "no_match")
    return predictions


def score_person_pairs(pairs: list[dict[str, Any]]) -> list[str]:
    """Score hand-labeled person golden pairs and return match predictions."""

    def _pair_ids(row: dict[str, Any]) -> tuple[str, str]:
        pair_id = row["pair_id"]
        return f"{pair_id}_a", f"{pair_id}_b"

    return _score_pairs(
        pairs,
        source_type="unified_person",
        build_input=_person_input,
        pair_ids=_pair_ids,
    )


def score_organization_pairs(pairs: list[dict[str, Any]]) -> list[str]:
    """Score hand-labeled organization golden pairs and return match predictions."""

    def _pair_ids(row: dict[str, Any]) -> tuple[str, str]:
        pair_id = row["pair_id"]
        return f"{pair_id}_a", f"{pair_id}_b"

    return _score_pairs(
        pairs,
        source_type="unified_entity",
        build_input=_organization_input,
        pair_ids=_pair_ids,
    )


def score_committee_pairs(pairs: list[dict[str, Any]]) -> list[str]:
    """Score hand-labeled committee golden pairs and return match predictions."""

    def _pair_ids(row: dict[str, Any]) -> tuple[str, str]:
        return row["filer_id_a"], row["filer_id_b"]

    return _score_pairs(
        pairs,
        source_type="unified_committee",
        build_input=_committee_input,
        pair_ids=_pair_ids,
    )
