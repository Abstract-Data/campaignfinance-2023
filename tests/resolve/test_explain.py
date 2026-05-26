"""Task 3b tests for explanation rendering and reporting."""

from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine

from app.resolve.models.resolution import (
    DecisionBand,
    DecisionOutcome,
    MatchDecision,
    MatchMethod,
    MatchRun,
    PassType,
    RunStatus,
    SourceType,
)
from app.resolve.review.explain import explanation_table, render_explanation, run_report


def test_render_explanation_outputs_one_line_per_field_and_probability() -> None:
    payload = {
        "first_name": {"gamma": 3, "label": "Exact match", "bf": 12.0},
        "last_name": {"gamma": 2, "label": "Near match", "bf": 6.0},
        "line_1": {"gamma": 1, "label": "TF adjusted", "bf_tf_adj": 0.5},
    }

    rendered = render_explanation(payload)
    lines = rendered.splitlines()

    assert any("first_name" in line and "Exact match" in line for line in lines)
    assert any("last_name" in line and "Near match" in line for line in lines)
    assert any("line_1" in line and "TF adjusted" in line for line in lines)
    assert lines[-1].startswith("Final match probability:")


def test_render_explanation_handles_malformed_input_gracefully() -> None:
    rendered = render_explanation("{not json")
    assert "No explanation available" in rendered


def test_explanation_table_returns_waterfall_rows() -> None:
    payload = {
        "first_name": {"gamma": 3, "label": "Exact match", "bf": 10.0},
        "last_name": {"gamma": 2, "label": "Near match", "bf": 2.0},
    }

    rows = explanation_table(payload)

    assert len(rows) == 2
    assert rows[0]["field"] == "first_name"
    assert rows[0]["similarity_level"] == "Exact match"
    assert rows[0]["contribution"] == 10.0
    assert rows[0]["running_total"] == 10.0
    assert rows[1]["running_total"] == 12.0


def test_run_report_renders_summary_and_respects_band_filter() -> None:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=[MatchRun.__table__, MatchDecision.__table__])

    payload = '{"first_name": {"gamma": 3, "label": "Exact match", "bf": 8.0}}'

    with Session(engine) as session:
        session.add(
            MatchRun(
                id=41,
                state_code="TX",
                pass_type=PassType.entity,
                status=RunStatus.completed,
            )
        )
        session.add(
            MatchDecision(
                run_id=41,
                source_a_type=SourceType.unified_person,
                source_a_id="a-1",
                source_b_type=SourceType.unified_person,
                source_b_id="b-1",
                score=0.99,
                method=MatchMethod.probabilistic,
                band=DecisionBand.auto,
                outcome=DecisionOutcome.merged,
                explanation_json=payload,
            )
        )
        session.add(
            MatchDecision(
                run_id=41,
                source_a_type=SourceType.unified_person,
                source_a_id="a-2",
                source_b_type=SourceType.unified_person,
                source_b_id="b-2",
                score=0.85,
                method=MatchMethod.probabilistic,
                band=DecisionBand.review,
                outcome=DecisionOutcome.queued,
                explanation_json=payload,
            )
        )
        session.commit()

        filtered = run_report(session, 41, band="review")
        full = run_report(session, 41)

    assert "Band counts: auto=1 review=1 reject=0" in filtered
    assert "Filtered band: review" in filtered
    assert "Pair unified_person:a-2 <-> unified_person:b-2" in filtered
    assert "Pair unified_person:a-1 <-> unified_person:b-1" not in filtered

    assert "Total decisions: 2" in full
    assert "Pair unified_person:a-1 <-> unified_person:b-1" in full
    assert "Pair unified_person:a-2 <-> unified_person:b-2" in full
