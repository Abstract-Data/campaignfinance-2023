"""Task 5 — ``review report`` CLI wiring tests."""

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

_PAYLOAD = '{"first_name": {"gamma": 3, "label": "Exact match", "bf": 8.0}}'
_RUN_ID = 41


def _make_seeded_engine():
    """Return an in-memory SQLite engine with one run and two decisions."""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=[MatchRun.__table__, MatchDecision.__table__])

    with Session(engine) as session:
        session.add(
            MatchRun(
                id=_RUN_ID,
                state_code="TX",
                pass_type=PassType.entity,
                status=RunStatus.completed,
            )
        )
        session.add(
            MatchDecision(
                run_id=_RUN_ID,
                source_a_type=SourceType.unified_person,
                source_a_id="a-1",
                source_b_type=SourceType.unified_person,
                source_b_id="b-1",
                score=0.99,
                method=MatchMethod.probabilistic,
                band=DecisionBand.auto,
                outcome=DecisionOutcome.merged,
                explanation_json=_PAYLOAD,
            )
        )
        session.add(
            MatchDecision(
                run_id=_RUN_ID,
                source_a_type=SourceType.unified_person,
                source_a_id="a-2",
                source_b_type=SourceType.unified_person,
                source_b_id="b-2",
                score=0.85,
                method=MatchMethod.probabilistic,
                band=DecisionBand.review,
                outcome=DecisionOutcome.queued,
                explanation_json=_PAYLOAD,
            )
        )
        session.commit()

    return engine


class TestReviewReportCli:
    def test_report_command_prints_full_summary(self, capsys, monkeypatch):
        from app.resolve.cli import main

        seeded_engine = _make_seeded_engine()
        monkeypatch.setattr(
            "sqlmodel.create_engine",
            lambda *_args, **_kwargs: seeded_engine,
        )

        exit_code = main(["review", "--sqlite", "report", "--run-id", str(_RUN_ID)])

        assert exit_code == 0
        captured = capsys.readouterr()
        assert f"Match explanation report for run {_RUN_ID}" in captured.out
        assert "Total decisions: 2" in captured.out
        assert "Pair unified_person:a-1 <-> unified_person:b-1" in captured.out
        assert "Pair unified_person:a-2 <-> unified_person:b-2" in captured.out

    def test_report_command_respects_band_filter(self, capsys, monkeypatch):
        from app.resolve.cli import main

        seeded_engine = _make_seeded_engine()
        monkeypatch.setattr(
            "sqlmodel.create_engine",
            lambda *_args, **_kwargs: seeded_engine,
        )

        exit_code = main(
            ["review", "--sqlite", "report", "--run-id", str(_RUN_ID), "--band", "review"]
        )

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "Filtered band: review" in captured.out
        assert "Rendered decisions: 1" in captured.out
        assert "Pair unified_person:a-2 <-> unified_person:b-2" in captured.out
        assert "Pair unified_person:a-1 <-> unified_person:b-1" not in captured.out

    def test_report_command_empty_run_exits_zero(self, capsys, monkeypatch):
        from app.resolve.cli import main

        engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
        SQLModel.metadata.create_all(engine, tables=[MatchRun.__table__, MatchDecision.__table__])
        with Session(engine) as session:
            session.add(
                MatchRun(
                    id=_RUN_ID,
                    state_code="TX",
                    pass_type=PassType.entity,
                    status=RunStatus.completed,
                )
            )
            session.commit()

        monkeypatch.setattr(
            "sqlmodel.create_engine",
            lambda *_args, **_kwargs: engine,
        )

        exit_code = main(["review", "--sqlite", "report", "--run-id", str(_RUN_ID)])

        assert exit_code == 0
        captured = capsys.readouterr()
        assert "Total decisions: 0" in captured.out
