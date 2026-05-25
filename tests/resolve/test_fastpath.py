"""Task 1f tests for stage 3 deterministic fast-path."""

from __future__ import annotations

import json

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.models.resolution import (
    DecisionBand,
    DecisionOutcome,
    EntityCrosswalk,
    MatchDecision,
    MatchMethod,
    MatchRun,
    PassType,
    RunStatus,
)
from app.resolve.stages.fastpath import MergeEdge, run_fastpath_stage
from app.resolve.standardize.staging import ResolutionInput


def _make_engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(
        engine,
        tables=[
            MatchRun.__table__,
            ResolutionInput.__table__,
            MatchDecision.__table__,
            MergeEdge.__table__,
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


def _person_row(
    *,
    run_id: int,
    source_id: str,
    first_name: str = "John",
    last_name: str = "Smith",
    line_1: str = "123 Main St",
    city: str = "Austin",
    state: str = "TX",
    zip5: str = "78701",
) -> ResolutionInput:
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_person",
        source_id=source_id,
        entity_type="person",
        first_name=first_name,
        last_name=last_name,
        line_1=line_1,
        city=city,
        state=state,
        zip5=zip5,
        parse_status="parsed",
    )


def _committee_row(
    *,
    run_id: int,
    source_id: str,
    line_1: str = "456 Oak Ave",
    city: str = "Dallas",
    state: str = "TX",
    zip5: str = "75201",
) -> ResolutionInput:
    return ResolutionInput(
        run_id=run_id,
        source_type="unified_committee",
        source_id=source_id,
        entity_type="committee",
        normalized_org="Friends of Texas",
        line_1=line_1,
        city=city,
        state=state,
        zip5=zip5,
        parse_status="parsed",
    )


def test_committees_with_same_filer_id_produce_exact_merge_edge():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session)
        filer_id = "CMT-100"
        session.add(
            _committee_row(run_id=1, source_id=filer_id, line_1="100 A St")
        )
        session.add(
            _committee_row(run_id=1, source_id=filer_id, line_1="200 B St")
        )
        session.commit()

        result = run_fastpath_stage(session, run_id=1, config={})
        assert result == {"auto_merges": 1}

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 1)).all()
        assert len(edges) == 1
        assert edges[0].edge_source == "deterministic"

        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()
        assert len(decisions) == 1
        assert decisions[0].method == MatchMethod.exact
        assert decisions[0].band == DecisionBand.auto
        assert decisions[0].outcome == DecisionOutcome.merged
        assert decisions[0].score is None


def test_persons_with_identical_name_and_address_merge():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session)
        session.add(_person_row(run_id=1, source_id="p-1"))
        session.add(_person_row(run_id=1, source_id="p-2"))
        session.commit()

        result = run_fastpath_stage(session, run_id=1, config={})
        assert result == {"auto_merges": 1}

        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()
        assert len(decisions) == 1
        assert decisions[0].method == MatchMethod.exact


def test_persons_with_same_name_different_addresses_do_not_merge():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session)
        session.add(_person_row(run_id=1, source_id="p-1", line_1="123 Main St"))
        session.add(
            _person_row(run_id=1, source_id="p-2", line_1="999 Other Blvd")
        )
        session.commit()

        result = run_fastpath_stage(session, run_id=1, config={})
        assert result == {"auto_merges": 0}

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 1)).all()
        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()
        assert edges == []
        assert decisions == []


def test_every_match_decision_has_nonempty_explanation_json():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session)
        session.add(_person_row(run_id=1, source_id="p-1"))
        session.add(_person_row(run_id=1, source_id="p-2"))
        session.commit()

        run_fastpath_stage(session, run_id=1, config={})

        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()
        assert decisions
        for decision in decisions:
            assert decision.explanation_json
            payload = json.loads(decision.explanation_json)
            assert payload.get("rule")


def test_identical_address_alone_does_not_merge():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session)
        session.add(
            _person_row(
                run_id=1,
                source_id="p-1",
                first_name="Alice",
                last_name="Jones",
            )
        )
        session.add(
            _person_row(
                run_id=1,
                source_id="p-2",
                first_name="Bob",
                last_name="Smith",
            )
        )
        session.commit()

        result = run_fastpath_stage(session, run_id=1, config={})
        assert result == {"auto_merges": 0}

        edges = session.exec(select(MergeEdge).where(MergeEdge.run_id == 1)).all()
        decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()
        assert edges == []
        assert decisions == []


def test_fastpath_does_not_write_crosswalk_rows():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(
        engine,
        tables=[
            MatchRun.__table__,
            ResolutionInput.__table__,
            MatchDecision.__table__,
            MergeEdge.__table__,
            EntityCrosswalk.__table__,
        ],
    )
    with Session(engine) as session:
        _seed_run(session)
        session.add(_person_row(run_id=1, source_id="p-1"))
        session.add(_person_row(run_id=1, source_id="p-2"))
        session.commit()

        run_fastpath_stage(session, run_id=1, config={})

        crosswalk_rows = session.exec(select(EntityCrosswalk)).all()
        assert crosswalk_rows == []


def test_running_fastpath_twice_is_deterministic():
    engine = _make_engine()
    with Session(engine) as session:
        _seed_run(session)
        session.add(_committee_row(run_id=1, source_id="CMT-1"))
        session.add(_committee_row(run_id=1, source_id="CMT-1"))
        session.add(_person_row(run_id=1, source_id="p-1"))
        session.add(_person_row(run_id=1, source_id="p-2"))
        session.commit()

        run_fastpath_stage(session, run_id=1, config={})
        first_edges = session.exec(
            select(MergeEdge).where(MergeEdge.run_id == 1)
        ).all()
        first_decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()

        run_fastpath_stage(session, run_id=1, config={})
        second_edges = session.exec(
            select(MergeEdge).where(MergeEdge.run_id == 1)
        ).all()
        second_decisions = session.exec(
            select(MatchDecision).where(MatchDecision.run_id == 1)
        ).all()

    def edge_key(edge: MergeEdge) -> tuple:
        return (
            edge.source_a_type,
            edge.source_a_id,
            edge.source_b_type,
            edge.source_b_id,
            edge.edge_source,
        )

    def decision_key(decision: MatchDecision) -> tuple:
        return (
            decision.source_a_type,
            decision.source_a_id,
            decision.source_b_type,
            decision.source_b_id,
            decision.method,
            decision.band,
            decision.outcome,
            decision.score,
            decision.explanation_json,
        )

    assert sorted(map(edge_key, first_edges)) == sorted(map(edge_key, second_edges))
    assert sorted(map(decision_key, first_decisions)) == sorted(
        map(decision_key, second_decisions)
    )
