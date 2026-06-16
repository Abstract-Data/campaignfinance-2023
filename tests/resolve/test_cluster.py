from __future__ import annotations

import logging

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.models.resolution import MatchRun, MergeReview, PassType
from app.resolve.stages.cluster import ClusterAssignment, run_cluster_stage
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.standardize.staging import ResolutionInput

_TABLES = [
    MatchRun.__table__,
    MergeEdge.__table__,
    MergeReview.__table__,
    ResolutionInput.__table__,
    ClusterAssignment.__table__,
]


@pytest.fixture()
def engine():
    eng = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(eng, tables=_TABLES)
    yield eng
    eng.dispose()


@pytest.fixture()
def session(engine):
    with Session(engine) as s:
        yield s


def _add_run(session: Session) -> int:
    run = MatchRun(state_code="TX", pass_type=PassType.entity)
    session.add(run)
    session.commit()
    session.refresh(run)
    assert run.id is not None
    return run.id


def _add_input(
    session: Session,
    run_id: int,
    source_type: str,
    source_id: str,
    *,
    entity_type: str = "person",
) -> None:
    session.add(
        ResolutionInput(
            run_id=run_id,
            source_type=source_type,
            source_id=source_id,
            entity_type=entity_type,
        )
    )
    session.commit()


def _add_edge(
    session: Session,
    run_id: int,
    source_a_type: str,
    source_a_id: str,
    source_b_type: str,
    source_b_id: str,
    *,
    edge_source: str = "deterministic",
) -> None:
    session.add(
        MergeEdge(
            run_id=run_id,
            source_a_type=source_a_type,
            source_a_id=source_a_id,
            source_b_type=source_b_type,
            source_b_id=source_b_id,
            edge_source=edge_source,
        )
    )
    session.commit()


def test_connected_components_collapse_transitive_edges(session: Session):
    run_id = _add_run(session)
    _add_input(session, run_id, "unified_person", "A")
    _add_input(session, run_id, "unified_person", "B")
    _add_input(session, run_id, "unified_person", "C")
    _add_edge(session, run_id, "unified_person", "A", "unified_person", "B")
    _add_edge(session, run_id, "unified_person", "B", "unified_person", "C")

    result = run_cluster_stage(session, run_id, {"max_cluster_size": 50})

    rows = session.exec(select(ClusterAssignment).where(ClusterAssignment.run_id == run_id)).all()
    by_cluster: dict[str, set[tuple[str, str]]] = {}
    for row in rows:
        by_cluster.setdefault(row.cluster_id, set()).add((row.source_type, row.source_id))

    assert result["clusters"] == 1
    assert len(by_cluster) == 1
    members = next(iter(by_cluster.values()))
    assert members == {
        ("unified_person", "A"),
        ("unified_person", "B"),
        ("unified_person", "C"),
    }


def test_records_with_no_edges_are_singletons(session: Session):
    run_id = _add_run(session)
    _add_input(session, run_id, "unified_person", "A")
    _add_input(session, run_id, "unified_person", "B")
    _add_input(session, run_id, "unified_person", "LONE")
    _add_edge(session, run_id, "unified_person", "A", "unified_person", "B")

    result = run_cluster_stage(session, run_id, {"max_cluster_size": 50})

    rows = session.exec(select(ClusterAssignment).where(ClusterAssignment.run_id == run_id)).all()
    by_cluster: dict[str, set[tuple[str, str]]] = {}
    for row in rows:
        by_cluster.setdefault(row.cluster_id, set()).add((row.source_type, row.source_id))

    assert result["clusters"] == 2
    assert len(by_cluster) == 2
    assert {("unified_person", "LONE")} in by_cluster.values()


def test_cluster_ids_are_stable_across_runs(session: Session):
    run_a = _add_run(session)
    run_b = _add_run(session)
    for run_id in (run_a, run_b):
        _add_input(session, run_id, "unified_person", "A")
        _add_input(session, run_id, "unified_person", "B")
        _add_input(session, run_id, "unified_person", "C")
        _add_input(session, run_id, "unified_person", "D")
        _add_edge(session, run_id, "unified_person", "A", "unified_person", "B")
        _add_edge(session, run_id, "unified_person", "B", "unified_person", "C")

    run_cluster_stage(session, run_a, {"max_cluster_size": 50})
    run_cluster_stage(session, run_b, {"max_cluster_size": 50})

    def assignments_for(run_id: int) -> dict[tuple[str, str], str]:
        rows = session.exec(
            select(ClusterAssignment).where(ClusterAssignment.run_id == run_id)
        ).all()
        return {(r.source_type, r.source_id): r.cluster_id for r in rows}

    assert assignments_for(run_a) == assignments_for(run_b)


def test_mega_cluster_guard_routes_pairs_to_review_and_holds_cluster(
    session: Session, caplog: pytest.LogCaptureFixture
):
    run_id = _add_run(session)
    nodes = ["A", "B", "C", "D", "E"]
    for node in nodes:
        _add_input(session, run_id, "unified_person", node)
    for i in range(len(nodes) - 1):
        _add_edge(
            session,
            run_id,
            "unified_person",
            nodes[i],
            "unified_person",
            nodes[i + 1],
            edge_source="probabilistic",
        )

    caplog.set_level(logging.WARNING)
    result = run_cluster_stage(session, run_id, {"max_cluster_size": 3})

    clusters = session.exec(
        select(ClusterAssignment).where(ClusterAssignment.run_id == run_id)
    ).all()
    reviews = session.exec(select(MergeReview).where(MergeReview.run_id == run_id)).all()

    assert result["clusters"] == 1
    assert result["held_for_review"] == 1
    assert len(clusters) == 5
    assert all(row.held_for_review for row in clusters)
    assert len(reviews) == 10
    assert "mega-cluster guard" in caplog.text.lower()
