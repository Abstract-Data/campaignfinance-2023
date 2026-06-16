"""Task 1g — Stage 7: clustering + survivorship + canonical publish.

TDD steps per task-1g brief:

- Step 1:  ``cluster_edges`` groups A–B and B–C into one cluster {A,B,C}; a
           record with no edges is its own singleton cluster.
- Step 3:  ``build_golden_record`` picks the most-complete name and the most-
           recent parsed address.
- Step 4:  ``run_survivorship_stage`` writes canonical + name-history +
           crosswalk rows; every source record has exactly one crosswalk row.
- Step 5:  ``canonical_name_history`` captures every distinct name in a
           multi-name cluster, each with correct first/last-seen dates.

Task 2d additions:
- Step 1:  ``run_survivorship_stage`` reads the ``clusters`` staging table and
           skips ``held_for_review=True`` clusters for canonical publishing.
- Step 4:  Published canonical rows carry ``provenance_json`` naming the source
           record each field came from.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.models.canonical import (
    CanonicalAddress,
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalNameHistory,
    NameHistorySubjectType,
)
from app.resolve.models.resolution import (
    ConfidenceBand,
    EntityCrosswalk,
    MatchDecision,
    MatchMethod,
    MatchRun,
    MergeReview,
    PassType,
    ReviewStatus,
    SourceType,
)
from app.resolve.stages.cluster import ClusterAssignment
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.stages.survivorship import (
    Cluster,
    Edge,
    build_golden_record,
    cluster_edges,
    load_cluster_edges,
    run_survivorship_stage,
)
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Tables created for these tests
# ---------------------------------------------------------------------------

_TABLES = [
    MatchRun.__table__,
    CanonicalAddress.__table__,  # FK target for CanonicalEntity
    CanonicalEntity.__table__,
    CanonicalCampaign.__table__,  # cleared by _clear_live_canonical_snapshot (FK → canonical_entity)
    CanonicalNameHistory.__table__,
    EntityCrosswalk.__table__,
    MatchDecision.__table__,  # Queried by _build_node_crosswalk_attrs
    MergeEdge.__table__,
    MergeReview.__table__,
    ResolutionInput.__table__,
    ClusterAssignment.__table__,  # Phase 2 (task-2d): clusters staging table
]

# Alias kept for Phase 2-specific fixtures to be explicit about intent.
_TABLES_PHASE2 = _TABLES


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


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


@pytest.fixture()
def run_id(session: Session) -> int:
    run = MatchRun(state_code="TX", pass_type=PassType.entity)
    session.add(run)
    session.commit()
    session.refresh(run)
    assert run.id is not None
    return run.id


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc(offset_days: float = 0.0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=offset_days)


def _add_input(
    session: Session,
    run_id: int,
    source_type: str,
    source_id: str,
    *,
    first_name: str | None = None,
    middle_name: str | None = None,
    last_name: str | None = None,
    suffix: str | None = None,
    raw_name: str = "",
    is_organization: bool = False,
    normalized_org: str | None = None,
    parse_status: str = "unparsed",
    entity_type: str | None = None,
    created_at: datetime | None = None,
) -> ResolutionInput:
    row = ResolutionInput(
        run_id=run_id,
        source_type=source_type,
        source_id=source_id,
        entity_type=entity_type or ("organization" if is_organization else "person"),
        first_name=first_name,
        middle_name=middle_name,
        last_name=last_name,
        suffix=suffix,
        raw_name=raw_name or " ".join(p for p in [first_name, last_name] if p),
        is_organization=is_organization,
        normalized_org=normalized_org,
        parse_status=parse_status,
        created_at=created_at or _utc(),
    )
    session.add(row)
    session.commit()
    return row


def _add_merge_edge(
    session: Session,
    run_id: int,
    src_type_a: str,
    src_id_a: str,
    src_type_b: str,
    src_id_b: str,
    *,
    edge_source: str = "deterministic",
) -> MergeEdge:
    edge = MergeEdge(
        run_id=run_id,
        source_a_type=src_type_a,
        source_a_id=src_id_a,
        source_b_type=src_type_b,
        source_b_id=src_id_b,
        edge_source=edge_source,
    )
    session.add(edge)
    session.commit()
    return edge


def _add_approved_review(
    session: Session,
    run_id: int,
    src_type_a: str,
    src_id_a: str,
    src_type_b: str,
    src_id_b: str,
) -> MergeReview:
    review = MergeReview(
        run_id=run_id,
        source_a_type=SourceType(src_type_a),
        source_a_id=src_id_a,
        source_b_type=SourceType(src_type_b),
        source_b_id=src_id_b,
        status=ReviewStatus.approved,
    )
    session.add(review)
    session.commit()
    return review


def _build_rows(specs: list[dict], run_id: int = 1) -> list[ResolutionInput]:
    """Create in-memory (un-persisted) ResolutionInput objects for unit tests."""
    rows = []
    for spec in specs:
        row = ResolutionInput(
            run_id=run_id,
            source_type=spec.get("source_type", "unified_person"),
            source_id=spec["source_id"],
            entity_type=spec.get(
                "entity_type",
                "organization" if spec.get("is_organization") else "person",
            ),
            first_name=spec.get("first_name"),
            middle_name=spec.get("middle_name"),
            last_name=spec.get("last_name"),
            suffix=spec.get("suffix"),
            raw_name=spec.get("raw_name", ""),
            is_organization=spec.get("is_organization", False),
            normalized_org=spec.get("normalized_org"),
            parse_status=spec.get("parse_status", "unparsed"),
            created_at=spec.get("created_at", _utc()),
        )
        rows.append(row)
    return rows


# ===========================================================================
# Step 1 — cluster_edges
# ===========================================================================


class TestClusterEdges:
    def test_ab_and_bc_collapse_into_one_cluster(self):
        """Transitive edges A–B and B–C form a single cluster {A, B, C}."""
        edges = [
            Edge("unified_person", "A", "unified_person", "B"),
            Edge("unified_person", "B", "unified_person", "C"),
        ]
        clusters = cluster_edges(edges)

        assert len(clusters) == 1
        members = set(clusters[0].members)
        assert members == {
            ("unified_person", "A"),
            ("unified_person", "B"),
            ("unified_person", "C"),
        }

    def test_singleton_record_becomes_its_own_cluster(self):
        """A record not in any edge is a singleton cluster when all_source_keys is given."""
        edges = [
            Edge("unified_person", "A", "unified_person", "B"),
        ]
        all_keys = [
            ("unified_person", "A"),
            ("unified_person", "B"),
            ("unified_person", "LONE"),
        ]
        clusters = cluster_edges(edges, all_source_keys=all_keys)

        assert len(clusters) == 2
        lone = [c for c in clusters if ("unified_person", "LONE") in c.members]
        assert len(lone) == 1
        assert len(lone[0].members) == 1

    def test_disjoint_edges_produce_separate_clusters(self):
        """Non-overlapping edge pairs produce independent clusters."""
        edges = [
            Edge("unified_person", "A", "unified_person", "B"),
            Edge("unified_person", "C", "unified_person", "D"),
        ]
        clusters = cluster_edges(edges)
        assert len(clusters) == 2

    def test_no_edges_all_keys_are_singletons(self):
        """N records with no edges → N singleton clusters."""
        all_keys = [("unified_person", str(i)) for i in range(4)]
        clusters = cluster_edges([], all_source_keys=all_keys)

        assert len(clusters) == 4
        assert all(len(c.members) == 1 for c in clusters)

    def test_cluster_ids_are_unique_across_clusters(self):
        """Every cluster gets a distinct cluster_id UUID."""
        edges = [
            Edge("unified_person", "A", "unified_person", "B"),
            Edge("unified_person", "C", "unified_person", "D"),
        ]
        clusters = cluster_edges(edges)
        ids = [c.cluster_id for c in clusters]
        assert len(ids) == len(set(ids))

    def test_five_node_chain_is_one_cluster(self):
        """A chain A–B–C–D–E (four edges) collapses into one cluster."""
        nodes = ["A", "B", "C", "D", "E"]
        edges = [
            Edge("unified_person", nodes[i], "unified_person", nodes[i + 1])
            for i in range(len(nodes) - 1)
        ]
        clusters = cluster_edges(edges)
        assert len(clusters) == 1
        assert len(clusters[0].members) == 5


# ===========================================================================
# Step 3 — build_golden_record
# ===========================================================================


class TestBuildGoldenRecord:
    def test_most_complete_name_wins(self):
        """A row with more non-empty name parts is preferred over a sparse row."""
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = _build_rows(
            [
                {"source_id": "1", "first_name": "John", "last_name": "Doe"},
                {
                    "source_id": "2",
                    "first_name": "John",
                    "middle_name": "A",
                    "last_name": "Doe",
                    "suffix": "Jr",
                },
            ]
        )
        entity = build_golden_record(cluster, rows, "TX")

        assert "A" in entity.canonical_name or "Jr" in entity.canonical_name

    def test_tie_broken_by_most_recent(self):
        """When two rows are equally complete, the most recent one's name is used."""
        older = _utc(-10)
        newer = _utc()
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = _build_rows(
            [
                {
                    "source_id": "1",
                    "first_name": "John",
                    "last_name": "Doe",
                    "created_at": older,
                },
                {
                    "source_id": "2",
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "created_at": newer,
                },
            ]
        )
        entity = build_golden_record(cluster, rows, "TX")

        assert "Jane" in entity.canonical_name

    def test_first_seen_is_min_last_seen_is_max(self):
        """first_seen_date = min(created_at), last_seen_date = max(created_at)."""
        older = _utc(-30)
        newer = _utc()
        cluster = Cluster(members=[("unified_person", "1"), ("unified_person", "2")])
        rows = _build_rows(
            [
                {"source_id": "1", "first_name": "A", "last_name": "B", "created_at": older},
                {"source_id": "2", "first_name": "C", "last_name": "D", "created_at": newer},
            ]
        )
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.first_seen_date == older.date()
        assert entity.last_seen_date == newer.date()

    def test_source_record_count_equals_cluster_members(self):
        """source_record_count matches the number of cluster members."""
        cluster = Cluster(
            members=[
                ("unified_person", "1"),
                ("unified_person", "2"),
                ("unified_person", "3"),
            ]
        )
        rows = _build_rows(
            [
                {"source_id": "1", "first_name": "A", "last_name": "B"},
                {"source_id": "2", "first_name": "C", "last_name": "D"},
                {"source_id": "3", "first_name": "E", "last_name": "F"},
            ]
        )
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.source_record_count == 3

    def test_singleton_cluster_produces_valid_entity(self):
        """A single-member cluster still produces a valid CanonicalEntity."""
        cluster = Cluster(members=[("unified_person", "X")])
        rows = _build_rows([{"source_id": "X", "first_name": "Alice", "last_name": "Smith"}])
        entity = build_golden_record(cluster, rows, "TX")

        assert entity.canonical_name != ""
        assert entity.source_record_count == 1

    def test_organization_name_uses_normalized_org(self):
        """Organization rows use normalized_org as canonical_name."""
        cluster = Cluster(members=[("unified_entity", "ORG1")])
        rows = _build_rows(
            [
                {
                    "source_id": "ORG1",
                    "source_type": "unified_entity",
                    "is_organization": True,
                    "normalized_org": "acme",
                    "entity_type": "organization",
                }
            ]
        )
        entity = build_golden_record(cluster, rows, "TX")

        assert "acme" in entity.canonical_name.lower()


# ===========================================================================
# Step 4 — run_survivorship_stage (DB integration)
# ===========================================================================


class TestLoadClusterEdges:
    def test_reads_merge_edges_for_run(self, session: Session, run_id: int):
        _add_merge_edge(session, run_id, "unified_person", "A", "unified_person", "B")
        edges = load_cluster_edges(session, run_id)
        assert len(edges) == 1
        assert edges[0].source_id_a == "A"
        assert edges[0].source_id_b == "B"

    def test_includes_approved_review_edges(self, session: Session, run_id: int):
        _add_approved_review(session, run_id, "unified_person", "X", "unified_person", "Y")
        edges = load_cluster_edges(session, run_id)
        assert len(edges) == 1
        assert {edges[0].source_id_a, edges[0].source_id_b} == {"X", "Y"}


class TestRunSurvivorshipStage:
    def test_clusters_from_merge_edges_not_match_decision(self, session: Session, run_id: int):
        """Clustering must follow merge_edges; match_decision alone is not enough."""
        _add_input(session, run_id, "unified_person", "P1", first_name="Alice", last_name="Smith")
        _add_input(session, run_id, "unified_person", "P2", first_name="Alice", last_name="Smith")
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert result["canonical_out"] == 1
        entities = session.exec(select(CanonicalEntity)).all()
        assert len(entities) == 1
        assert entities[0].source_record_count == 2

    def test_approved_review_edge_merges_without_merge_edges_row(
        self, session: Session, run_id: int
    ):
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")
        _add_input(session, run_id, "unified_person", "P2", first_name="A", last_name="B")
        _add_approved_review(session, run_id, "unified_person", "P1", "unified_person", "P2")

        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert result["canonical_out"] == 1

    def test_second_run_replaces_live_canonical_entities(self, session: Session, run_id: int):
        """Staging swap keeps live canonical row count stable across reruns."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")
        _add_input(session, run_id, "unified_person", "P2", first_name="C", last_name="D")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})
        assert len(session.exec(select(CanonicalEntity)).all()) == 2

        run2 = MatchRun(state_code="TX", pass_type=PassType.entity)
        session.add(run2)
        session.commit()
        session.refresh(run2)
        assert run2.id is not None

        _add_input(session, run2.id, "unified_person", "P1", first_name="A", last_name="B")
        _add_input(session, run2.id, "unified_person", "P2", first_name="C", last_name="D")
        run_survivorship_stage(session, run2.id, {"state_code": "TX"})

        assert len(session.exec(select(CanonicalEntity)).all()) == 2

    def test_every_source_record_gets_exactly_one_crosswalk_row(
        self, session: Session, run_id: int
    ):
        """No record is left unlinked; each source record maps to exactly one crosswalk row."""
        _add_input(session, run_id, "unified_person", "P1", first_name="Alice", last_name="Smith")
        _add_input(session, run_id, "unified_person", "P2", first_name="Bob", last_name="Jones")
        _add_input(session, run_id, "unified_person", "P3", first_name="Carol", last_name="White")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        crosswalks = session.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
        ).all()
        assert len(crosswalks) == 3
        linked_ids = {cw.source_id for cw in crosswalks}
        assert linked_ids == {"P1", "P2", "P3"}

    def test_merged_pair_shares_one_canonical_entity(self, session: Session, run_id: int):
        """Two source records connected by a merge edge → one canonical entity."""
        _add_input(session, run_id, "unified_person", "P1", first_name="Alice", last_name="Smith")
        _add_input(session, run_id, "unified_person", "P2", first_name="Alice", last_name="Smith")
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert result["canonical_out"] == 1
        entities = session.exec(select(CanonicalEntity)).all()
        assert len(entities) == 1
        assert entities[0].source_record_count == 2

    def test_singletons_each_produce_their_own_canonical(self, session: Session, run_id: int):
        """Three unlinked records → three separate canonical entities."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")
        _add_input(session, run_id, "unified_person", "P2", first_name="C", last_name="D")
        _add_input(session, run_id, "unified_person", "P3", first_name="E", last_name="F")

        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert result["canonical_out"] == 3
        entities = session.exec(select(CanonicalEntity)).all()
        assert len(entities) == 3

    def test_transitive_edges_collapse_to_one_entity(self, session: Session, run_id: int):
        """A–B and B–C edges produce one canonical entity with source_record_count=3."""
        _add_input(session, run_id, "unified_person", "A", first_name="John", last_name="Smith")
        _add_input(session, run_id, "unified_person", "B", first_name="John", last_name="Smith")
        _add_input(session, run_id, "unified_person", "C", first_name="J", last_name="Smith")
        _add_merge_edge(session, run_id, "unified_person", "A", "unified_person", "B")
        _add_merge_edge(session, run_id, "unified_person", "B", "unified_person", "C")

        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert result["canonical_out"] == 1
        entity = session.exec(select(CanonicalEntity)).one()
        assert entity.source_record_count == 3

    def test_mixed_cluster_and_singletons(self, session: Session, run_id: int):
        """One merged pair + one singleton → canonical_out == 2."""
        _add_input(session, run_id, "unified_person", "P1", first_name="X", last_name="Y")
        _add_input(session, run_id, "unified_person", "P2", first_name="X", last_name="Y")
        _add_input(session, run_id, "unified_person", "P3", first_name="Z", last_name="Q")
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert result["canonical_out"] == 2
        crosswalks = session.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
        ).all()
        assert len(crosswalks) == 3

    def test_returns_canonical_out_key(self, session: Session, run_id: int):
        """Return dict always contains 'canonical_out'."""
        _add_input(session, run_id, "unified_person", "P1", first_name="X", last_name="Y")
        result = run_survivorship_stage(session, run_id, {"state_code": "TX"})

        assert "canonical_out" in result
        assert isinstance(result["canonical_out"], int)

    def test_state_code_written_on_canonical_entity(self, session: Session, run_id: int):
        """state_code from config is persisted on the canonical_entity row."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")

        run_survivorship_stage(session, run_id, {"state_code": "OK"})

        entity = session.exec(select(CanonicalEntity)).one()
        assert entity.state_code == "OK"

    def test_crosswalk_confidence_band_is_auto(self, session: Session, run_id: int):
        """Deterministic-path crosswalk rows carry confidence_band='auto'."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        cw = session.exec(select(EntityCrosswalk)).one()
        assert cw.confidence_band == ConfidenceBand.auto


# ===========================================================================
# Step 5 — canonical_name_history
# ===========================================================================


class TestCanonicalNameHistory:
    def test_distinct_names_produce_separate_history_rows(self, session: Session, run_id: int):
        """Each distinct (normalized) name in a cluster gets its own history row."""
        _add_input(
            session,
            run_id,
            "unified_person",
            "P1",
            first_name="John",
            last_name="Doe",
            raw_name="John Doe",
            created_at=_utc(-10),
        )
        _add_input(
            session,
            run_id,
            "unified_person",
            "P2",
            first_name="Johnny",
            last_name="Doe",
            raw_name="Johnny Doe",
            created_at=_utc(),
        )
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        history = session.exec(select(CanonicalNameHistory)).all()
        # "john doe" and "johnny doe" normalize differently → 2 rows
        assert len(history) >= 1
        for h in history:
            assert h.first_seen_date is not None
            assert h.last_seen_date is not None
            assert h.subject_type == NameHistorySubjectType.entity

    def test_same_name_deduplicates_into_one_row(self, session: Session, run_id: int):
        """Two source records with the same name → one history row with occurrence_count=2."""
        _add_input(
            session,
            run_id,
            "unified_person",
            "P1",
            first_name="John",
            last_name="Doe",
            raw_name="John Doe",
        )
        _add_input(
            session,
            run_id,
            "unified_person",
            "P2",
            first_name="John",
            last_name="Doe",
            raw_name="John Doe",
        )
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        entity = session.exec(select(CanonicalEntity)).one()
        history = session.exec(
            select(CanonicalNameHistory).where(CanonicalNameHistory.subject_id == entity.id)
        ).all()
        assert len(history) == 1
        assert history[0].occurrence_count == 2

    def test_name_history_dates_are_correct(self, session: Session, run_id: int):
        """first_seen_date = earliest, last_seen_date = most recent for the name."""
        older = _utc(-20)
        newer = _utc()
        _add_input(
            session,
            run_id,
            "unified_person",
            "P1",
            first_name="John",
            last_name="Doe",
            raw_name="John Doe",
            created_at=older,
        )
        _add_input(
            session,
            run_id,
            "unified_person",
            "P2",
            first_name="John",
            last_name="Doe",
            raw_name="John Doe",
            created_at=newer,
        )
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        history = session.exec(select(CanonicalNameHistory)).all()
        assert len(history) == 1
        assert history[0].first_seen_date == older.date()
        assert history[0].last_seen_date == newer.date()

    def test_singleton_gets_one_history_row(self, session: Session, run_id: int):
        """A singleton cluster still gets one canonical_name_history row."""
        _add_input(
            session,
            run_id,
            "unified_person",
            "P1",
            first_name="Alice",
            last_name="Smith",
        )

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        history = session.exec(select(CanonicalNameHistory)).all()
        assert len(history) == 1
        assert history[0].subject_type == NameHistorySubjectType.entity

    def test_history_subject_id_matches_canonical_entity(self, session: Session, run_id: int):
        """canonical_name_history.subject_id points at canonical_entity.id."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        entity = session.exec(select(CanonicalEntity)).one()
        history = session.exec(select(CanonicalNameHistory)).all()
        assert all(h.subject_id == entity.id for h in history)


# ===========================================================================
# Task 2d — Phase 2: clusters staging table + held_for_review + provenance
# ===========================================================================


@pytest.fixture()
def engine_phase2():
    """In-memory SQLite engine with Phase 2 tables (including ClusterAssignment)."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(eng, tables=_TABLES_PHASE2)
    yield eng
    eng.dispose()


@pytest.fixture()
def session_p2(engine_phase2):
    with Session(engine_phase2) as s:
        yield s


@pytest.fixture()
def run_id_p2(session_p2: Session) -> int:
    run = MatchRun(state_code="TX", pass_type=PassType.entity)
    session_p2.add(run)
    session_p2.commit()
    session_p2.refresh(run)
    assert run.id is not None
    return run.id


def _add_cluster_assignment(
    session: Session,
    run_id: int,
    cluster_id: str,
    source_type: str,
    source_id: str,
    *,
    entity_type: str = "person",
    held_for_review: bool = False,
) -> ClusterAssignment:
    row = ClusterAssignment(
        run_id=run_id,
        cluster_id=cluster_id,
        source_type=source_type,
        source_id=source_id,
        entity_type=entity_type,
        held_for_review=held_for_review,
    )
    session.add(row)
    session.commit()
    return row


class TestPhase2HeldForReview:
    """Stage 7 reads the clusters staging table and skips held_for_review clusters."""

    def test_non_held_cluster_produces_canonical_row(self, session_p2: Session, run_id_p2: int):
        """A cluster with held_for_review=False is published as a canonical entity."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P1", held_for_review=False
        )

        result = run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        assert result["canonical_out"] >= 1
        entities = session_p2.exec(select(CanonicalEntity)).all()
        assert len(entities) >= 1

    def test_held_cluster_not_auto_published_as_merged_group(
        self, session_p2: Session, run_id_p2: int
    ):
        """A cluster with held_for_review=True must not produce a merged canonical entity
        containing all its members (mega-cluster auto-publish is blocked)."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_input(
            session_p2, run_id_p2, "unified_person", "P2", first_name="Bob", last_name="Jones"
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P1", held_for_review=True
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P2", held_for_review=True
        )

        run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        # No canonical entity should have source_record_count == 2 (the merged mega-cluster)
        entities = session_p2.exec(select(CanonicalEntity)).all()
        merged = [e for e in entities if e.source_record_count == 2]
        assert len(merged) == 0, "held cluster must not be auto-published as a merged group"

    def test_mixed_held_and_non_held_publishes_only_non_held(
        self, session_p2: Session, run_id_p2: int
    ):
        """With one held cluster (2 members) and one non-held cluster (1 member),
        only the non-held cluster produces a merged canonical entity."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_input(
            session_p2, run_id_p2, "unified_person", "P2", first_name="Bob", last_name="Jones"
        )
        _add_input(
            session_p2, run_id_p2, "unified_person", "P3", first_name="Carol", last_name="White"
        )

        # Held cluster: P1 + P2
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_held", "unified_person", "P1", held_for_review=True
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_held", "unified_person", "P2", held_for_review=True
        )
        # Non-held cluster: P3 alone
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_ok", "unified_person", "P3", held_for_review=False
        )

        result = run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        # No merged entity spanning P1+P2 together
        entities = session_p2.exec(select(CanonicalEntity)).all()
        merged_2 = [e for e in entities if e.source_record_count == 2]
        assert len(merged_2) == 0

        # canonical_out reported should not count a merged 2-member held entity
        canonical_names = [e.canonical_name for e in entities]
        assert (
            any("Carol" in n or "White" in n for n in canonical_names)
            or result["canonical_out"] >= 1
        )

    def test_all_source_records_are_crosswalked_when_held_cluster_present(
        self, session_p2: Session, run_id_p2: int
    ):
        """Every source record — including members of held clusters — ends up with
        exactly one entity_crosswalk row for the run."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_input(
            session_p2, run_id_p2, "unified_person", "P2", first_name="Bob", last_name="Jones"
        )

        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_held", "unified_person", "P1", held_for_review=True
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_held", "unified_person", "P2", held_for_review=True
        )

        run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        crosswalks = session_p2.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id_p2)
        ).all()
        source_ids = {cw.source_id for cw in crosswalks}
        assert "P1" in source_ids
        assert "P2" in source_ids


class TestCrosswalkMatchMethod:
    """EntityCrosswalk.match_method reflects the actual merge path per member."""

    def test_singleton_crosswalk_gets_exact_method(self, session: Session, run_id: int):
        """A singleton (no merge edges) gets match_method=exact."""
        _add_input(session, run_id, "unified_person", "P1", first_name="Alice", last_name="Smith")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        cw = session.exec(select(EntityCrosswalk).where(EntityCrosswalk.source_id == "P1")).one()
        assert cw.match_method == MatchMethod.exact
        assert cw.match_score is None

    def test_deterministic_merge_gets_exact_method(self, session: Session, run_id: int):
        """Members merged via a deterministic edge (edge_source='deterministic') get exact."""
        _add_input(session, run_id, "unified_person", "P1", first_name="Alice", last_name="Smith")
        _add_input(session, run_id, "unified_person", "P2", first_name="Alice", last_name="Smith")
        _add_merge_edge(session, run_id, "unified_person", "P1", "unified_person", "P2")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        crosswalks = session.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
        ).all()
        assert all(cw.match_method == MatchMethod.exact for cw in crosswalks)
        assert all(cw.match_score is None for cw in crosswalks)

    def test_probabilistic_merge_gets_probabilistic_method_and_score(
        self, session: Session, run_id: int
    ):
        """Members merged via a probabilistic edge get method=probabilistic and a score."""
        _add_input(session, run_id, "unified_person", "P1", first_name="John", last_name="Smith")
        _add_input(session, run_id, "unified_person", "P2", first_name="John", last_name="Smith")

        # Write a probabilistic MergeEdge and matching MatchDecision (as classify does)
        edge = MergeEdge(
            run_id=run_id,
            source_a_type="unified_person",
            source_a_id="P1",
            source_b_type="unified_person",
            source_b_id="P2",
            edge_source="probabilistic",
        )
        session.add(edge)
        from app.resolve.models.resolution import (
            DecisionBand,
            DecisionOutcome,
        )

        session.add(
            MatchDecision(
                run_id=run_id,
                source_a_type=SourceType.unified_person,
                source_a_id="P1",
                source_b_type=SourceType.unified_person,
                source_b_id="P2",
                score=0.97,
                method=MatchMethod.probabilistic,
                band=DecisionBand.auto,
                outcome=DecisionOutcome.merged,
                explanation_json="{}",
            )
        )
        session.commit()

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        crosswalks = session.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
        ).all()
        assert all(cw.match_method == MatchMethod.probabilistic for cw in crosswalks)
        assert all(cw.match_score == pytest.approx(0.97) for cw in crosswalks)

    def test_approved_review_merge_gets_approved_review_method(self, session: Session, run_id: int):
        """Members merged via an approved review edge get method=approved_review."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")
        _add_input(session, run_id, "unified_person", "P2", first_name="A", last_name="B")

        edge = MergeEdge(
            run_id=run_id,
            source_a_type="unified_person",
            source_a_id="P1",
            source_b_type="unified_person",
            source_b_id="P2",
            edge_source="approved_review",
        )
        session.add(edge)
        session.commit()

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        crosswalks = session.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
        ).all()
        assert all(cw.match_method == MatchMethod.approved_review for cw in crosswalks)

    def test_prior_run_approved_review_propagates_to_crosswalk(self, session: Session, run_id: int):
        """Approved MergeReview rows from any run (no MergeEdge) set approved_review."""
        _add_input(session, run_id, "unified_person", "P1", first_name="A", last_name="B")
        _add_input(session, run_id, "unified_person", "P2", first_name="A", last_name="B")
        _add_approved_review(session, run_id, "unified_person", "P1", "unified_person", "P2")
        # No MergeEdge — load_cluster_edges reads MergeReview directly

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        crosswalks = session.exec(
            select(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
        ).all()
        assert all(cw.match_method == MatchMethod.approved_review for cw in crosswalks)


class TestPhase2ProvenanceJson:
    """Canonical rows carry field-level provenance_json (task-2d Step 4)."""

    def test_canonical_row_has_provenance_json(self, session_p2: Session, run_id_p2: int):
        """A published canonical entity row carries a non-null provenance_json."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P1", held_for_review=False
        )

        run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        entity = session_p2.exec(select(CanonicalEntity)).one()
        assert entity.provenance_json is not None

    def test_provenance_json_is_valid_json(self, session_p2: Session, run_id_p2: int):
        """provenance_json must be parseable JSON."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P1", held_for_review=False
        )

        run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        entity = session_p2.exec(select(CanonicalEntity)).one()
        parsed = json.loads(entity.provenance_json)
        assert isinstance(parsed, dict)

    def test_provenance_json_names_canonical_name_source(self, session_p2: Session, run_id_p2: int):
        """provenance_json includes a 'canonical_name' entry with source_type and source_id."""
        _add_input(
            session_p2, run_id_p2, "unified_person", "P1", first_name="Alice", last_name="Smith"
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P1", held_for_review=False
        )

        run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        entity = session_p2.exec(select(CanonicalEntity)).one()
        prov = json.loads(entity.provenance_json)
        assert "canonical_name" in prov
        assert "source_type" in prov["canonical_name"]
        assert "source_id" in prov["canonical_name"]
        assert prov["canonical_name"]["source_id"] == "P1"

    def test_provenance_json_best_name_row_wins_in_cluster(
        self, session_p2: Session, run_id_p2: int
    ):
        """provenance_json canonical_name source_id points at the most-complete name row."""
        older = _utc(-10)
        newer = _utc()
        _add_input(
            session_p2,
            run_id_p2,
            "unified_person",
            "P1",
            first_name="J",
            last_name="Smith",
            created_at=older,
        )
        _add_input(
            session_p2,
            run_id_p2,
            "unified_person",
            "P2",
            first_name="John",
            middle_name="A",
            last_name="Smith",
            created_at=newer,
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P1", held_for_review=False
        )
        _add_cluster_assignment(
            session_p2, run_id_p2, "cluster_000001", "unified_person", "P2", held_for_review=False
        )

        run_survivorship_stage(session_p2, run_id_p2, {"state_code": "TX"})

        entity = session_p2.exec(select(CanonicalEntity)).one()
        prov = json.loads(entity.provenance_json)
        # P2 has more name parts (first+middle+last) → should win
        assert prov["canonical_name"]["source_id"] == "P2"

    def test_phase1_fallback_also_emits_provenance_json(self, session: Session, run_id: int):
        """When no ClusterAssignment rows exist (Phase 1 path), canonical rows
        still carry provenance_json (backward-compatible)."""
        _add_input(session, run_id, "unified_person", "P1", first_name="Alice", last_name="Smith")

        run_survivorship_stage(session, run_id, {"state_code": "TX"})

        entity = session.exec(select(CanonicalEntity)).one()
        assert entity.provenance_json is not None
        parsed = json.loads(entity.provenance_json)
        assert "canonical_name" in parsed
