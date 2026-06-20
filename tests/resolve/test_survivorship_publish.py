"""Characterize the survivorship delete-and-replace publish contract.

Stage 7 (``run_survivorship_stage``) clears the live canonical snapshot
(``_clear_live_canonical_snapshot``) before writing fresh golden records.
This test documents that behavior so that any change to the publish path
produces a visible test failure.

Plan ref: docs/superpowers/plans/2026-06-20-staging-publish-contract.md Task 1
"""

from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.models.canonical import (
    CanonicalAddress,
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalNameHistory,
)
from app.resolve.models.resolution import (
    EntityCrosswalk,
    MatchDecision,
    MatchRun,
    MergeReview,
    PassType,
    RunStatus,
)
from app.resolve.stages.cluster import ClusterAssignment
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.stages.survivorship import run_survivorship_stage
from app.resolve.standardize.staging import ResolutionInput


def _engine():
    """Create an in-memory SQLite engine with all tables required by Stage 7."""
    tables = [
        MatchRun.__table__,
        CanonicalAddress.__table__,  # FK target for CanonicalEntity.canonical_address_id
        CanonicalEntity.__table__,
        CanonicalCampaign.__table__,  # cleared first (FK → canonical_entity)
        CanonicalNameHistory.__table__,
        EntityCrosswalk.__table__,
        MatchDecision.__table__,  # queried by _build_node_crosswalk_attrs
        MergeEdge.__table__,
        MergeReview.__table__,
        ResolutionInput.__table__,
        ClusterAssignment.__table__,  # Phase 2 cluster staging; empty → Phase 1 fallback
    ]
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=tables)
    return engine


def test_survivorship_publish_replaces_live_canonical_snapshot():
    """Prior live canonical rows must be deleted before fresh records are inserted.

    Asserts:
    - The stale ``CanonicalEntity`` row seeded before the run is gone after publish.
    - At least one new canonical row exists (the source record produced a singleton cluster).
    """
    engine = _engine()
    with Session(engine) as session:
        # Seed a stale canonical row from a previous run.
        session.add(
            CanonicalEntity(
                entity_type="person",
                canonical_name="Stale",
                normalized_name="stale",
                state_code="TX",
            )
        )
        # Seed a MatchRun and one ResolutionInput for this run.
        session.add(
            MatchRun(id=1, state_code="TX", pass_type=PassType.entity, status=RunStatus.running)
        )
        session.add(
            ResolutionInput(
                run_id=1,
                source_type="unified_person",
                source_id="p1",
                entity_type="person",
                first_name="Ann",
                last_name="Adams",
                parse_status="parsed",
            )
        )
        session.commit()

        run_survivorship_stage(session, 1, {"state_code": "TX"})

        stale = session.exec(
            select(CanonicalEntity).where(CanonicalEntity.canonical_name == "Stale")
        ).first()
        assert stale is None, "prior live canonical row must be deleted before publish"

        live = session.exec(select(CanonicalEntity)).all()
        assert len(live) >= 1, "at least one canonical entity must exist after publish"
