"""Travel-dedup integration test: TRVL record -> traveller entity -> resolution_input.

Scope
-----
The processor maps TRVL records to ``PersonRole.PAYEE`` with field prefix
``"traveller"`` (see ``app/core/processor.py`` RECORD_TYPE_ROLE_MAP).  In the
unified layer, a TRVL row produces a ``UnifiedPerson`` (the traveller) linked
via an entity row with ``entity_type=EntityType.PERSON``.

This test seeds a ``UnifiedPerson`` that represents a traveller (as the
processor would produce) and runs Stage 1 (``build_resolution_input`` /
``_compute_features``) to verify that the traveller-derived person appears in
``resolution_input`` with the correct ``source_type`` and ``entity_type``.

Full pipeline wiring (processor -> loader -> stage1) against real TRVL parquet
files requires ``tmp/texas`` data; that is out of scope here.  This test
exercises the Stage 1 boundary: given a ``UnifiedPerson`` in the DB, stage1
must produce a ``ResolutionInput`` row for it.  The person can originate from
any record type (TRVL, EXPN, RCPT) — the resolve pipeline is agnostic to TEC
record type; it resolves entities, not records.
"""

from __future__ import annotations

import pytest
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, SQLModel, create_engine, select

import app.resolve.models  # noqa: F401 — registers ORM tables
from app.core.models import (
    State,
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.resolve.blocking import CandidatePair
from app.resolve.models.canonical import (
    CanonicalAddress,
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalNameHistory,
)
from app.resolve.models.resolution import (
    AddressCrosswalk,
    CampaignCrosswalk,
    EntityCrosswalk,
    MatchDecision,
    MatchRun,
    MergeReview,
    PassType,
)
from app.resolve.stages.cluster import ClusterAssignment
from app.resolve.stages.fastpath import MergeEdge
from app.resolve.standardize.stage1 import _compute_features, build_resolution_input
from app.resolve.standardize.staging import ResolutionInput

# ---------------------------------------------------------------------------
# Tables needed for stage-1 tests
# ---------------------------------------------------------------------------

_TABLES_TO_CREATE = [
    State.__table__,
    UnifiedAddress.__table__,
    UnifiedPerson.__table__,
    UnifiedCommittee.__table__,
    UnifiedEntity.__table__,
    # Resolution pipeline tables
    MatchRun.__table__,
    ResolutionInput.__table__,
    CandidatePair.__table__,
    MergeEdge.__table__,
    MatchDecision.__table__,
    MergeReview.__table__,
    CanonicalAddress.__table__,
    CanonicalEntity.__table__,
    CanonicalCampaign.__table__,
    CanonicalNameHistory.__table__,
    EntityCrosswalk.__table__,
    AddressCrosswalk.__table__,
    CampaignCrosswalk.__table__,
    ClusterAssignment.__table__,
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine():
    """In-memory SQLite engine with the tables required for stage-1."""
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(eng, tables=_TABLES_TO_CREATE)
    return eng


@pytest.fixture()
def seeded_engine(engine):
    """Engine pre-seeded with a traveller-sourced person and a MatchRun."""
    with Session(engine) as session:
        state = State(code="TX", name="Texas")
        session.add(state)
        session.flush()

        # A traveller person: no address (TRVL public data omits contributor
        # street lines — same source-data limitation as contribution CSVs).
        traveller = UnifiedPerson(
            first_name="Alice",
            last_name="Traveller",
            state_id=state.id,
        )
        session.add(traveller)
        session.flush()

        run = MatchRun(state_code="TX", pass_type=PassType.entity)
        session.add(run)
        session.commit()

    return engine


# ---------------------------------------------------------------------------
# Stage-1 unit tests (traveller via _compute_features)
# ---------------------------------------------------------------------------


class TestTravellerViaComputeFeatures:
    """_compute_features picks up traveller-shaped rows correctly."""

    def test_traveller_row_produces_resolution_input(self):
        """A row dict shaped like a TRVL-sourced person becomes a ResolutionInput."""
        rows = [
            {
                "source_type": "unified_person",
                "source_id": "42",
                "entity_type": "person",
                "raw_name": "Alice Traveller",
                "raw_address": "",
            }
        ]
        staged = _compute_features(rows, run_id=1)

        assert len(staged) == 1
        ri = staged[0]
        assert ri.source_type == "unified_person"
        assert ri.source_id == "42"
        assert ri.entity_type == "person"
        assert ri.run_id == 1

    def test_traveller_name_is_standardized(self):
        """First/last name fields are populated from the raw_name."""
        rows = [
            {
                "source_type": "unified_person",
                "source_id": "99",
                "entity_type": "person",
                "raw_name": "Bob Wanderer",
                "raw_address": "",
            }
        ]
        staged = _compute_features(rows, run_id=7)

        ri = staged[0]
        # standardize_name should parse the two-token name into first+last
        assert ri.last_name is not None
        assert ri.last_name.lower() == "wanderer"

    def test_two_identical_travellers_produce_two_resolution_inputs(self):
        """Duplicate traveller rows both appear in resolution_input (dedup is
        stage-2/3's job, not stage-1's)."""
        rows = [
            {
                "source_type": "unified_person",
                "source_id": "1",
                "entity_type": "person",
                "raw_name": "Carol Smith",
                "raw_address": "",
            },
            {
                "source_type": "unified_person",
                "source_id": "2",
                "entity_type": "person",
                "raw_name": "Carol Smith",
                "raw_address": "",
            },
        ]
        staged = _compute_features(rows, run_id=3)

        assert len(staged) == 2
        source_ids = {ri.source_id for ri in staged}
        assert source_ids == {"1", "2"}


# ---------------------------------------------------------------------------
# Stage-1 integration tests (traveller via build_resolution_input DB query)
# ---------------------------------------------------------------------------


class TestTravellerViaStage1:
    """build_resolution_input picks up a traveller-sourced UnifiedPerson."""

    def test_traveller_person_appears_in_resolution_input(self, seeded_engine):
        """A UnifiedPerson seeded as a traveller produces a ResolutionInput row."""
        engine = seeded_engine
        with Session(engine) as session:
            run = session.exec(select(MatchRun)).first()
            run_id = run.id
            count = build_resolution_input(session, run_id=run_id, state_code="TX")

        assert count >= 1

        with Session(engine) as session:
            inputs = session.exec(
                select(ResolutionInput).where(ResolutionInput.run_id == run_id)
            ).all()

        assert len(inputs) >= 1
        person_inputs = [ri for ri in inputs if ri.source_type == "unified_person"]
        assert len(person_inputs) >= 1

    def test_traveller_resolution_input_has_correct_entity_type(self, seeded_engine):
        """The ResolutionInput row for a traveller-person has entity_type='person'."""
        engine = seeded_engine
        with Session(engine) as session:
            run = session.exec(select(MatchRun)).first()
            run_id = run.id
            build_resolution_input(session, run_id=run_id, state_code="TX")

        with Session(engine) as session:
            inputs = session.exec(
                select(ResolutionInput).where(ResolutionInput.run_id == run_id)
            ).all()

        person_inputs = [ri for ri in inputs if ri.source_type == "unified_person"]
        assert all(ri.entity_type == "person" for ri in person_inputs)

    def test_traveller_name_parsed_into_resolution_input(self, seeded_engine):
        """The traveller's name is standardized into first_name/last_name fields."""
        engine = seeded_engine
        with Session(engine) as session:
            run = session.exec(select(MatchRun)).first()
            run_id = run.id
            build_resolution_input(session, run_id=run_id, state_code="TX")

        with Session(engine) as session:
            inputs = session.exec(
                select(ResolutionInput).where(ResolutionInput.run_id == run_id)
            ).all()

        # "Alice Traveller" should parse to last_name="Traveller"
        person_inputs = [ri for ri in inputs if ri.source_type == "unified_person"]
        last_names = [ri.last_name for ri in person_inputs if ri.last_name]
        assert any("traveller" in ln.lower() for ln in last_names)
