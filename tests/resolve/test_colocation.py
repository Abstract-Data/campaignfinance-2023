"""Task 4c — co_located_with association tests (TDD).

Covers:
- find_colocated returns all entities at an address.
- find_colocated returns empty for an unknown address.
- assert_colocation creates a CO_LOCATED_WITH association.
- assert_colocation refuses a self-link (SelfColocationError).
- assert_colocation stores reason and asserted_by on the row.
- assert_colocation does NOT write to entity_crosswalk.
- assert_colocation does NOT modify canonical_entity rows.
- suggest_colocations returns pairs for a low-frequency address.
- suggest_colocations returns nothing above max_address_frequency.
- suggest_colocations is advisory — no association rows created.
- suggest_colocations does NOT write to entity_crosswalk.
"""

from __future__ import annotations

import json

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.core.unified_sqlmodels import AssociationType, UnifiedEntityAssociation
from app.resolve.models.canonical import CanonicalAddress, CanonicalEntity, EntityType
from app.resolve.models.resolution import EntityCrosswalk
from app.resolve.publish.colocation import (
    SelfColocationError,
    assert_colocation,
    find_colocated,
    suggest_colocations,
)

# ---------------------------------------------------------------------------
# Shared in-memory engine
# ---------------------------------------------------------------------------

_TABLES = [
    CanonicalAddress.__table__,
    CanonicalEntity.__table__,
    UnifiedEntityAssociation.__table__,
    EntityCrosswalk.__table__,
]


@pytest.fixture
def engine():
    """Fresh in-memory SQLite engine per test.

    SQLite does not enforce FK constraints by default, so inserting canonical
    entity IDs into UnifiedEntityAssociation.source/target_entity_id (which
    carry a declared FK to unified_entities.id) works without creating the
    unified_entities table.
    """
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(eng, tables=_TABLES)
    yield eng
    eng.dispose()


@pytest.fixture
def session(engine):
    """Fresh session per test; rolls back after each test."""
    with Session(engine) as s:
        yield s
        s.rollback()


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _make_address(session: Session, *, frequency: int = 1) -> CanonicalAddress:
    addr = CanonicalAddress(
        standardized_line_1="123 Main St",
        city="Austin",
        state="TX",
        zip5="78701",
        frequency=frequency,
    )
    session.add(addr)
    session.flush()
    return addr


def _make_entity(
    session: Session,
    canonical_address_id: int | None = None,
    *,
    name: str = "Test Entity",
) -> CanonicalEntity:
    entity = CanonicalEntity(
        entity_type=EntityType.person,
        canonical_name=name,
        normalized_name=name.lower(),
        canonical_address_id=canonical_address_id,
        state_code="TX",
    )
    session.add(entity)
    session.flush()
    return entity


# ---------------------------------------------------------------------------
# find_colocated
# ---------------------------------------------------------------------------


class TestFindColocated:
    def test_returns_all_entities_at_address(self, session):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice Smith")
        e2 = _make_entity(session, addr.id, name="Bob Smith")
        other_addr = _make_address(session)
        _make_entity(session, other_addr.id, name="Charlie Jones")

        result = find_colocated(session, addr.id)

        assert len(result) == 2
        assert {r.id for r in result} == {e1.id, e2.id}

    def test_returns_empty_for_unknown_address(self, session):
        result = find_colocated(session, 999_999)
        assert result == []

    def test_single_entity_at_address(self, session):
        addr = _make_address(session)
        e = _make_entity(session, addr.id, name="Solo Filer")
        result = find_colocated(session, addr.id)
        assert result == [e]

    def test_entities_without_address_are_excluded(self, session):
        addr = _make_address(session)
        _make_entity(session, addr.id, name="Has Address")
        _make_entity(session, None, name="No Address")

        result = find_colocated(session, addr.id)
        assert len(result) == 1
        assert result[0].canonical_name == "Has Address"


# ---------------------------------------------------------------------------
# assert_colocation
# ---------------------------------------------------------------------------


class TestAssertColocation:
    def test_creates_co_located_with_association(self, session):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        assoc = assert_colocation(
            session, e1.id, e2.id, reason="household", asserted_by="analyst"
        )

        assert assoc.id is not None
        assert assoc.source_entity_id == e1.id
        assert assoc.target_entity_id == e2.id
        assert assoc.association_type == AssociationType.CO_LOCATED_WITH

    def test_association_persists_after_commit(self, session):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        assert_colocation(
            session, e1.id, e2.id, reason="shared office", asserted_by="system"
        )
        session.commit()

        rows = session.exec(select(UnifiedEntityAssociation)).all()
        assert len(rows) == 1
        assert rows[0].association_type == AssociationType.CO_LOCATED_WITH

    def test_refuses_self_link_raises_self_colocation_error(self, session):
        addr = _make_address(session)
        e = _make_entity(session, addr.id)

        with pytest.raises(SelfColocationError):
            assert_colocation(
                session, e.id, e.id, reason="test", asserted_by="test"
            )

    def test_self_link_refusal_creates_no_rows(self, session):
        addr = _make_address(session)
        e = _make_entity(session, addr.id)

        with pytest.raises(SelfColocationError):
            assert_colocation(
                session, e.id, e.id, reason="test", asserted_by="test"
            )

        rows = session.exec(select(UnifiedEntityAssociation)).all()
        assert rows == []

    def test_stores_reason_in_description(self, session):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        assoc = assert_colocation(
            session, e1.id, e2.id, reason="household", asserted_by="reviewer"
        )

        assert assoc.description == "household"

    def test_stores_asserted_by_in_metadata(self, session):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        assoc = assert_colocation(
            session, e1.id, e2.id, reason="test", asserted_by="reviewer@example.com"
        )

        meta = json.loads(assoc.metadata_json)
        assert meta["asserted_by"] == "reviewer@example.com"

    def test_co_located_with_enum_value_is_defined(self):
        assert AssociationType.CO_LOCATED_WITH == "co_located_with"


# ---------------------------------------------------------------------------
# suggest_colocations
# ---------------------------------------------------------------------------


class TestSuggestColocations:
    def test_returns_pair_for_two_entities_at_low_frequency_address(self, session):
        addr = _make_address(session, frequency=2)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        suggestions = suggest_colocations(session, addr.id, max_address_frequency=5)

        assert len(suggestions) == 1
        pair_ids = {e.id for e in suggestions[0]}
        assert pair_ids == {e1.id, e2.id}

    def test_returns_empty_above_max_frequency(self, session):
        addr = _make_address(session, frequency=100)
        _make_entity(session, addr.id, name="Alice")
        _make_entity(session, addr.id, name="Bob")

        suggestions = suggest_colocations(session, addr.id, max_address_frequency=5)

        assert suggestions == []

    def test_at_max_frequency_boundary_returns_pairs(self, session):
        """Exactly at max_address_frequency is still low-frequency."""
        addr = _make_address(session, frequency=5)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        suggestions = suggest_colocations(session, addr.id, max_address_frequency=5)

        assert len(suggestions) == 1
        pair_ids = {e.id for e in suggestions[0]}
        assert pair_ids == {e1.id, e2.id}

    def test_one_above_boundary_returns_empty(self, session):
        addr = _make_address(session, frequency=6)
        _make_entity(session, addr.id, name="Alice")
        _make_entity(session, addr.id, name="Bob")

        suggestions = suggest_colocations(session, addr.id, max_address_frequency=5)

        assert suggestions == []

    def test_single_entity_produces_no_pairs(self, session):
        addr = _make_address(session, frequency=1)
        _make_entity(session, addr.id)

        suggestions = suggest_colocations(session, addr.id, max_address_frequency=10)

        assert suggestions == []

    def test_three_entities_produce_three_pairs(self, session):
        addr = _make_address(session, frequency=3)
        _make_entity(session, addr.id, name="Alice")
        _make_entity(session, addr.id, name="Bob")
        _make_entity(session, addr.id, name="Carol")

        suggestions = suggest_colocations(session, addr.id, max_address_frequency=10)

        assert len(suggestions) == 3

    def test_advisory_only_creates_no_association_rows(self, session):
        addr = _make_address(session, frequency=2)
        _make_entity(session, addr.id, name="Alice")
        _make_entity(session, addr.id, name="Bob")

        suggest_colocations(session, addr.id, max_address_frequency=5)

        rows = session.exec(select(UnifiedEntityAssociation)).all()
        assert rows == [], "suggest_colocations must not create association rows"

    def test_returns_empty_for_unknown_address(self, session):
        result = suggest_colocations(session, 999_999, max_address_frequency=5)
        assert result == []


# ---------------------------------------------------------------------------
# Step 4 — No crosswalk or canonical-entity writes
# ---------------------------------------------------------------------------


class TestNoMergeWrites:
    """Nothing in this module writes to entity_crosswalk or modifies
    canonical_entity rows.  Assertions only — no merges."""

    def test_assert_colocation_does_not_write_to_crosswalk(self, session):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        assert_colocation(
            session, e1.id, e2.id, reason="household", asserted_by="test"
        )
        session.commit()

        crosswalk = session.exec(select(EntityCrosswalk)).all()
        assert crosswalk == [], "assert_colocation must not write to entity_crosswalk"

    def test_assert_colocation_does_not_create_new_canonical_entity_rows(
        self, session
    ):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        before_count = len(session.exec(select(CanonicalEntity)).all())
        assert_colocation(
            session, e1.id, e2.id, reason="test", asserted_by="test"
        )
        session.commit()
        after_count = len(session.exec(select(CanonicalEntity)).all())

        assert after_count == before_count, (
            "assert_colocation must not create new canonical_entity rows"
        )

    def test_assert_colocation_does_not_modify_existing_canonical_entity(
        self, session
    ):
        addr = _make_address(session)
        e1 = _make_entity(session, addr.id, name="Alice")
        e2 = _make_entity(session, addr.id, name="Bob")

        snap_a = (e1.canonical_name, e1.entity_type, e1.canonical_address_id)
        snap_b = (e2.canonical_name, e2.entity_type, e2.canonical_address_id)

        assert_colocation(
            session, e1.id, e2.id, reason="test", asserted_by="test"
        )
        session.commit()
        session.refresh(e1)
        session.refresh(e2)

        assert (e1.canonical_name, e1.entity_type, e1.canonical_address_id) == snap_a
        assert (e2.canonical_name, e2.entity_type, e2.canonical_address_id) == snap_b

    def test_suggest_colocations_does_not_write_to_crosswalk(self, session):
        addr = _make_address(session, frequency=2)
        _make_entity(session, addr.id, name="Alice")
        _make_entity(session, addr.id, name="Bob")

        suggest_colocations(session, addr.id, max_address_frequency=5)
        session.commit()

        crosswalk = session.exec(select(EntityCrosswalk)).all()
        assert crosswalk == []

    def test_suggest_colocations_does_not_create_canonical_entity_rows(
        self, session
    ):
        addr = _make_address(session, frequency=2)
        _make_entity(session, addr.id, name="Alice")
        _make_entity(session, addr.id, name="Bob")

        before_count = len(session.exec(select(CanonicalEntity)).all())
        suggest_colocations(session, addr.id, max_address_frequency=5)
        session.commit()
        after_count = len(session.exec(select(CanonicalEntity)).all())

        assert after_count == before_count
