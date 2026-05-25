"""Tests for the cross-state hook primitives."""

from __future__ import annotations

import pytest
from sqlmodel import Session, SQLModel, create_engine

from app.resolve.models.canonical import CanonicalEntity, EntityType
from app.resolve.publish.crossstate import (
    entities_for_master,
    get_master_entity,
    link_to_master,
)


def _build_session() -> Session:
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=[CanonicalEntity.__table__])
    return Session(engine)


def _create_entity(session: Session, *, state_code: str) -> CanonicalEntity:
    entity = CanonicalEntity(
        entity_type=EntityType.person,
        canonical_name=f"entity-{state_code}",
        normalized_name=f"entity-{state_code}".lower(),
        state_code=state_code,
    )
    session.add(entity)
    session.commit()
    session.refresh(entity)
    return entity


def test_link_to_master_sets_reference():
    with _build_session() as session:
        master = _create_entity(session, state_code="MA")
        child = _create_entity(session, state_code="TX")

        link_to_master(session, canonical_entity_id=child.id, master_entity_id=master.id)

        session.refresh(child)
        assert child.master_entity_id == master.id


def test_get_master_entity_follows_chain():
    with _build_session() as session:
        master = _create_entity(session, state_code="CA")
        intermediary = _create_entity(session, state_code="VA")
        terminal = _create_entity(session, state_code="OR")

        link_to_master(session, canonical_entity_id=intermediary.id, master_entity_id=master.id)
        link_to_master(session, canonical_entity_id=terminal.id, master_entity_id=intermediary.id)

        resolved = get_master_entity(session, canonical_entity_id=terminal.id)
        assert resolved is not None
        assert resolved.id == master.id


def test_entities_for_master_groups_all_states():
    with _build_session() as session:
        master = _create_entity(session, state_code="WA")
        linked1 = _create_entity(session, state_code="OR")
        linked2 = _create_entity(session, state_code="ID")
        unrelated = _create_entity(session, state_code="FL")

        link_to_master(session, canonical_entity_id=linked1.id, master_entity_id=master.id)
        link_to_master(session, canonical_entity_id=linked2.id, master_entity_id=master.id)

        grouped = entities_for_master(session, master_entity_id=master.id)
        grouped_ids = {entity.id for entity in grouped}

        assert master.id in grouped_ids
        assert linked1.id in grouped_ids
        assert linked2.id in grouped_ids
        assert unrelated.id not in grouped_ids
        assert len(grouped) == 3


def test_link_to_master_rejects_self_link():
    with _build_session() as session:
        entity = _create_entity(session, state_code="NY")

        with pytest.raises(ValueError):
            link_to_master(
                session,
                canonical_entity_id=entity.id,
                master_entity_id=entity.id,
            )


def test_link_to_master_detects_cycles():
    with _build_session() as session:
        master = _create_entity(session, state_code="PA")
        child = _create_entity(session, state_code="OH")

        link_to_master(session, canonical_entity_id=child.id, master_entity_id=master.id)

        with pytest.raises(ValueError):
            link_to_master(session, canonical_entity_id=master.id, master_entity_id=child.id)


def test_entities_for_master_nested_chain():
    with _build_session() as session:
        master = _create_entity(session, state_code="WA")
        intermediary = _create_entity(session, state_code="OR")
        terminal = _create_entity(session, state_code="ID")

        link_to_master(session, canonical_entity_id=intermediary.id, master_entity_id=master.id)
        link_to_master(session, canonical_entity_id=terminal.id, master_entity_id=intermediary.id)

        grouped = entities_for_master(session, master_entity_id=master.id)
        grouped_ids = {entity.id for entity in grouped}

        assert grouped_ids == {master.id, intermediary.id, terminal.id}
