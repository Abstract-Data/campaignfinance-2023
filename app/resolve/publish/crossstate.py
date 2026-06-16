"""Cross-state hook primitives for the canonical entity master reference.

The ``canonical_entity.master_entity_id`` column is reserved for a future
cross-state resolution pass.  This module exposes low-level helpers that can
prove the self-foreign-key is sound and make it safer for later work to build a
cross-state linking pass on top of it.  No matching, scoring, or cross-state
aggregation logic lives here.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session, select

from app.resolve.models.canonical import CanonicalEntity

_MASTER_GROUP_IDS_SQL = """
WITH RECURSIVE master_group(id) AS (
    SELECT :master_id
    UNION ALL
    SELECT ce.id
    FROM canonical_entity ce
    INNER JOIN master_group mg ON ce.master_entity_id = mg.id
)
SELECT id FROM master_group
"""


def get_master_entity(
    session: Session,
    canonical_entity_id: int,
) -> CanonicalEntity | None:
    """Return the master that ``canonical_entity_id`` points at or ``None``."""

    entity = _fetch_entity(session, canonical_entity_id)
    if entity.master_entity_id is None:
        return None

    visited: set[int] = {entity.id}
    current = entity
    while current.master_entity_id is not None:
        next_id = current.master_entity_id
        if next_id in visited:
            raise ValueError("cycle detected while following master_entity_id")
        visited.add(next_id)

        next_entity = _fetch_entity(session, next_id)
        current = next_entity

    return current


def entities_for_master(
    session: Session,
    master_entity_id: int,
) -> list[CanonicalEntity]:
    """Return the master plus every entity whose master chain ends at it."""

    _fetch_entity(session, master_entity_id)

    rows = session.execute(
        text(_MASTER_GROUP_IDS_SQL),
        {"master_id": master_entity_id},
    ).all()
    ids = [row[0] for row in rows]
    if not ids:
        return []

    return list(session.exec(select(CanonicalEntity).where(CanonicalEntity.id.in_(ids))).all())


def link_to_master(
    session: Session,
    canonical_entity_id: int,
    master_entity_id: int,
) -> None:
    """Set the ``master_entity_id`` pointer while guarding against cycles."""

    if canonical_entity_id == master_entity_id:
        raise ValueError("cannot link an entity to itself as its master")

    canonical = _fetch_entity(session, canonical_entity_id)
    master = _fetch_entity(session, master_entity_id)

    visited: set[int] = {canonical.id}
    current = master
    while True:
        if current.id in visited:
            raise ValueError("cycle detected while linking to master")
        visited.add(current.id)

        if current.master_entity_id is None:
            break

        current = _fetch_entity(session, current.master_entity_id)

    canonical.master_entity_id = master.id
    session.add(canonical)
    session.flush()


def _fetch_entity(session: Session, canonical_entity_id: int) -> CanonicalEntity:
    entity = session.get(CanonicalEntity, canonical_entity_id)
    if entity is None:
        raise ValueError(f"canonical entity with id {canonical_entity_id} does not exist")
    return entity
