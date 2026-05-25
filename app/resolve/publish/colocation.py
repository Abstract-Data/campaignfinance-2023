"""Co-location linker for the resolve publish layer.

Entities that share a canonical address are *not* the same entity — but
sometimes you want to record that they are related (a household, a shared
office).  This module lets such links be asserted via the existing
``UnifiedEntityAssociation`` pattern as a ``co_located_with`` edge.

Nothing here writes to ``entity_crosswalk`` or modifies a
``canonical_entity`` row.  Co-location produces *associations*, never merges.

Spec reference:
  docs/superpowers/specs/2026-05-23-data-resolution-pipeline-design.md
  § "Address-as-shared-hub model"
"""

from __future__ import annotations

import json
from itertools import combinations

from sqlmodel import Session, select

from app.core.unified_sqlmodels import AssociationType, UnifiedEntityAssociation
from app.resolve.models.canonical import CanonicalAddress, CanonicalEntity


class SelfColocationError(ValueError):
    """Raised when ``assert_colocation`` is called with the same entity twice."""


def find_colocated(
    session: Session,
    canonical_address_id: int,
) -> list[CanonicalEntity]:
    """Return all canonical entities whose ``canonical_address_id`` matches.

    Parameters
    ----------
    session:
        An active SQLModel session.
    canonical_address_id:
        Primary key of the target ``canonical_address`` row.

    Returns
    -------
    list[CanonicalEntity]
        Every canonical entity linked to that address (may be empty).
    """
    return list(
        session.exec(
            select(CanonicalEntity).where(
                CanonicalEntity.canonical_address_id == canonical_address_id
            )
        ).all()
    )


def assert_colocation(
    session: Session,
    entity_id_a: int,
    entity_id_b: int,
    *,
    reason: str,
    asserted_by: str,
) -> UnifiedEntityAssociation:
    """Record a ``co_located_with`` association between two distinct canonical entities.

    The association is stored as a ``UnifiedEntityAssociation`` row with
    ``association_type = CO_LOCATED_WITH``.  This function **never** writes to
    ``entity_crosswalk``, changes a ``canonical_entity`` row, or triggers a
    merge.  It only creates an association edge.

    Parameters
    ----------
    session:
        An active SQLModel session.  Caller is responsible for commit.
    entity_id_a, entity_id_b:
        Primary keys of the two ``canonical_entity`` rows to link.
    reason:
        Human-readable note explaining why the co-location is being recorded.
    asserted_by:
        Identifier of the reviewer or system that is asserting the link.

    Returns
    -------
    UnifiedEntityAssociation
        The newly flushed (but not committed) association row.

    Raises
    ------
    SelfColocationError
        If ``entity_id_a == entity_id_b``.
    """
    if entity_id_a == entity_id_b:
        raise SelfColocationError(
            f"Cannot assert co-location: entity {entity_id_a} cannot be "
            "linked to itself."
        )

    association = UnifiedEntityAssociation(
        source_entity_id=entity_id_a,
        target_entity_id=entity_id_b,
        association_type=AssociationType.CO_LOCATED_WITH,
        description=reason,
        metadata_json=json.dumps({"asserted_by": asserted_by}),
    )
    session.add(association)
    session.flush()
    return association


def suggest_colocations(
    session: Session,
    canonical_address_id: int,
    *,
    max_address_frequency: int,
) -> list[tuple[CanonicalEntity, CanonicalEntity]]:
    """Suggest entity pairs at a low-frequency address for human review.

    Addresses shared by many filers (registered-agent addresses, large office
    buildings) are *not* evidence of a household or shared-office relationship.
    Any address whose ``frequency`` exceeds ``max_address_frequency`` produces
    no suggestions.

    Suggestions are **advisory only**.  This function never creates association
    rows, never writes to the crosswalk, and never modifies a canonical entity.

    Parameters
    ----------
    session:
        An active SQLModel session.
    canonical_address_id:
        Primary key of the ``canonical_address`` to inspect.
    max_address_frequency:
        Upper bound (inclusive) on address frequency.  Addresses with
        ``frequency > max_address_frequency`` return an empty list.

    Returns
    -------
    list[tuple[CanonicalEntity, CanonicalEntity]]
        All unordered pairs of canonical entities at the address, or ``[]``
        if the address is too busy, unknown, or has fewer than two entities.
    """
    address = session.get(CanonicalAddress, canonical_address_id)
    if address is None or address.frequency > max_address_frequency:
        return []

    entities = find_colocated(session, canonical_address_id)
    return list(combinations(entities, 2))
