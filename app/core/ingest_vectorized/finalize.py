"""Post-load finalization for the vectorized ingest engine.

``finalize_entity_representatives`` is the SINGLE, deterministic step that assigns each
PERSON/ORGANIZATION entity its representative ``person_id`` (and that person's
``address_id``). It replaces the three families' independent per-entity assignment, which
violated the ``unified_entities.person_id`` UNIQUE constraint on real Postgres data.

The bug it fixes: the person dedup key is ``(lower(first), lower(last))`` for individuals
(suffix EXCLUDED — matches ``BuilderCache.person_key`` / ``uix_persons_name_state``), but an
entity's ``normalized_name`` is ``normalize_entity_name(full_name)`` where ``full_name``
INCLUDES the suffix. So "JOHN ANDERSON" (no suffix) and "John Anderson JR" collapse to ONE
person yet spawn TWO entities (``PERSON:"john anderson"`` / ``PERSON:"john anderson jr"``).
When each family independently picked that one person as the representative of an entity it
touched, the single person ended up on BOTH entities -> ``unified_entities_person_id_key``
UniqueViolation.

The fix guarantees one-to-one by computing each PERSON entity's representative from the
entity side: every entity's ``(entity_type, normalized_name)`` maps to exactly one entity row
(the entity unique key), and we pick ONE person per entity (min person id). A person whose
(type, normalized_name) matches no entity, or an entity matched by no person (a suffix-variant
orphan), simply gets no/keeps-NULL representative — exactly as the ORM leaves orphaned
suffix-variant sibling entities with ``person_id = NULL``.

Mirrors the ORM's ``_link_after_load``: a post-load reconciliation that runs once, after every
family has written its dim rows, just before the session closes.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from app.core.ingest_vectorized import common


def _person_frame(session: Any, state_id: int) -> pl.DataFrame:
    """Persons for the state with their entity key (entity_type, normalized_name).

    The entity key is computed EXACTLY as the dim families compute it when they create the
    entity row: ``entity_type`` = ORGANIZATION when organization present else PERSON;
    ``normalized_name`` = ``normalize_entity_name(full_name_expr(...))`` where full_name is the
    organization when present else first/middle/last/suffix joined. Reusing the shared
    ``common`` expressions is what guarantees the join below matches the stored
    ``unified_entities.normalized_name``.
    """
    from app.core.models import UnifiedPerson

    rows = session.execute(
        select_person_cols(UnifiedPerson).where(UnifiedPerson.state_id == state_id)
    ).all()
    frame = pl.DataFrame(
        {
            "_pid": [r[0] for r in rows],
            "first_name": [r[1] for r in rows],
            "middle_name": [r[2] for r in rows],
            "last_name": [r[3] for r in rows],
            "suffix": [r[4] for r in rows],
            "organization": [r[5] for r in rows],
            "address_id": [r[6] for r in rows],
        },
        schema={
            "_pid": pl.Int64,
            "first_name": pl.Utf8,
            "middle_name": pl.Utf8,
            "last_name": pl.Utf8,
            "suffix": pl.Utf8,
            "organization": pl.Utf8,
            "address_id": pl.Int64,
        },
    )
    org = common.clean_str("organization")
    full_name = common.full_name_expr(
        "first_name", "middle_name", "last_name", "suffix", "organization"
    )
    entity_name = pl.when(org.is_not_null()).then(org).otherwise(full_name)
    return frame.with_columns(
        pl.when(org.is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .otherwise(pl.lit("PERSON"))
        .alias("_ent_type"),
        common.normalize_entity_name_expr(entity_name).alias("_ent_norm"),
    ).select(["_pid", "address_id", "_ent_type", "_ent_norm"])


def select_person_cols(model: type):
    """``select`` over the person columns finalize needs (id + name parts + address_id)."""
    from sqlalchemy import select

    return select(
        model.id,
        model.first_name,
        model.middle_name,
        model.last_name,
        model.suffix,
        model.organization,
        model.address_id,
    )


def _entity_frame(session: Any, state_id: int) -> pl.DataFrame:
    """PERSON/ORGANIZATION entities for the state, keyed on (entity_type, normalized_name).

    COMMITTEE entities are excluded — they keep their ``committee_id`` and never get a
    person representative. A ``""`` normalized_name is stored NULL by the ORM (coalesced to
    ``""`` here for join parity); the person side also never produces a blank-name entity key
    that should resolve, so an all-blank join target is harmless.
    """
    from sqlalchemy import select

    from app.core.models import UnifiedEntity

    rows = session.execute(
        select(
            UnifiedEntity.id,
            UnifiedEntity.entity_type,
            UnifiedEntity.normalized_name,
        ).where(UnifiedEntity.state_id == state_id)
    ).all()
    return pl.DataFrame(
        {
            "_ent_id": [r[0] for r in rows],
            "_ent_type": [getattr(r[1], "name", r[1]) for r in rows],
            "_ent_norm": [(r[2] if r[2] is not None else "") for r in rows],
        },
        schema={"_ent_id": pl.Int64, "_ent_type": pl.Utf8, "_ent_norm": pl.Utf8},
    ).filter(pl.col("_ent_type") != "COMMITTEE")


def finalize_entity_representatives(session: Any, state_id: int) -> int:
    """Assign each PERSON/ORGANIZATION entity ONE representative person, one-to-one.

    Joins persons -> entities on (entity_type, normalized_name); within each entity picks the
    minimum person id (deterministic). Because a person's (type, normalized_name) maps to
    exactly one entity, and each entity gets at most one person, the resulting
    ``unified_entities.person_id`` is unique. Sets ``person_id`` and ``address_id`` (the rep
    person's address) via a single parameterized core UPDATE; entities with no matching person
    are left untouched (``person_id`` stays NULL).

    Returns the number of entities updated.
    """
    from sqlalchemy import bindparam, update

    from app.core.models import UnifiedEntity

    persons = _person_frame(session, state_id)
    entities = _entity_frame(session, state_id)
    if persons.is_empty() or entities.is_empty():
        return 0

    # Resolve each person's entity id, then pick the min person id per entity. Blank-name
    # persons (no resolvable entity key) and committee entities never match here.
    matched = persons.filter(pl.col("_ent_norm") != "").join(
        entities, on=["_ent_type", "_ent_norm"], how="inner"
    )
    if matched.is_empty():
        return 0

    reps = (
        matched.sort("_pid")
        .group_by("_ent_id")
        .agg(
            pl.col("_pid").first().alias("_pid"),
            pl.col("address_id").first().alias("_aid"),
        )
    )

    params = [
        {"b_eid": r["_ent_id"], "b_pid": r["_pid"], "b_aid": r["_aid"]}
        for r in reps.to_dicts()
    ]
    if not params:
        return 0

    stmt = (
        update(UnifiedEntity.__table__)
        .where(UnifiedEntity.__table__.c.id == bindparam("b_eid"))
        .values(person_id=bindparam("b_pid"), address_id=bindparam("b_aid"))
    )
    session.connection().execute(stmt, params)
    session.commit()
    return len(params)
