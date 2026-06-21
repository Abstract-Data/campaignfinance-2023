"""Committee and party dim writes for the detail_children family.

Functions here are module-level (not methods); worker is passed explicitly
only where ``worker._addr_lookup`` is required.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.id_maps import (
    address_id_map as _address_id_map,
)
from app.core.ingest_vectorized.id_maps import (
    entity_id_map as _entity_id_map,
)
from app.core.ingest_vectorized.id_maps import (
    person_id_map as _person_id_map,
)
from app.core.ingest_vectorized.registry import FamilyContext
from app.core.models import (
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)

from .exprs import (
    _addr_key_cols,
    _address_has_anchor,
    _cs,
    _full_name,
    _norm_name,
    _spec_party_frame,
)
from .specs import _SPECS

if TYPE_CHECKING:
    from .worker import DetailChildrenWorker


# ---------------------------------------------------------------------------
# Committees
# ---------------------------------------------------------------------------


def write_committees(
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
    ctx: FamilyContext,
) -> int:
    parts: list[pl.DataFrame] = []
    for rt in ordered:
        df = frames[rt]
        part = df.select(
            _cs("filerIdent").alias("filer_id"),
            _cs("filerName").alias("name"),
            _cs("filerTypeCd").alias("committee_type"),
        ).filter(pl.col("filer_id").is_not_null())
        if part.height:
            parts.append(part)
    if not parts:
        return 0
    comb = pl.concat(parts, how="diagonal_relaxed")
    comb = comb.unique(subset=["filer_id"], keep="first", maintain_order=True)
    comb = comb.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("filer_status"),
        pl.lit(ctx.state_id).alias("state_id"),
    )
    # DO NOTHING on conflict (update_cols=[]): a committee already created by the
    # FILER family (authoritative name/type/status/address) must NOT be clobbered by
    # an incidental transaction filerName. FILER runs first (priority 0); this mirrors
    # the ORM's find-or-create first-occurrence-wins.
    return common.write_frame(
        ctx.session, UnifiedCommittee, comb, conflict_cols=["filer_id"], update_cols=[]
    )


# ---------------------------------------------------------------------------
# Party dims (addresses, persons, entities)
# ---------------------------------------------------------------------------


def all_parties(
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
) -> pl.DataFrame:
    """Concatenated per-row party frame across all types in load order."""
    parts: list[pl.DataFrame] = []
    offset = 0
    for rt in ordered:
        spec = _SPECS[rt]
        part = _spec_party_frame(frames[rt], spec, offset)
        offset += 10_000_000
        if part.height:
            parts.append(part)
    if not parts:
        return pl.DataFrame(
            schema={
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "suffix": pl.Utf8,
                "organization": pl.Utf8,
                "person_type": pl.Utf8,
                "a_street_1": pl.Utf8,
                "a_street_2": pl.Utf8,
                "a_city": pl.Utf8,
                "a_state": pl.Utf8,
                "a_zip": pl.Utf8,
                "a_country": pl.Utf8,
                "a_county": pl.Utf8,
                "_pk_org": pl.Utf8,
                "_pk_fn": pl.Utf8,
                "_pk_ln": pl.Utf8,
                "_pk_addr": pl.Utf8,
                "_sort_key": pl.Int64,
            }
        )
    return pl.concat(parts, how="diagonal_relaxed").sort("_sort_key")


def _committee_entity_frame(
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
) -> pl.DataFrame:
    parts: list[pl.DataFrame] = []
    for rt in ordered:
        df = frames[rt]
        part = df.select(
            _cs("filerIdent").alias("filer_id"),
            _cs("filerName").alias("name"),
        ).filter(pl.col("filer_id").is_not_null())
        if part.height:
            parts.append(part)
    if not parts:
        return pl.DataFrame(
            schema={
                "entity_type": pl.Utf8,
                "name": pl.Utf8,
                "normalized_name": pl.Utf8,
                "committee_id": pl.Utf8,
                "notes": pl.Utf8,
                "_sort_key": pl.Int64,
            }
        )
    comb = pl.concat(parts, how="diagonal_relaxed").unique(
        subset=["filer_id"], keep="first", maintain_order=True
    )
    return comb.with_columns(
        pl.lit("COMMITTEE").alias("entity_type"),
        _norm_name(pl.col("name")).alias("normalized_name"),
        pl.col("filer_id").alias("committee_id"),
        pl.lit(None, dtype=pl.Utf8).alias("notes"),
        pl.lit(-1, dtype=pl.Int64).alias("_sort_key"),
    ).select("entity_type", "name", "normalized_name", "committee_id", "notes", "_sort_key")


def write_dims(
    worker: DetailChildrenWorker,
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
    ctx: FamilyContext,
) -> tuple[int, int, int]:
    parties = all_parties(frames, ordered)
    if parties.height == 0:
        return 0, 0, 0

    # Omit-null address match: a street-less party (these record types carry no source
    # street) inherits a fuller existing address's street, so its dedup_addr_key matches
    # the ORM's. a_city/a_state/a_zip are already the cleaned/cased dim columns. Then
    # recompute _pk_addr from the (possibly inherited) street; org-persons keep NULL.
    parties = common.add_resolved_street(
        parties,
        worker._addr_lookup,
        city_col="a_city",
        state_col="a_state",
        zip_col="a_zip",
        out_col="a_street_1",
    )
    parties = parties.with_columns(
        pl.when(pl.col("_pk_org").is_not_null())
        .then(None)
        .otherwise(common.person_addr_key_expr("a_street_1", "a_city", "a_state", "a_zip"))
        .alias("_pk_addr")
    )

    # Deduped persons: first occurrence per (org, fn, ln, addr) key in load order.
    # The address dimension splits same-name individuals at distinct locations
    # (matching uix_persons_name_state); org-persons keep _pk_addr NULL.
    persons = parties.unique(
        subset=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
        keep="first",
        maintain_order=True,
    )
    # Each deduped person carries the 4-field address key of its first address
    # (null key components when no address anchor).
    persons = persons.with_columns(_addr_key_cols())

    # Addresses: each person's first address, globally deduped on the 4-field key.
    # Anti-join existing DB addresses (shared dim — another family or this state's
    # prior load may already hold the row) so plain inserts never hit the unique
    # index. The ORM does this via find-or-create.
    existing_addr = _address_id_map(ctx.engine)
    addr = (
        persons.filter(_address_has_anchor(persons))
        .unique(
            subset=["_k_s1", "_k_city", "_k_state", "_k_zip"],
            keep="first",
            maintain_order=True,
        )
        .join(
            existing_addr.select("_k_s1", "_k_city", "_k_state", "_k_zip"),
            on=["_k_s1", "_k_city", "_k_state", "_k_zip"],
            how="anti",
            join_nulls=True,
        )
    )
    addr_out = addr.select(
        pl.col("a_street_1").alias("street_1"),
        pl.col("a_street_2").alias("street_2"),
        pl.col("a_city").alias("city"),
        pl.col("a_state").alias("state"),
        pl.col("a_zip").alias("zip_code"),
        pl.col("a_country").alias("country"),
        pl.col("a_county").alias("county"),
    )
    n_addr = common.write_frame(ctx.session, UnifiedAddress, addr_out, conflict_cols=None)

    # address_id map: 4-field key -> surrogate id (for person/entity linkage).
    addr_map = _address_id_map(ctx.engine)
    persons = persons.join(
        addr_map,
        on=["_k_s1", "_k_city", "_k_state", "_k_zip"],
        how="left",
        join_nulls=True,
    )

    # Anti-join existing persons so we only INSERT new ones (shared person dim).
    # ``persons`` keeps the full deduped set (needed for entity.person_id linkage);
    # ``persons_new`` is the subset to actually insert.
    existing_persons = _person_id_map(ctx.engine, ctx.state_id)
    persons_new = persons.join(
        existing_persons.select("_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"),
        on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
        how="anti",
        join_nulls=True,
    )

    # Persons (with address_id).
    persons_out = persons_new.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("middle_name"),
        pl.lit(None, dtype=pl.Utf8).alias("employer"),
        pl.lit(None, dtype=pl.Utf8).alias("occupation"),
        pl.lit(None, dtype=pl.Utf8).alias("job_title"),
        pl.col("_pk_addr").alias("dedup_addr_key"),
        pl.lit(ctx.state_id).alias("state_id"),
    ).select(
        "first_name",
        "last_name",
        "middle_name",
        "suffix",
        "organization",
        "employer",
        "occupation",
        "job_title",
        "person_type",
        "dedup_addr_key",
        "address_id",
        "state_id",
    )
    n_persons = common.write_frame(ctx.session, UnifiedPerson, persons_out, conflict_cols=None)

    # Entities: person entities + committee entities (carry committee_id), deduped on
    # (entity_type, normalized_name) first-seen. The entity's representative person_id /
    # address_id are NOT set here — they are assigned once, deterministically, by
    # finalize_entity_representatives after all families run (a person can map to >1
    # entity via suffix-variant normalized names, so a per-family entity-rep assignment
    # violates the one-to-one unified_entities.person_id unique).
    full_name = _full_name(
        pl.col("first_name"), pl.col("last_name"), pl.col("suffix"), pl.col("organization")
    )
    person_entities = (
        persons.with_columns(
            pl.when(pl.col("organization").is_not_null())
            .then(pl.lit("ORGANIZATION"))
            .otherwise(pl.lit("PERSON"))
            .alias("entity_type"),
            pl.when(pl.col("organization").is_not_null())
            .then(pl.col("organization"))
            .otherwise(full_name)
            .alias("name"),
        )
        .with_columns(
            _norm_name(pl.col("name")).alias("normalized_name"),
            pl.lit(None, dtype=pl.Utf8).alias("committee_id"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
            pl.lit(None, dtype=pl.Int64).alias("person_id"),
            pl.lit(None, dtype=pl.Int64).alias("address_id"),
        )
        .select(
            "entity_type",
            "name",
            "normalized_name",
            "committee_id",
            "notes",
            "person_id",
            "address_id",
            "_sort_key",
        )
    )

    comm_entities = _committee_entity_frame(frames, ordered).with_columns(
        pl.lit(None, dtype=pl.Int64).alias("person_id"),
        pl.lit(None, dtype=pl.Int64).alias("address_id"),
    )

    ent_parts = [p for p in (comm_entities, person_entities) if p.height > 0]
    if ent_parts:
        entities = pl.concat(ent_parts, how="diagonal_relaxed")
        entities = entities.sort("_sort_key").unique(
            subset=["entity_type", "normalized_name"], keep="first", maintain_order=True
        )
        # Anti-join existing entities (shared entity dim) on the unique-index key
        # (entity_type, normalized_name) so plain inserts never collide.
        existing_ent = _entity_id_map(ctx.engine, ctx.state_id)
        entities = entities.join(
            existing_ent.select("entity_type", "normalized_name"),
            on=["entity_type", "normalized_name"],
            how="anti",
        )
        entities_out = entities.with_columns(pl.lit(ctx.state_id).alias("state_id")).select(
            "entity_type",
            "name",
            "normalized_name",
            "committee_id",
            "notes",
            "person_id",
            "address_id",
            "state_id",
        )
        n_entities = common.write_frame(
            ctx.session,
            UnifiedEntity,
            entities_out,
            conflict_cols=["entity_type", "normalized_name", "state_id"],
            update_cols=[],
            conflict_where="state_id IS NOT NULL",
        )
    else:
        n_entities = 0

    return n_addr, n_persons, n_entities
