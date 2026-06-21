"""Vectorized FILER family: authoritative committees + their officers.

A TEC ``FILER`` row (``filers_*.parquet``) is the canonical source of committee
identity: it carries the full address, officer names, office sought, and status
that never appear on individual transaction rows. The vectorized engine
previously skipped FILER entirely, so committees were only created incidentally
from transaction ``filerName`` (missing the authoritative name / type / status /
address AND every committee officer) — a foundational gap that blocks correct
campaign names downstream.

This worker reproduces — columnar (pure Polars, no per-row Python callbacks)
— the ORM loader's ``app/core/source_models/filer_ingest.build_filer_committee``:

* ``unified_committees`` (find-or-create by ``filerIdent``): ``name`` <- filerName,
  ``committee_type`` <- filerTypeCd, ``filer_status`` <- committeeStatusCd, plus a
  ``filer``-prefixed address (street_1 / city / state / zip ONLY — mirrors
  ``_extract_address_fields(raw, "filer")``), linked when the committee has no
  address yet.
* ``unified_committee_persons`` for treasurer / assistant-treasurer / chair, emitted
  ONLY when that officer's name (first / last / org) is present (mirrors
  ``_upsert_officer``).
* the officer ``unified_persons`` (``ORGANIZATION`` when org present else
  ``INDIVIDUAL`` — the ORM's ``_find_or_create_person`` rule, no placeholder path),
  and officer ``unified_entities`` keyed by ``(entity_type, normalized_name,
  state)`` — the SAME key contributor / payee entities use (via
  ``collapse_org_person_key``) so an officer who is also a contributor collapses to
  one entity / person.

**Post-#48 reconciliation.** The individual-person dedup key is now
``(lower(first), lower(last), state, dedup_addr_key)``.  Officers carry NO address
in the FILER record (the ORM's ``_find_or_create_person`` never builds one for
them), so their ``dedup_addr_key`` is ``NULL`` and they key name-only — exactly
what the post-#48 ORM does.  This worker still threads a ``_pk_addr`` (always
``NULL`` here) through the person dedup key / id-map read-back and writes the
``dedup_addr_key`` column, so the key matches the other families and the
``uix_persons_name_state`` partial index that now includes ``dedup_addr_key``.

Priority 0 — runs FIRST (before reports=1 and every committee / transaction /
report family) so the authoritative committee exists before anything references
it. The non-FILER committee writers use ``ON CONFLICT DO NOTHING`` so the FILER
row is never clobbered by an incidental transaction ``filerName``.

Officer person / entity / committee_person rows reference the committee and each
other by surrogate id; those FKs are filled by reading the just-written tables'
ids by natural key (SQLAlchemy core ``select``) and Polars-joining the id-maps
back onto the frames — the same read-back pattern ``detail_children`` uses.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy import MetaData, Table, select

from app.core.ingest_vectorized import common, id_maps
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.models import (
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedCommitteePerson,
    UnifiedEntity,
    UnifiedPerson,
)
from app.logger import Logger

_logger = Logger(__name__)


# Officer role prefixes -> CommitteeRole enum NAME (Postgres stores the member name).
# Order matters: it is the ORM's iteration order in build_filer_committee.
_OFFICER_ROLES: tuple[tuple[str, str], ...] = (
    ("treas", "TREASURER"),
    ("assttreas", "ASSISTANT_TREASURER"),
    ("chair", "CHAIR"),
)

# Source columns the worker reads. Padding-in missing ones keeps the transforms
# total regardless of which columns a given filers file happens to carry.
_FILER_COLS: tuple[str, ...] = (
    "recordType",
    "filerIdent",
    "filerTypeCd",
    "filerName",
    "committeeStatusCd",
    # committee address (filer prefix) — only these 4 feed _find_or_create_address.
    "filerStreetAddr1",
    "filerStreetCity",
    "filerStreetStateCd",
    "filerStreetPostalCode",
)


def _officer_cols() -> tuple[str, ...]:
    cols: list[str] = []
    for prefix, _role in _OFFICER_ROLES:
        cols.extend(
            (
                f"{prefix}NameFirst",
                f"{prefix}NameLast",
                f"{prefix}NameSuffixCd",
                f"{prefix}NameOrganization",
            )
        )
    return tuple(cols)


# ---------------------------------------------------------------------------
# Frame helpers
# ---------------------------------------------------------------------------


def _read(files: list[Path]) -> pl.DataFrame | None:
    frames = [pl.read_parquet(p) for p in files]
    if not frames:
        return None
    return frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")


def _ensure_cols(df: pl.DataFrame, names: Iterable[str]) -> pl.DataFrame:
    missing = [pl.lit(None, dtype=pl.Utf8).alias(n) for n in names if n not in df.columns]
    return df.with_columns(missing) if missing else df


def _cs(col: str) -> pl.Expr:
    return common.clean_str(col)


def _addr_key_cols() -> list[pl.Expr]:
    """4-field lower-cased natural address key (matches uix_addresses_* + the other
    families' _address_id_map keys)."""
    return [
        pl.col("street_1").cast(pl.Utf8).str.to_lowercase().alias("_k_s1"),
        pl.col("city").cast(pl.Utf8).str.to_lowercase().alias("_k_city"),
        pl.col("state").cast(pl.Utf8).str.to_lowercase().alias("_k_state"),
        pl.col("zip_code").alias("_k_zip"),
    ]


def _officer_full_name(first: pl.Expr, last: pl.Expr, suffix: pl.Expr, org: pl.Expr) -> pl.Expr:
    """PersonName.full_name (no middle for officer columns): org if present, else
    first/last/suffix joined by single spaces (blanks skipped)."""
    joined = pl.concat_str([first, last, suffix], separator=" ", ignore_nulls=True)
    joined = pl.when(joined.str.len_chars() > 0).then(joined).otherwise(pl.lit(""))
    return pl.when(org.is_not_null()).then(org).otherwise(joined)


# ---------------------------------------------------------------------------
# Per-officer extraction
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _OfficerFrame:
    """One officer role's per-row frame, keyed back to its committee filer_id."""

    role: str
    frame: pl.DataFrame  # cols: filer_id, first_name, last_name, suffix, organization,
    #     person_type, _pk_org, _pk_fn, _pk_ln, _pk_addr, _full_name


def _officer_frame(df: pl.DataFrame, prefix: str, role: str) -> _OfficerFrame:
    """Build the per-row officer frame for *role*, keeping only rows whose officer
    name (first/last/org) is present (mirrors ``_upsert_officer``'s emit rule)."""
    first = _cs(f"{prefix}NameFirst")
    last = _cs(f"{prefix}NameLast")
    suffix = _cs(f"{prefix}NameSuffixCd")
    org = _cs(f"{prefix}NameOrganization")
    full_name = _officer_full_name(first, last, suffix, org)
    # person_type: ORGANIZATION if org else INDIVIDUAL (the ORM _find_or_create_person
    # rule — no placeholder/UNKNOWN path for officers).
    person_type = (
        pl.when(org.is_not_null()).then(pl.lit("ORGANIZATION")).otherwise(pl.lit("INDIVIDUAL"))
    )
    out = (
        df.with_columns(
            _cs("filerIdent").alias("filer_id"),
            first.alias("first_name"),
            last.alias("last_name"),
            suffix.alias("suffix"),
            org.alias("organization"),
            person_type.alias("person_type"),
            org.str.to_lowercase().alias("_pk_org"),
            first.str.to_lowercase().alias("_pk_fn"),
            last.str.to_lowercase().alias("_pk_ln"),
            # Officers have no address in the FILER record — the post-#48 individual
            # dedup key still carries _pk_addr, here always NULL (name-only key, exactly
            # what the ORM's _find_or_create_person produces for officers).
            pl.lit(None, dtype=pl.Utf8).alias("_pk_addr"),
            full_name.alias("_full_name"),
        )
        .filter(pl.col("filer_id").is_not_null())
        # Emit only when the officer name produces a person (full_name non-empty).
        .filter(pl.col("_full_name").str.len_chars() > 0)
    )
    out = common.collapse_org_person_key(out)
    return _OfficerFrame(
        role=role,
        frame=out.select(
            "filer_id",
            "first_name",
            "last_name",
            "suffix",
            "organization",
            "person_type",
            "_pk_org",
            "_pk_fn",
            "_pk_ln",
            "_pk_addr",
            "_full_name",
        ),
    )


def _all_officers(df: pl.DataFrame) -> list[_OfficerFrame]:
    return [
        of
        for prefix, role in _OFFICER_ROLES
        if (of := _officer_frame(df, prefix, role)).frame.height > 0
    ]


# ---------------------------------------------------------------------------
# Id-map reads (natural key -> surrogate id)
# ---------------------------------------------------------------------------


def _reflect(engine: Any, name: str) -> Table:
    return Table(name, MetaData(), autoload_with=engine)


def _lower_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s.lower() if s else None


def _person_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{_pk_org, _pk_fn, _pk_ln, _pk_addr} -> person id (lower-cased name keys;
    org-persons keyed on lower(org) ALONE via collapse_org_person_key — matches
    uix_persons_org_state; individuals split by dedup_addr_key per the post-#48
    uix_persons_name_state). Officers have no address, so _pk_addr is NULL — but the
    key shape MUST match the other families' read-back exactly."""
    tbl = _reflect(engine, "unified_persons")
    stmt = select(
        tbl.c.id,
        tbl.c.first_name,
        tbl.c.last_name,
        tbl.c.organization,
        tbl.c.dedup_addr_key,
    ).where(tbl.c.state_id == state_id)
    with engine.connect() as conn:
        rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
    frame = pl.DataFrame(
        {
            "person_id": [r["id"] for r in rows],
            "_pk_org": [_lower_or_none(r["organization"]) for r in rows],
            "_pk_fn": [_lower_or_none(r["first_name"]) for r in rows],
            "_pk_ln": [_lower_or_none(r["last_name"]) for r in rows],
            "_pk_addr": [r["dedup_addr_key"] for r in rows],
        },
        schema={
            "person_id": pl.Int64,
            "_pk_org": pl.Utf8,
            "_pk_fn": pl.Utf8,
            "_pk_ln": pl.Utf8,
            "_pk_addr": pl.Utf8,
        },
    )
    return common.collapse_org_person_key(frame)


def _entity_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{entity_type, normalized_name} -> entity id for this state."""
    tbl = _reflect(engine, "unified_entities")
    stmt = select(tbl.c.id, tbl.c.entity_type, tbl.c.normalized_name).where(
        tbl.c.state_id == state_id
    )
    with engine.connect() as conn:
        rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
    return pl.DataFrame(
        {
            "entity_id": [r["id"] for r in rows],
            "entity_type": [_enum_name(r["entity_type"]) for r in rows],
            "normalized_name": [r["normalized_name"] for r in rows],
        },
        schema={"entity_id": pl.Int64, "entity_type": pl.Utf8, "normalized_name": pl.Utf8},
    )


def _address_id_map(engine: Any) -> pl.DataFrame:
    """4-field lower-cased address key -> address surrogate id."""
    tbl = _reflect(engine, "unified_addresses")
    stmt = select(tbl.c.id, tbl.c.street_1, tbl.c.city, tbl.c.state, tbl.c.zip_code)
    with engine.connect() as conn:
        rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
    return pl.DataFrame(
        {
            "address_id": [r["id"] for r in rows],
            "_k_s1": [_lower_or_none(r["street_1"]) for r in rows],
            "_k_city": [_lower_or_none(r["city"]) for r in rows],
            "_k_state": [_lower_or_none(r["state"]) for r in rows],
            "_k_zip": [r["zip_code"] for r in rows],
        },
        schema={
            "address_id": pl.Int64,
            "_k_s1": pl.Utf8,
            "_k_city": pl.Utf8,
            "_k_state": pl.Utf8,
            "_k_zip": pl.Utf8,
        },
    )


def _enum_name(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "name", str(value))


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class FilerWorker:
    """FILER: authoritative committees (+ address) and their officers."""

    record_types = frozenset({"FILER"})
    # Lower than every committee/transaction/report family (reports=1, dims=9, ...),
    # so committees + officers exist before anything references them.
    priority = 0

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        df = _read(files_by_type.get("FILER", []))
        if df is None or df.height == 0:
            return {"loaded": 0}
        df = _ensure_cols(df, _FILER_COLS + _officer_cols())

        counts: dict[str, int] = {}
        counts["committees"] = self._write_committees(df, ctx)
        n_persons, n_entities, n_cps = self._write_officers(df, ctx)
        counts["persons"] = n_persons
        counts["entities"] = n_entities
        counts["committee_persons"] = n_cps

        loaded = sum(counts.values())
        _logger.info(f"[vectorized.filer] loaded {loaded} rows: {counts}")
        return {"loaded": loaded, **counts}

    # ---- committees (+ address) -----------------------------------------

    @staticmethod
    def _collapse_committee_rows(rows: pl.DataFrame) -> pl.DataFrame:
        """Collapse multiple FILER rows for one filer_id into a single committee row,
        mirroring how the ORM mutates an existing committee on each successive FILER row:

        * ``name``           -> last NON-NULL filerName  (``new or old`` applied per row)
        * ``committee_type`` -> last NON-NULL filerTypeCd
        * ``filer_status``   -> the LAST row's committeeStatusCd, even when null
          (the ORM assigns it unconditionally: ``committee.filer_status = _s(...)``)
        * address            -> from the FIRST row carrying an address anchor (the ORM
          sets ``committee.address`` only when ``not committee.address_id``)
        """
        rows = rows.with_row_index("_ord")
        anchor = (
            pl.col("street_1").is_not_null()
            | pl.col("city").is_not_null()
            | pl.col("state").is_not_null()
            | pl.col("zip_code").is_not_null()
        )
        rows = rows.with_columns(
            pl.when(anchor).then(pl.col("_ord")).otherwise(None).alias("_addr_ord")
        )

        def _last_non_null(col: str) -> pl.Expr:
            # value of *col* at the row with the greatest _ord among non-null *col*.
            return (
                pl.col(col)
                .filter(pl.col(col).is_not_null())
                .sort_by(pl.col("_ord").filter(pl.col(col).is_not_null()))
                .last()
            )

        def _first_addr(col: str) -> pl.Expr:
            # value of *col* at the FIRST row with an address anchor. Rows without an
            # anchor (null _addr_ord) sort LAST so an anchored row always wins when one
            # exists (mirrors the ORM setting the address from the first anchored row).
            return pl.col(col).sort_by(pl.col("_addr_ord"), nulls_last=True).first()

        return (
            rows.group_by("filer_id", maintain_order=True)
            .agg(
                _last_non_null("name").alias("name"),
                _last_non_null("committee_type").alias("committee_type"),
                # filer_status: last ROW's value (sorted by _ord), even if null.
                pl.col("filer_status").sort_by(pl.col("_ord")).last().alias("filer_status"),
                _first_addr("street_1").alias("street_1"),
                _first_addr("city").alias("city"),
                _first_addr("state").alias("state"),
                _first_addr("zip_code").alias("zip_code"),
                pl.col("_ord").min().alias("_first_ord"),
            )
            .sort("_first_ord")
            .drop("_first_ord")
        )

    def _write_committees(self, df: pl.DataFrame, ctx: FamilyContext) -> int:
        rows = df.select(
            _cs("filerIdent").alias("filer_id"),
            _cs("filerName").alias("name"),
            _cs("filerTypeCd").alias("committee_type"),
            _cs("committeeStatusCd").alias("filer_status"),
            # Committee address: filer-prefix street/city/state/zip ONLY (mirrors
            # _extract_address_fields(raw, "filer") -> _find_or_create_address, which
            # builds UnifiedAddress with just these 4 fields).
            _cs("filerStreetAddr1").alias("street_1"),
            _cs("filerStreetCity").alias("city"),
            common.upper_str("filerStreetStateCd").alias("state"),
            _cs("filerStreetPostalCode").alias("zip_code"),
        ).filter(pl.col("filer_id").is_not_null())
        if rows.height == 0:
            return 0
        comm = self._collapse_committee_rows(rows)

        # --- Address: create the committee addresses first, then resolve ids. ---
        # The ORM links a committee address only when the 4-field address has an
        # anchor (street_1/city/state/zip non-null). street_2/country/county are NOT
        # populated by _find_or_create_address, so they stay null here too.
        has_addr = (
            pl.col("street_1").is_not_null()
            | pl.col("city").is_not_null()
            | pl.col("state").is_not_null()
            | pl.col("zip_code").is_not_null()
        )
        existing_addr = id_maps.address_key_frame(ctx.engine)
        addr_candidates = comm.filter(has_addr).select("street_1", "city", "state", "zip_code")
        addr_new = common.filter_new_rows(
            addr_candidates,
            existing_addr,
            key_cols=["street_1", "city", "state", "zip_code"],
            normalize_lower=["street_1", "city", "state"],
        )
        addr_out = addr_new.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("street_2"),
            pl.lit(None, dtype=pl.Utf8).alias("country"),
            pl.lit(None, dtype=pl.Utf8).alias("county"),
        )
        n_addr = common.write_frame(ctx.session, UnifiedAddress, addr_out, conflict_cols=None)
        _logger.info(f"[filer._write_committees] addresses written={n_addr}")

        # Resolve each committee's address_id via the 4-field key (null when no anchor).
        addr_map = _address_id_map(ctx.engine)
        comm = comm.with_columns(_addr_key_cols()).join(
            addr_map, on=["_k_s1", "_k_city", "_k_state", "_k_zip"], how="left", join_nulls=True
        )
        comm = comm.with_columns(
            pl.when(has_addr).then(pl.col("address_id")).otherwise(None).alias("address_id")
        )

        comm_out = comm.with_columns(pl.lit(ctx.state_id).alias("state_id")).select(
            "filer_id", "name", "committee_type", "filer_status", "address_id", "state_id"
        )
        # FILER is the authoritative source and runs first; on conflict KEEP the existing
        # row (DO NOTHING) — same first-occurrence-wins as the ORM find-or-create.
        return common.write_frame(
            ctx.session, UnifiedCommittee, comm_out, conflict_cols=["filer_id"], update_cols=[]
        )

    # ---- officers (persons + entities + committee_persons) --------------

    def _write_officers(self, df: pl.DataFrame, ctx: FamilyContext) -> tuple[int, int, int]:
        officers = _all_officers(df)
        if not officers:
            return 0, 0, 0

        # All officer parties across roles, in role order (treas, assttreas, chair),
        # for the shared person/entity dedup. A stable _sort_key gives first-occurrence
        # determinism across roles (mirrors the ORM's per-role iteration order).
        parts: list[pl.DataFrame] = []
        offset = 0
        for of in officers:
            parts.append(
                of.frame.with_columns(
                    (pl.int_range(0, pl.len(), dtype=pl.Int64) + offset).alias("_sort_key")
                )
            )
            offset += 10_000_000
        parties = pl.concat(parts, how="diagonal_relaxed").sort("_sort_key")

        n_persons = self._write_officer_persons(parties, ctx)
        n_entities = self._write_officer_entities(parties, ctx)
        n_cps = self._write_committee_persons(officers, ctx)
        return n_persons, n_entities, n_cps

    def _write_officer_persons(self, parties: pl.DataFrame, ctx: FamilyContext) -> int:
        """INSERT officer persons not already present (shared person dim; FILER first).

        Dedup / anti-join on the post-#48 address-inclusive key (_pk_addr is NULL for
        officers, so individuals key name-only) — the SAME key the other families use."""
        existing = id_maps.person_key_frame(ctx.engine, ctx.state_id)
        new = parties.unique(
            subset=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"], keep="first", maintain_order=True
        ).join(
            existing.select("_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"),
            on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
            how="anti",
            join_nulls=True,
        )
        out = new.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("middle_name"),
            pl.lit(None, dtype=pl.Utf8).alias("employer"),
            pl.lit(None, dtype=pl.Utf8).alias("occupation"),
            pl.lit(None, dtype=pl.Utf8).alias("job_title"),
            # Officers carry no address -> dedup_addr_key NULL (name-only key), matching
            # the post-#48 ORM _find_or_create_person path for officers.
            pl.col("_pk_addr").alias("dedup_addr_key"),
            pl.lit(None, dtype=pl.Int64).alias("address_id"),
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
        n_persons = common.write_frame(ctx.session, UnifiedPerson, out, conflict_cols=None)
        _logger.info(f"[filer._write_officer_persons] persons written={n_persons}")
        return n_persons

    def _write_officer_entities(self, parties: pl.DataFrame, ctx: FamilyContext) -> int:
        """INSERT officer entities not already present, keyed (entity_type,
        normalized_name) — the SAME key contributor/payee entities use. person_id /
        address_id are left NULL (assigned later by finalize_entity_representatives)."""
        entity_type = (
            pl.when(pl.col("organization").is_not_null())
            .then(pl.lit("ORGANIZATION"))
            .otherwise(pl.lit("PERSON"))
        )
        name = (
            pl.when(pl.col("organization").is_not_null())
            .then(pl.col("organization"))
            .otherwise(pl.col("_full_name"))
        )
        ents = (
            parties.with_columns(
                entity_type.alias("entity_type"),
                name.alias("name"),
            )
            .with_columns(
                common.normalize_entity_name_expr(pl.col("name")).alias("normalized_name")
            )
            .sort("_sort_key")
            .unique(subset=["entity_type", "normalized_name"], keep="first", maintain_order=True)
        )
        existing = _entity_id_map(ctx.engine, ctx.state_id)
        new = ents.join(
            existing.select("entity_type", "normalized_name"),
            on=["entity_type", "normalized_name"],
            how="anti",
        )
        out = new.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("committee_id"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
            pl.lit(None, dtype=pl.Int64).alias("person_id"),
            pl.lit(None, dtype=pl.Int64).alias("address_id"),
            pl.lit(ctx.state_id).alias("state_id"),
        ).select(
            "entity_type",
            "name",
            "normalized_name",
            "committee_id",
            "notes",
            "person_id",
            "address_id",
            "state_id",
        )
        return common.write_frame(
            ctx.session,
            UnifiedEntity,
            out,
            conflict_cols=["entity_type", "normalized_name", "state_id"],
            update_cols=[],
            conflict_where="state_id IS NOT NULL",
        )

    def _write_committee_persons(self, officers: list[_OfficerFrame], ctx: FamilyContext) -> int:
        """One committee_person per (committee, officer person, role). Resolves
        person_id + entity_id by natural key after persons/entities are written."""
        person_map = _person_id_map(ctx.engine, ctx.state_id)
        entity_map = _entity_id_map(ctx.engine, ctx.state_id)

        parts: list[pl.DataFrame] = []
        for of in officers:
            f = of.frame.with_columns(
                pl.lit(of.role).alias("role"),
                # Officer entity key (matches _write_officer_entities).
                pl.when(pl.col("organization").is_not_null())
                .then(pl.lit("ORGANIZATION"))
                .otherwise(pl.lit("PERSON"))
                .alias("entity_type"),
            )
            f = f.with_columns(
                common.normalize_entity_name_expr(
                    pl.when(pl.col("organization").is_not_null())
                    .then(pl.col("organization"))
                    .otherwise(pl.col("_full_name"))
                ).alias("normalized_name")
            )
            f = f.join(
                person_map,
                on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
                how="left",
                join_nulls=True,
            )
            f = f.join(entity_map, on=["entity_type", "normalized_name"], how="left")
            parts.append(f.select("filer_id", "role", "person_id", "entity_id"))

        cps = pl.concat(parts, how="diagonal_relaxed")
        # A person resolves to exactly one row; drop rows that failed to resolve (the
        # ORM skips an officer whose person could not be built/found).
        cps = cps.filter(pl.col("person_id").is_not_null())
        # Dedup (committee, person, role) — the ORM's existing-link guard.
        cps = cps.unique(
            subset=["filer_id", "person_id", "role"], keep="first", maintain_order=True
        )
        if cps.height == 0:
            return 0
        out = cps.with_columns(
            pl.col("filer_id").alias("committee_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            pl.lit(True).alias("is_active"),
            pl.lit(None, dtype=pl.Date).alias("start_date"),
            pl.lit(None, dtype=pl.Date).alias("end_date"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
            pl.lit(None, dtype=pl.Utf8).alias("last_modified_by"),
            pl.lit(None, dtype=pl.Utf8).alias("change_reason"),
        ).select(
            "committee_id",
            "person_id",
            "entity_id",
            "state_id",
            "role",
            "start_date",
            "end_date",
            "is_active",
            "notes",
            "last_modified_by",
            "change_reason",
        )
        return common.write_frame(
            ctx.session,
            UnifiedCommitteePerson,
            out,
            conflict_cols=["committee_id", "person_id", "role"],
            update_cols=[],
        )


register(FilerWorker())
