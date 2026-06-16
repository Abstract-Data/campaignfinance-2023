"""Vectorized detail_children family: LOAN, DEBT, CRED, TRVL, ASSET, PLDG.

Reproduces — columnar (pure Polars, no ``map_elements`` / ``.apply``) — the ORM
loader's full per-row pipeline for these six TEC record types:

* the dim rows each row implies (``unified_committees``, ``unified_persons``,
  ``unified_entities``, ``unified_addresses``) via the role-prefixed name/address
  columns each type uses (lender / payor / traveller / pledger),
* the ``unified_transactions`` row (mirrors
  ``builders.UnifiedSQLModelBuilder.build_transaction`` +
  ``production_loader._finalize_transaction_for_persist``),
* the detail child row for each type
  (``unified_loans`` / ``unified_debts`` / ``unified_credits`` /
  ``unified_travel`` / ``unified_assets`` / ``unified_pledges``) mirroring the
  ``DETAIL_BUILDERS`` in ``app/core/processor.py`` (PLDG goes through
  ``pledges_ingest.build_pledge``),
* ``loan_guarantors`` for LOAN/DEBT via ``struct``/``explode`` (mirrors
  ``processor._build_guarantors``).

Detail/guarantor rows reference dim rows and the parent transaction by surrogate
id; those FKs are filled by reading the just-written tables' ids by **natural
key** (SQLAlchemy core ``select``, parameter-free reflection) and Polars-joining
the id-maps back onto the frames.

Field resolution order for ``transaction_id`` / ``amount`` / ``transaction_date``
matches ``_get_field_value`` over the Texas field library (see
``unified_field_library``), determined per record type from the columns actually
present in the TEC source (e.g. DEBT carries only ``loanInfoId`` and ``receivedDt``
so its date falls back to ``receivedDt`` and its amount is null).

Priority 11 runs AFTER the dim family (``flat_txns_dims`` priority 9) and the flat
txns family (priority 10); this family builds its OWN dims (the dim family only
covers RCPT/EXPN) before its transactions + details so id-joins resolve.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy import MetaData, Table, select

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.models import (
    LoanGuarantor,
    UnifiedAddress,
    UnifiedAsset,
    UnifiedCommittee,
    UnifiedCredit,
    UnifiedDebt,
    UnifiedEntity,
    UnifiedLoan,
    UnifiedPerson,
    UnifiedTransaction,
    UnifiedTravel,
)
from app.core.source_models.pledges import UnifiedPledge
from app.logger import Logger

_logger = Logger(__name__)

_PLACEHOLDER_NAMES_UPPER = frozenset(
    {"NON-ITEMIZED CONTRIBUTOR", "NON-ITEMIZED", "UNKNOWN", "ANONYMOUS"}
)


# ---------------------------------------------------------------------------
# Per-type configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TypeSpec:
    """Static description of how one TEC record type maps onto unified rows."""

    record_type: str
    transaction_type: str
    id_col: str  # source column resolved to transaction_id
    amount_col: str | None  # source column resolved to amount (None => null)
    date_col: str | None  # source column resolved to transaction_date
    date_fallback_received: bool  # fall back to receivedDt when date_col null/absent
    descr_col: str | None
    # Role-prefixed person columns ({prefix}NameFirst/Last/Organization/SuffixCd).
    name_first: str | None = None
    name_last: str | None = None
    name_org: str | None = None
    name_suffix: str | None = None
    # Role-prefixed address columns (None when the role has no mapped address).
    addr_city: str | None = None
    addr_state: str | None = None
    addr_zip: str | None = None
    addr_country: str | None = None
    addr_county: str | None = None
    # Priority for load order (mirrors production_loader._FILE_PRIORITY).
    priority: int = 50
    #: Extra source columns referenced (so they are nulled-in when absent).
    extra_cols: tuple[str, ...] = field(default_factory=tuple)


_GUARANTOR_COLS = (
    "guarantorPersentTypeCd",
    "guarantorNameOrganization",
    "guarantorNameLast",
    "guarantorNameSuffixCd",
    "guarantorNameFirst",
    "guarantorNamePrefixCd",
    "guarantorStreetCity",
    "guarantorStreetStateCd",
    "guarantorStreetCountyCd",
    "guarantorStreetCountryCd",
    "guarantorStreetPostalCode",
    "guarantorStreetRegion",
)


def _guarantor_source_cols() -> tuple[str, ...]:
    return tuple(f"{base}{i}" for i in range(1, 6) for base in _GUARANTOR_COLS)


_LOAN = TypeSpec(
    record_type="LOAN",
    transaction_type="LOAN",
    id_col="loanInfoId",
    amount_col="loanAmount",
    date_col="loanDt",
    date_fallback_received=True,
    descr_col="loanDescr",
    name_first="lenderNameFirst",
    name_last="lenderNameLast",
    name_org="lenderNameOrganization",
    name_suffix="lenderNameSuffixCd",
    addr_city="lenderStreetCity",
    addr_state="lenderStreetStateCd",
    addr_zip="lenderStreetPostalCode",
    addr_country="lenderStreetCountryCd",
    addr_county="lenderStreetCountyCd",
    priority=12,
    extra_cols=("interestRate", "maturityDt", "collateralDescr") + _guarantor_source_cols(),
)

_DEBT = TypeSpec(
    record_type="DEBT",
    transaction_type="DEBT",
    id_col="loanInfoId",
    amount_col=None,  # debts fixture has no loanAmount/debtAmount column
    date_col=None,
    date_fallback_received=True,
    descr_col=None,
    name_first="lenderNameFirst",
    name_last="lenderNameLast",
    name_org="lenderNameOrganization",
    name_suffix="lenderNameSuffixCd",
    addr_city="lenderStreetCity",
    addr_state="lenderStreetStateCd",
    addr_zip="lenderStreetPostalCode",
    addr_country="lenderStreetCountryCd",
    addr_county="lenderStreetCountyCd",
    priority=13,
    extra_cols=("loanGuaranteedFlag", "loanGuaranteeAmount") + _guarantor_source_cols(),
)

_PLDG = TypeSpec(
    record_type="PLDG",
    transaction_type="PLEDGE",
    id_col="pledgeInfoId",
    amount_col="pledgeAmount",
    date_col="pledgeDt",
    date_fallback_received=False,
    descr_col="pledgeDescr",
    name_first="pledgerNameFirst",
    name_last="pledgerNameLast",
    name_org="pledgerNameOrganization",
    name_suffix="pledgerNameSuffixCd",
    addr_city="pledgerStreetCity",
    addr_state="pledgerStreetStateCd",
    addr_zip="pledgerStreetPostalCode",
    addr_country="pledgerStreetCountryCd",
    addr_county="pledgerStreetCountyCd",
    priority=14,
)

_CRED = TypeSpec(
    record_type="CRED",
    transaction_type="CREDIT",
    id_col="creditInfoId",
    amount_col="creditAmount",
    date_col="creditDt",
    date_fallback_received=False,
    descr_col="creditDescr",
    name_first="payorNameFirst",
    name_last="payorNameLast",
    name_org="payorNameOrganization",
    name_suffix="payorNameSuffixCd",
    # payor has NO mapped address columns in the field library -> no address.
    priority=15,
)

_TRVL = TypeSpec(
    record_type="TRVL",
    transaction_type="TRAVEL",
    id_col="travelInfoId",
    amount_col=None,  # travel rows carry the value on parentAmount (amount fallback)
    date_col="parentDt",
    date_fallback_received=False,
    descr_col=None,
    name_first="travellerNameFirst",
    name_last="travellerNameLast",
    name_org="travellerNameOrganization",
    name_suffix="travellerNameSuffixCd",
    priority=16,
    extra_cols=(
        "parentType",
        "parentId",
        "parentAmount",
        "parentFullName",
        "transportationTypeCd",
        "transportationTypeDescr",
        "departureCity",
        "arrivalCity",
        "departureDt",
        "arrivalDt",
        "travelPurpose",
    ),
)

_ASSET = TypeSpec(
    record_type="ASSET",
    transaction_type="ASSET",
    id_col="assetInfoId",
    amount_col=None,
    date_col=None,
    date_fallback_received=True,
    descr_col="assetDescr",  # assetDescr -> description (0.9) and asset_descr (1.0)
    priority=17,
    extra_cols=("assetDescr",),
)

_SPECS: dict[str, TypeSpec] = {
    s.record_type: s for s in (_LOAN, _DEBT, _PLDG, _CRED, _TRVL, _ASSET)
}

# Common columns every TEC transaction file carries.
_BASE_COLS = (
    "recordType",
    "formTypeCd",
    "schedFormTypeCd",
    "reportInfoIdent",
    "receivedDt",
    "infoOnlyFlag",
    "filerIdent",
    "filerTypeCd",
    "filerName",
)


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


def _spec_cols(spec: TypeSpec) -> tuple[str, ...]:
    cols: list[str] = list(_BASE_COLS)
    for c in (
        spec.id_col,
        spec.amount_col,
        spec.date_col,
        spec.descr_col,
        spec.name_first,
        spec.name_last,
        spec.name_org,
        spec.name_suffix,
        spec.addr_city,
        spec.addr_state,
        spec.addr_zip,
        spec.addr_country,
        spec.addr_county,
    ):
        if c is not None:
            cols.append(c)
    cols.extend(spec.extra_cols)
    # De-dup, preserve order.
    seen: set[str] = set()
    out: list[str] = []
    for c in cols:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return tuple(out)


def _cs(col: str) -> pl.Expr:
    return common.clean_str(col)


def _nullify(e: pl.Expr) -> pl.Expr:
    s = e.cast(pl.Utf8).str.strip_chars()
    return pl.when(s.str.len_chars() > 0).then(s).otherwise(None)


def _opt_col(df: pl.DataFrame, col: str | None) -> pl.Expr:
    """clean_str of a possibly-absent column (null Utf8 when col is None)."""
    if col is None or col not in df.columns:
        return pl.lit(None, dtype=pl.Utf8)
    return _cs(col)


# ---------------------------------------------------------------------------
# Person / entity / address per-spec extraction
# ---------------------------------------------------------------------------


def _person_type_expr(last: pl.Expr, first: pl.Expr, org: pl.Expr) -> pl.Expr:
    """Mirror ORM build_person person_type priority order."""
    last_upper = last.cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    return (
        pl.when(last_upper.is_in(list(_PLACEHOLDER_NAMES_UPPER)))
        .then(pl.lit("UNKNOWN"))
        .when(org.is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .when(first.is_not_null() & last.is_not_null())
        .then(pl.lit("INDIVIDUAL"))
        .otherwise(pl.lit("UNKNOWN"))
    )


def _full_name(first: pl.Expr, last: pl.Expr, suffix: pl.Expr, org: pl.Expr) -> pl.Expr:
    """Mirror PersonName.full_name (no middle column for these types)."""
    joined = pl.concat_str(
        [first, last, suffix], separator=" ", ignore_nulls=True
    )
    joined = pl.when(joined.str.len_chars() > 0).then(joined).otherwise(pl.lit(""))
    return pl.when(org.is_not_null()).then(org).otherwise(joined)


def _spec_party_frame(df: pl.DataFrame, spec: TypeSpec, sort_offset: int) -> pl.DataFrame:
    """Per-row party (person + address) frame for *spec*, before global dedup.

    Columns:
        first_name, last_name, suffix, organization, person_type,
        a_street_1, a_street_2, a_city, a_state, a_zip, a_country, a_county,
        has_address, _pk_org, _pk_fn, _pk_ln, _pk_addr, _sort_key
    """
    first = _opt_col(df, spec.name_first)
    last = _opt_col(df, spec.name_last)
    org = _opt_col(df, spec.name_org)
    suffix = _opt_col(df, spec.name_suffix)
    city = _opt_col(df, spec.addr_city)
    state = (
        _opt_col(df, spec.addr_state).str.to_uppercase()
        if spec.addr_state
        else pl.lit(None, dtype=pl.Utf8)
    )
    zip_code = _opt_col(df, spec.addr_zip)
    country = _opt_col(df, spec.addr_country)
    county = _opt_col(df, spec.addr_county)

    row_id = (pl.int_range(0, pl.len(), dtype=pl.Int64) + sort_offset).alias("_sort_key")

    out = df.with_columns(
        [
            first.alias("first_name"),
            last.alias("last_name"),
            suffix.alias("suffix"),
            org.alias("organization"),
            _person_type_expr(last, first, org).alias("person_type"),
            pl.lit(None, dtype=pl.Utf8).alias("a_street_1"),
            pl.lit(None, dtype=pl.Utf8).alias("a_street_2"),
            city.alias("a_city"),
            state.alias("a_state"),
            zip_code.alias("a_zip"),
            country.alias("a_country"),
            county.alias("a_county"),
            org.str.to_lowercase().alias("_pk_org"),
            first.str.to_lowercase().alias("_pk_fn"),
            last.str.to_lowercase().alias("_pk_ln"),
            # Address dimension of the individual dedup key (these record types carry no
            # street_1). collapse_org_person_key nulls it for org-persons below.
            common.person_addr_key_expr(
                pl.lit(None, dtype=pl.Utf8), city, state, zip_code
            ).alias("_pk_addr"),
            row_id,
        ]
    )
    # Org-persons dedup on lower(org) ALONE (null fn/ln/addr) — matches uix_persons_org_state.
    out = common.collapse_org_person_key(out)
    # Keep only rows that produce a person (full_name non-empty).
    fn = _full_name(
        pl.col("first_name"), pl.col("last_name"), pl.col("suffix"), pl.col("organization")
    )
    out = out.filter(fn.str.len_chars() > 0)
    return out.select(
        "first_name",
        "last_name",
        "suffix",
        "organization",
        "person_type",
        "a_street_1",
        "a_street_2",
        "a_city",
        "a_state",
        "a_zip",
        "a_country",
        "a_county",
        "_pk_org",
        "_pk_fn",
        "_pk_ln",
        "_pk_addr",
        "_sort_key",
    )


def _address_has_anchor(df: pl.DataFrame) -> pl.Expr:
    return (
        pl.col("a_street_1").is_not_null()
        | pl.col("a_city").is_not_null()
        | pl.col("a_state").is_not_null()
        | pl.col("a_zip").is_not_null()
    )


def _addr_key_cols() -> list[pl.Expr]:
    return [
        pl.col("a_street_1").cast(pl.Utf8).str.to_lowercase().alias("_k_s1"),
        pl.col("a_city").cast(pl.Utf8).str.to_lowercase().alias("_k_city"),
        pl.col("a_state").cast(pl.Utf8).str.to_lowercase().alias("_k_state"),
        pl.col("a_zip").alias("_k_zip"),
    ]


# ---------------------------------------------------------------------------
# Id-map reads (natural key -> surrogate id)
# ---------------------------------------------------------------------------


def _reflect(engine: Any, name: str) -> Table:
    return Table(name, MetaData(), autoload_with=engine)


def _entity_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{entity_type, normalized_name} -> entity id for this state."""
    tbl = _reflect(engine, "unified_entities")
    stmt = select(tbl.c.id, tbl.c.entity_type, tbl.c.normalized_name).where(
        tbl.c.state_id == state_id
    )
    with engine.connect() as conn:
        rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
    if not rows:
        return pl.DataFrame(
            schema={"entity_id": pl.Int64, "entity_type": pl.Utf8, "normalized_name": pl.Utf8}
        )
    return pl.DataFrame(
        {
            "entity_id": [r["id"] for r in rows],
            "entity_type": [_enum_name(r["entity_type"]) for r in rows],
            "normalized_name": [r["normalized_name"] for r in rows],
        }
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


def _person_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{_pk_org, _pk_fn, _pk_ln, _pk_addr} -> person id for this state (lower-cased keys;
    org-persons keyed on lower(org) ALONE via collapse_org_person_key, matching
    uix_persons_org_state; individuals split by dedup_addr_key per uix_persons_name_state)."""
    tbl = _reflect(engine, "unified_persons")
    stmt = select(
        tbl.c.id, tbl.c.first_name, tbl.c.last_name, tbl.c.organization,
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


def _txn_id_map(engine: Any, state_id: int, record_types: list[str]) -> pl.DataFrame:
    """{transaction_id, transaction_type} -> txn surrogate id for this state.

    *record_types* are TEC record-type codes (e.g. ``CRED``); they are mapped to
    the corresponding transaction-type enum names (e.g. ``CREDIT``) before
    filtering, since ``unified_transactions.transaction_type`` stores the enum.
    """
    tbl = _reflect(engine, "unified_transactions")
    stmt = select(tbl.c.id, tbl.c.transaction_id, tbl.c.transaction_type).where(
        tbl.c.state_id == state_id
    )
    with engine.connect() as conn:
        rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
    want = {_SPECS[rt].transaction_type for rt in record_types if rt in _SPECS}
    keep = [r for r in rows if _enum_name(r["transaction_type"]) in want]
    return pl.DataFrame(
        {
            "txn_pk": [r["id"] for r in keep],
            "transaction_id": [
                None if r["transaction_id"] is None else str(r["transaction_id"])
                for r in keep
            ],
            "transaction_type": [_enum_name(r["transaction_type"]) for r in keep],
        },
        schema={"txn_pk": pl.Int64, "transaction_id": pl.Utf8, "transaction_type": pl.Utf8},
    )


def _committee_entity_map(engine: Any, state_id: int) -> dict[str, int]:
    """committee filer_id -> committee entity id."""
    tbl = _reflect(engine, "unified_entities")
    stmt = select(tbl.c.id, tbl.c.committee_id).where(
        tbl.c.state_id == state_id, tbl.c.committee_id.is_not(None)
    )
    with engine.connect() as conn:
        return {m["committee_id"]: m["id"] for m in conn.execute(stmt).mappings().all()}


def _loan_pk_map(engine: Any, table: str) -> dict[int, int]:
    """parent transaction_id (surrogate) -> detail surrogate id, for loan/debt."""
    tbl = _reflect(engine, table)
    stmt = select(tbl.c.id, tbl.c.transaction_id)
    with engine.connect() as conn:
        return {m["transaction_id"]: m["id"] for m in conn.execute(stmt).mappings().all()}


def _enum_name(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "name", str(value))


def _lower_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s.lower() if s else None


def _norm_name(value: pl.Expr) -> pl.Expr:
    """value_objects.normalize_entity_name as a column expression."""
    s = value.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    s = s.str.replace_all(r"[^a-z0-9]+", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return s.fill_null("")


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class DetailChildrenWorker:
    """LOAN/DEBT/CRED/TRVL/ASSET/PLDG: dims + transactions + detail children."""

    record_types = frozenset({"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"})
    priority = 11  # after flat_txns_dims (9) and flat_txns (10)

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        # Read & normalize every present type, in load-priority order. The ORIGINAL
        # source columns (before _ensure_cols pads missing ones) are tracked per type
        # so the transaction's raw_data provenance matches json.dumps(raw) exactly.
        frames: dict[str, pl.DataFrame] = {}
        self._orig_cols: dict[str, list[str]] = {}
        for rt in sorted(files_by_type, key=lambda r: _SPECS[r].priority):
            df = _read(files_by_type[rt])
            if df is None or df.height == 0:
                continue
            spec = _SPECS[rt]
            self._orig_cols[rt] = list(df.columns)
            df = _ensure_cols(df, _spec_cols(spec))
            frames[rt] = df

        if not frames:
            return {"loaded": 0}

        ordered = sorted(frames, key=lambda r: _SPECS[r].priority)

        # Omit-null address match lookup, built ONCE from addresses already in the DB (FILER +
        # flat_txns, all earlier priorities) so a street-less loan/debt/etc. party inherits a
        # fuller existing address's street — the ORM's _find_address_by_fields. Built before
        # any address this family writes, and reused by BOTH the dim layer (_write_dims) and
        # the detail->person link (_party_keys) so the two compute the SAME person key.
        # Per-run instance state, like self._orig_cols above.
        self._addr_lookup = common.full_address_lookup(ctx.engine)

        counts: dict[str, int] = {}

        # 1. Committees (shared natural-key dim).
        counts["committees"] = self._write_committees(frames, ordered, ctx)

        # 2. Addresses + persons + entities.
        counts["addresses"], counts["persons"], counts["entities"] = self._write_dims(
            frames, ordered, ctx
        )

        # 3. Transactions.
        counts["transactions"] = self._write_transactions(frames, ordered, ctx)

        # 4. Detail children (+ guarantors).
        counts.update(self._write_details(frames, ordered, ctx))

        loaded = sum(counts.values())
        _logger.info(f"[vectorized.detail_children] loaded {loaded} rows: {counts}")
        return {"loaded": loaded, **counts}

    # ---- committees -----------------------------------------------------

    def _write_committees(
        self, frames: dict[str, pl.DataFrame], ordered: list[str], ctx: FamilyContext
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

    # ---- dims (addresses, persons, entities) ----------------------------

    def _all_parties(
        self, frames: dict[str, pl.DataFrame], ordered: list[str]
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

    def _write_dims(
        self, frames: dict[str, pl.DataFrame], ordered: list[str], ctx: FamilyContext
    ) -> tuple[int, int, int]:
        parties = self._all_parties(frames, ordered)
        if parties.height == 0:
            return 0, 0, 0

        # Omit-null address match: a street-less party (these record types carry no source
        # street) inherits a fuller existing address's street, so its dedup_addr_key matches
        # the ORM's. a_city/a_state/a_zip are already the cleaned/cased dim columns. Then
        # recompute _pk_addr from the (possibly inherited) street; org-persons keep NULL.
        parties = common.add_resolved_street(
            parties, self._addr_lookup,
            city_col="a_city", state_col="a_state", zip_col="a_zip", out_col="a_street_1",
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
            subset=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"], keep="first",
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
                subset=["_k_s1", "_k_city", "_k_state", "_k_zip"], keep="first",
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
        n_persons = common.write_frame(
            ctx.session, UnifiedPerson, persons_out, conflict_cols=None
        )

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
                "entity_type", "name", "normalized_name", "committee_id", "notes",
                "person_id", "address_id", "_sort_key",
            )
        )

        comm_entities = self._committee_entity_frame(frames, ordered).with_columns(
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
            entities_out = entities.with_columns(
                pl.lit(ctx.state_id).alias("state_id")
            ).select(
                "entity_type", "name", "normalized_name", "committee_id", "notes",
                "person_id", "address_id", "state_id",
            )
            n_entities = common.write_frame(
                ctx.session, UnifiedEntity, entities_out, conflict_cols=None
            )
        else:
            n_entities = 0

        return n_addr, n_persons, n_entities

    def _committee_entity_frame(
        self, frames: dict[str, pl.DataFrame], ordered: list[str]
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
        ).select(
            "entity_type", "name", "normalized_name", "committee_id", "notes", "_sort_key"
        )

    # ---- transactions ---------------------------------------------------

    def _transaction_frame(self, df: pl.DataFrame, spec: TypeSpec, ctx: FamilyContext) -> pl.DataFrame:
        # raw_data provenance must match json.dumps(raw) over the ORIGINAL source
        # columns only — not the null-padded columns _ensure_cols added.
        orig_cols = self._orig_cols.get(spec.record_type, list(df.columns))

        txn_id = (
            pl.col(spec.id_col).cast(pl.Utf8)
            if spec.id_col in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        )

        # amount
        if spec.amount_col and spec.amount_col in df.columns:
            amount = common.builder_amount(spec.amount_col)
        else:
            amount = pl.lit(None, dtype=pl.Decimal(38, 4))
        if spec.record_type == "TRVL":
            amount = amount.fill_null(common.builder_amount("parentAmount"))

        # date
        if spec.date_col and spec.date_col in df.columns:
            date_expr = common.builder_date(spec.date_col)
        else:
            date_expr = pl.lit(None, dtype=pl.Date)
        if spec.date_fallback_received:
            date_expr = date_expr.fill_null(common.builder_date("receivedDt"))

        # description: build_transaction assigns the raw _get_field_value result
        # UNSTRIPPED (no clean_str).
        if spec.descr_col and spec.descr_col in df.columns:
            descr = pl.col(spec.descr_col).cast(pl.Utf8)
        else:
            descr = pl.lit(None, dtype=pl.Utf8)

        return df.with_columns(
            pl.lit(ctx.state_id).alias("state_id"),
            txn_id.alias("transaction_id"),
            amount.alias("amount"),
            date_expr.alias("transaction_date"),
            descr.alias("description"),
            pl.lit(spec.transaction_type).alias("transaction_type"),
            _cs("filerIdent").alias("committee_id"),
            _cs("reportInfoIdent").alias("report_ident"),
            # filed_date: filedDt mapped 1.0, receivedDt 0.9. These files carry only
            # receivedDt (filedDt is CVR1-only), so filed_date = builder_date(receivedDt).
            common.builder_date("receivedDt").alias("filed_date"),
            pl.lit(False).alias("amended"),
            pl.lit(None, dtype=pl.Utf8).alias("file_origin_id"),
            common.raw_json_expr(orig_cols, alias="raw_data"),
        ).select(
            "state_id",
            "transaction_id",
            "amount",
            "transaction_date",
            "description",
            "transaction_type",
            "committee_id",
            "report_ident",
            "filed_date",
            "amended",
            "file_origin_id",
            "raw_data",
        )

    def _write_transactions(
        self, frames: dict[str, pl.DataFrame], ordered: list[str], ctx: FamilyContext
    ) -> int:
        total = 0
        for rt in ordered:
            out = self._transaction_frame(frames[rt], _SPECS[rt], ctx)
            total += common.write_frame(
                ctx.session, UnifiedTransaction, out, conflict_cols=None
            )
        return total

    # ---- detail children ------------------------------------------------

    def _write_details(
        self, frames: dict[str, pl.DataFrame], ordered: list[str], ctx: FamilyContext
    ) -> dict[str, int]:
        engine = ctx.engine
        entity_map = _entity_id_map(engine, ctx.state_id)
        txn_map = _txn_id_map(engine, ctx.state_id, ordered)
        committee_entity = _committee_entity_map(engine, ctx.state_id)

        counts: dict[str, int] = {}
        for rt in ordered:
            spec = _SPECS[rt]
            df = frames[rt]
            if rt == "LOAN":
                counts["loans"] = self._build_loan(df, spec, ctx, entity_map, txn_map,
                                                    committee_entity)
            elif rt == "DEBT":
                counts["debts"] = self._build_debt(df, spec, ctx, entity_map, txn_map,
                                                   committee_entity)
            elif rt == "CRED":
                counts["credits"] = self._build_credit(df, spec, ctx, entity_map, txn_map,
                                                       committee_entity)
            elif rt == "TRVL":
                counts["travel"] = self._build_travel(df, spec, ctx, txn_map)
            elif rt == "ASSET":
                counts["assets"] = self._build_asset(df, spec, ctx, txn_map)
            elif rt == "PLDG":
                counts["pledges"] = self._build_pledge(df, spec, ctx, txn_map)

        # Guarantors depend on loan/debt surrogate ids (written above).
        counts["loan_guarantors"] = self._build_guarantors(frames, ordered, ctx)
        return counts

    def _party_keys(self, df: pl.DataFrame, spec: TypeSpec) -> pl.DataFrame:
        """Attach the per-row party dedup keys + parent transaction id to *df*."""
        first = _opt_col(df, spec.name_first)
        last = _opt_col(df, spec.name_last)
        org = _opt_col(df, spec.name_org)
        city = _opt_col(df, spec.addr_city)
        state = (
            _opt_col(df, spec.addr_state).str.to_uppercase()
            if spec.addr_state
            else pl.lit(None, dtype=pl.Utf8)
        )
        zip_code = _opt_col(df, spec.addr_zip)
        txn_id = (
            pl.col(spec.id_col).cast(pl.Utf8)
            if spec.id_col in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        )
        # Resolve the inherited street via the SAME omit-null match the dim layer used
        # (self._addr_lookup, built once in run()), so this detail->person key matches the
        # enriched person stored by _write_dims. Materialize city/state/zip as columns for
        # add_resolved_street, then key on the resolved street.
        keyed = df.with_columns(
            city.alias("_rc_city"), state.alias("_rc_state"), zip_code.alias("_rc_zip"),
        )
        keyed = common.add_resolved_street(
            keyed, self._addr_lookup,
            city_col="_rc_city", state_col="_rc_state", zip_col="_rc_zip", out_col="_res_street",
        )
        return common.collapse_org_person_key(
            keyed.with_columns(
                org.str.to_lowercase().alias("_pk_org"),
                first.str.to_lowercase().alias("_pk_fn"),
                last.str.to_lowercase().alias("_pk_ln"),
                # Address dimension of the individual key — uses the inherited street so it
                # matches the dim-layer person. collapse_org_person_key nulls it for orgs.
                common.person_addr_key_expr(
                    pl.col("_res_street"), pl.col("_rc_city"), pl.col("_rc_state"),
                    pl.col("_rc_zip"),
                ).alias("_pk_addr"),
                _full_name(first, last, _opt_col(df, spec.name_suffix), org).alias("_full_name"),
                txn_id.alias("_txn_id"),
            )
        ).drop("_rc_city", "_rc_state", "_rc_zip", "_res_street")

    def _join_party_entity(self, keyed: pl.DataFrame, person_map: pl.DataFrame,
                           entity_map: pl.DataFrame) -> pl.DataFrame:
        """Resolve party -> person id -> entity id (PERSON or ORGANIZATION entity)."""
        joined = keyed.join(
            person_map, on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"], how="left",
            join_nulls=True,
        )
        # The party's entity normalized_name == normalize(org) for orgs else
        # normalize(full_name); entity_type ORGANIZATION vs PERSON.
        joined = joined.with_columns(
            pl.when(pl.col("_pk_org").is_not_null())
            .then(pl.lit("ORGANIZATION"))
            .otherwise(pl.lit("PERSON"))
            .alias("_party_etype"),
            _norm_name(pl.col("_full_name")).alias("_party_nname"),
        )
        emap = entity_map.rename(
            {"entity_id": "_party_entity_id", "entity_type": "_party_etype",
             "normalized_name": "_party_nname"}
        )
        return joined.join(emap, on=["_party_etype", "_party_nname"], how="left")

    def _join_txn(self, df: pl.DataFrame, txn_map: pl.DataFrame, ttype: str) -> pl.DataFrame:
        tmap = txn_map.filter(pl.col("transaction_type") == ttype).select(
            pl.col("transaction_id").alias("_txn_id"), pl.col("txn_pk")
        )
        return df.join(tmap, on="_txn_id", how="left")

    def _committee_entity_expr(self, df: pl.DataFrame, committee_entity: dict[str, int]) -> pl.DataFrame:
        filer = _cs("filerIdent")
        mapping = pl.DataFrame(
            {
                "_filer": list(committee_entity.keys()),
                "_committee_entity_id": list(committee_entity.values()),
            },
            schema={"_filer": pl.Utf8, "_committee_entity_id": pl.Int64},
        )
        return df.with_columns(filer.alias("_filer")).join(mapping, on="_filer", how="left")

    def _build_loan(self, df, spec, ctx, entity_map, txn_map, committee_entity) -> int:
        keyed = self._party_keys(df, spec)
        keyed = self._join_party_entity(keyed, _person_id_map(ctx.engine, ctx.state_id),
                                        entity_map)
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        keyed = self._committee_entity_expr(keyed, committee_entity)
        # ORM skips loans with no lender entity or no borrower (committee) entity.
        out = keyed.filter(
            pl.col("_party_entity_id").is_not_null()
            & pl.col("_committee_entity_id").is_not_null()
            & pl.col("txn_pk").is_not_null()
        ).with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.col("_party_entity_id").alias("lender_entity_id"),
            pl.col("_committee_entity_id").alias("borrower_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            common.builder_amount(spec.amount_col).alias("amount")
            if spec.amount_col in df.columns
            else pl.lit(None, dtype=pl.Decimal(38, 4)).alias("amount"),
            self._loan_date_expr(spec, df).alias("loan_date"),
            common.builder_date("maturityDt").alias("due_date"),
            common.builder_amount("interestRate").alias("interest_rate"),
            _get_unstripped(df, "collateralDescr").alias("collateral"),
        ).select(
            "transaction_id", "lender_entity_id", "borrower_entity_id", "state_id",
            "amount", "loan_date", "due_date", "interest_rate", "collateral",
        )
        return common.write_frame(ctx.session, UnifiedLoan, out, conflict_cols=None)

    def _loan_date_expr(self, spec: TypeSpec, df: pl.DataFrame) -> pl.Expr:
        if spec.date_col and spec.date_col in df.columns:
            d = common.builder_date(spec.date_col)
        else:
            d = pl.lit(None, dtype=pl.Date)
        if spec.date_fallback_received:
            d = d.fill_null(common.builder_date("receivedDt"))
        return d

    def _build_debt(self, df, spec, ctx, entity_map, txn_map, committee_entity) -> int:
        keyed = self._party_keys(df, spec)
        keyed = self._join_party_entity(keyed, _person_id_map(ctx.engine, ctx.state_id),
                                        entity_map)
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        keyed = self._committee_entity_expr(keyed, committee_entity)
        # ORM skips debts with no creditor entity. debtor falls back to committee
        # entity, else to creditor entity.
        debtor = pl.coalesce([pl.col("_committee_entity_id"), pl.col("_party_entity_id")])
        # amount is None for debts (no amount column); original_amount = parse or amount.
        out = keyed.filter(
            pl.col("_party_entity_id").is_not_null() & pl.col("txn_pk").is_not_null()
        ).with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.col("_party_entity_id").alias("creditor_entity_id"),
            debtor.alias("debtor_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("amount"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("original_amount"),
            self._loan_date_expr(spec, df).alias("debt_date"),
            pl.lit(None, dtype=pl.Date).alias("due_date"),
            pl.lit(None, dtype=pl.Utf8).alias("description"),
            common.bool_expr("loanGuaranteedFlag").alias("is_guaranteed"),
            pl.lit(None, dtype=pl.Utf8).alias("guarantor_name"),
            common.builder_amount("loanGuaranteeAmount").alias("guarantee_amount")
            if "loanGuaranteeAmount" in df.columns
            else pl.lit(None, dtype=pl.Decimal(38, 4)).alias("guarantee_amount"),
            pl.lit(False).alias("is_paid"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("payment_amount"),
            pl.lit(None, dtype=pl.Date).alias("payment_date"),
        ).select(
            "transaction_id", "creditor_entity_id", "debtor_entity_id", "state_id",
            "amount", "original_amount", "debt_date", "due_date", "description",
            "is_guaranteed", "guarantor_name", "guarantee_amount", "is_paid",
            "payment_amount", "payment_date",
        )
        return common.write_frame(ctx.session, UnifiedDebt, out, conflict_cols=None)

    def _build_credit(self, df, spec, ctx, entity_map, txn_map, committee_entity) -> int:
        keyed = self._party_keys(df, spec)
        keyed = self._join_party_entity(keyed, _person_id_map(ctx.engine, ctx.state_id),
                                        entity_map)
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        keyed = self._committee_entity_expr(keyed, committee_entity)
        recipient = pl.coalesce([pl.col("_committee_entity_id"), pl.col("_party_entity_id")])
        out = keyed.filter(
            pl.col("_party_entity_id").is_not_null() & pl.col("txn_pk").is_not_null()
        ).with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.col("_party_entity_id").alias("payor_entity_id"),
            recipient.alias("recipient_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            common.builder_amount(spec.amount_col).alias("amount"),
            common.builder_date(spec.date_col).alias("credit_date"),
            pl.lit(None, dtype=pl.Utf8).alias("credit_type"),
            _get_unstripped(df, spec.descr_col).alias("description"),
            pl.lit(None, dtype=pl.Utf8).alias("related_transaction_id"),
        ).select(
            "transaction_id", "payor_entity_id", "recipient_entity_id", "state_id",
            "amount", "credit_date", "credit_type", "description", "related_transaction_id",
        )
        return common.write_frame(ctx.session, UnifiedCredit, out, conflict_cols=None)

    def _build_travel(self, df, spec, ctx, txn_map) -> int:
        keyed = self._party_keys(df, spec)
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        # traveler_person_id is ALWAYS None: TRVL maps the traveller into the PAYEE
        # role (RECORD_TYPE_ROLE_MAP), but _build_travel_detail reads ctx["contributor"]
        # — which is None for TRVL — so traveler is never linked.
        descr = (
            pl.col(spec.descr_col).cast(pl.Utf8)
            if spec.descr_col and spec.descr_col in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        )
        # traveler_name <- traveler_name (no source col) or parent_full_name.
        traveler_name = _get_unstripped(df, "parentFullName")
        out = keyed.filter(pl.col("txn_pk").is_not_null()).with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.lit(None, dtype=pl.Int64).alias("traveler_person_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            _get_unstripped(df, "parentType").alias("parent_transaction_type"),
            _get_unstripped(df, "parentId").alias("parent_transaction_id"),
            common.builder_amount("parentAmount").alias("parent_amount"),
            common.builder_amount("parentAmount").alias("amount"),
            common.builder_date(spec.date_col).alias("travel_date"),
            _get_unstripped(df, "transportationTypeCd").alias("transportation_type"),
            _get_unstripped(df, "transportationTypeDescr").alias("transportation_description"),
            _get_unstripped(df, "departureCity").alias("departure_city"),
            pl.lit(None, dtype=pl.Utf8).alias("departure_state"),
            _get_unstripped(df, "arrivalCity").alias("arrival_city"),
            pl.lit(None, dtype=pl.Utf8).alias("arrival_state"),
            common.builder_date("departureDt").alias("departure_date"),
            common.builder_date("arrivalDt").alias("arrival_date"),
            pl.coalesce([_get_unstripped(df, "travelPurpose"), descr]).alias("travel_purpose"),
            traveler_name.alias("traveler_name"),
        ).select(
            "transaction_id", "traveler_person_id", "state_id",
            "parent_transaction_type", "parent_transaction_id", "parent_amount",
            "amount", "travel_date", "transportation_type", "transportation_description",
            "departure_city", "departure_state", "arrival_city", "arrival_state",
            "departure_date", "arrival_date", "travel_purpose", "traveler_name",
        )
        return common.write_frame(ctx.session, UnifiedTravel, out, conflict_cols=None)

    def _build_asset(self, df, spec, ctx, txn_map) -> int:
        keyed = df.with_columns(
            (pl.col(spec.id_col).cast(pl.Utf8) if spec.id_col in df.columns
             else pl.lit(None, dtype=pl.Utf8)).alias("_txn_id")
        )
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        # description <- assetDescr (0.9) ; transaction.description set; detail
        # description = transaction.description or asset_descr.
        descr = _get_unstripped(df, "assetDescr")
        date_expr = common.builder_date("receivedDt")  # acquisition_date = txn date (fallback)
        out = keyed.filter(pl.col("txn_pk").is_not_null()).with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            _cs("filerIdent").alias("committee_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            pl.lit(None, dtype=pl.Utf8).alias("asset_type"),
            descr.alias("description"),
            date_expr.alias("acquisition_date"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("acquisition_cost"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("current_value"),
            pl.lit(None, dtype=pl.Date).alias("valuation_date"),
            pl.lit(None, dtype=pl.Date).alias("disposition_date"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("disposition_amount"),
            pl.lit(False).alias("is_disposed"),
        ).select(
            "transaction_id", "committee_id", "state_id", "asset_type", "description",
            "acquisition_date", "acquisition_cost", "current_value", "valuation_date",
            "disposition_date", "disposition_amount", "is_disposed",
        )
        return common.write_frame(ctx.session, UnifiedAsset, out, conflict_cols=None)

    def _build_pledge(self, df, spec, ctx, txn_map) -> int:
        keyed = df.with_columns(
            (pl.col(spec.id_col).cast(pl.Utf8) if spec.id_col in df.columns
             else pl.lit(None, dtype=pl.Utf8)).alias("_txn_id")
        )
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        orig_cols = self._orig_cols.get(spec.record_type, list(df.columns))
        # build_pledge: pledgor/recipient entity ids = None (loader passes None).
        # amount = pledge_amount(pledgeAmount) or txn.amount (== pledgeAmount here).
        # pledge_date = pledgeDt or txn.transaction_date. description = pledgeDescr.
        amount = common.builder_amount("pledgeAmount")
        pdate = _pledge_date_expr("pledgeDt")
        descr = _get_unstripped(df, "pledgeDescr")
        out = keyed.filter(pl.col("txn_pk").is_not_null()).with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.lit(None, dtype=pl.Int64).alias("pledgor_entity_id"),
            pl.lit(None, dtype=pl.Int64).alias("recipient_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            amount.alias("amount"),
            pdate.alias("pledge_date"),
            pl.lit(False).alias("is_fulfilled"),
            descr.alias("description"),
            common.raw_json_expr(orig_cols, alias="metadata_json"),
        ).select(
            "transaction_id", "pledgor_entity_id", "recipient_entity_id", "state_id",
            "amount", "pledge_date", "is_fulfilled", "description", "metadata_json",
        )
        return common.write_frame(ctx.session, UnifiedPledge, out, conflict_cols=None)

    # ---- guarantors -----------------------------------------------------

    def _build_guarantors(
        self, frames: dict[str, pl.DataFrame], ordered: list[str], ctx: FamilyContext
    ) -> int:
        total = 0
        loan_pk = _loan_pk_map(ctx.engine, "unified_loans") if "LOAN" in frames else {}
        debt_pk = _loan_pk_map(ctx.engine, "unified_debts") if "DEBT" in frames else {}
        txn_map = _txn_id_map(ctx.engine, ctx.state_id, ordered)

        for rt, parent_table, pk_map in (
            ("LOAN", "loan_id", loan_pk),
            ("DEBT", "debt_id", debt_pk),
        ):
            if rt not in frames:
                continue
            spec = _SPECS[rt]
            df = frames[rt]
            rows = self._guarantor_rows(df, spec, ctx, txn_map, pk_map, parent_table)
            if rows is not None and rows.height:
                total += common.write_frame(
                    ctx.session, LoanGuarantor, rows, conflict_cols=None
                )
        return total

    def _guarantor_rows(self, df, spec, ctx, txn_map, pk_map: dict[int, int],
                        parent_col: str) -> pl.DataFrame | None:
        # Map each row to its parent detail surrogate id via transaction id.
        txn_id = (
            pl.col(spec.id_col).cast(pl.Utf8)
            if spec.id_col in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        )
        keyed = df.with_columns(txn_id.alias("_txn_id"))
        keyed = self._join_txn(keyed, txn_map, spec.transaction_type)
        # txn_pk -> detail pk
        pk_frame = pl.DataFrame(
            {"txn_pk": list(pk_map.keys()), "_detail_pk": list(pk_map.values())},
            schema={"txn_pk": pl.Int64, "_detail_pk": pl.Int64},
        )
        keyed = keyed.join(pk_frame, on="txn_pk", how="left")

        # Build one struct list per slot 1..5, then explode.
        slot_structs = []
        for i in range(1, 6):
            slot_structs.append(
                pl.struct(
                    [
                        pl.lit(i, dtype=pl.Int64).alias("position"),
                        _guar(df, f"guarantorPersentTypeCd{i}", 30).alias("person_type"),
                        _guar(df, f"guarantorNameOrganization{i}", 200).alias("organization"),
                        _guar(df, f"guarantorNameLast{i}", 100).alias("last_name"),
                        _guar(df, f"guarantorNameFirst{i}", 100).alias("first_name"),
                        _guar(df, f"guarantorNameSuffixCd{i}", 30).alias("suffix"),
                        _guar(df, f"guarantorNamePrefixCd{i}", 30).alias("prefix"),
                        _guar(df, f"guarantorStreetCity{i}", 100).alias("city"),
                        _guar(df, f"guarantorStreetStateCd{i}", 2).alias("state_code"),
                        _guar(df, f"guarantorStreetCountyCd{i}", 10).alias("county"),
                        _guar(df, f"guarantorStreetCountryCd{i}", 3).alias("country"),
                        _guar(df, f"guarantorStreetPostalCode{i}", 20).alias("postal_code"),
                        _guar(df, f"guarantorStreetRegion{i}", 50).alias("region"),
                    ]
                )
            )
        keyed = keyed.with_columns(
            pl.concat_list(slot_structs).alias("_slots")
        ).explode("_slots")
        keyed = keyed.unnest("_slots")
        # Emit a slot only when last/first/org present (mirrors _build_guarantors).
        keyed = keyed.filter(
            pl.col("last_name").is_not_null()
            | pl.col("first_name").is_not_null()
            | pl.col("organization").is_not_null()
        )
        keyed = keyed.filter(pl.col("_detail_pk").is_not_null())
        if keyed.height == 0:
            return None
        out = keyed.with_columns(
            pl.col("_detail_pk").alias(parent_col),
            pl.lit(None, dtype=pl.Int64).alias(
                "debt_id" if parent_col == "loan_id" else "loan_id"
            ),
            pl.lit(None, dtype=pl.Int64).alias("entity_id"),
        ).select(
            "loan_id", "debt_id", "entity_id", "position", "person_type",
            "organization", "last_name", "first_name", "suffix", "prefix",
            "city", "state_code", "county", "country", "postal_code", "region",
        )
        return out


# ---------------------------------------------------------------------------
# Module-level expression helpers
# ---------------------------------------------------------------------------


def _get_unstripped(df: pl.DataFrame, col: str | None) -> pl.Expr:
    """Mirror _get_field_value: return the raw column value unstripped, or null.

    The ORM detail builders assign ``_get_field_value`` results without
    stripping; only the empty-string -> None normalization differs (Polars keeps
    ""), so for parity with the snapshot (which str()s values) we treat empty as
    the raw value.  TEC golden columns carry no such empties on these fields.
    """
    if col is None or col not in df.columns:
        return pl.lit(None, dtype=pl.Utf8)
    return pl.col(col).cast(pl.Utf8)


def _guar(df: pl.DataFrame, col: str, max_len: int) -> pl.Expr:
    """Mirror processor._guarantor_str: strip; empty -> None; clip to max_len."""
    if col not in df.columns:
        return pl.lit(None, dtype=pl.Utf8)
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    s = pl.when(s.str.len_chars() > 0).then(s).otherwise(None)
    return s.str.slice(0, max_len)


def _pledge_date_expr(col: str) -> pl.Expr:
    """Mirror pledges_ingest._parse_date: %Y%m%d, %Y-%m-%d, %m/%d/%Y only."""
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    parsed = s.str.to_date("%Y%m%d", strict=False)
    parsed = parsed.fill_null(s.str.to_date("%Y-%m-%d", strict=False))
    parsed = parsed.fill_null(s.str.to_date("%m/%d/%Y", strict=False))
    return parsed


register(DetailChildrenWorker())
