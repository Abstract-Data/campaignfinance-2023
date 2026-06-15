"""Vectorized flat-transactions DETAIL/JUNCTION family for RCPT / EXPN.

Runs AFTER ``flat_txns_dims`` (priority 9, writes persons/entities/addresses/
committees) and ``flat_txns`` (priority 10, writes ``unified_transactions``), so
every surrogate id this family references already exists. Priority 11.

Produces, with REAL surrogate-id linkage (id-maps read back by natural key):
  RCPT -> ``unified_contributions``      (contributor_entity_id, recipient_entity_id)
  EXPN -> ``unified_expenditures``       (payer_entity_id, payee_entity_id)
  both -> ``unified_transaction_persons``(transaction_id, person_id, entity_id, role)

Mirrors ``app/core/processor.py``::
  * ``RECORD_TYPE_ROLE_MAP``: RCPT -> {CONTRIBUTOR: "contributor"},
    EXPN -> {PAYEE: "payee"}.
  * ``_build_contribution_detail``: created ONLY when contributor_entity AND
    recipient_entity (the committee entity) both exist.
  * ``_build_expenditure_detail``: created ONLY when payer_entity (committee) AND
    payee_entity both exist.
  * ``_attach_transaction_persons``: one junction row per built participant, with
    RECIPIENT excluded (RCPT only builds CONTRIBUTOR, EXPN only builds PAYEE, so the
    exclusion is a no-op here but kept explicit), ``entity_id = person.entity.id``.
  * ``contribution_type`` / ``expenditure_type`` are unmapped for Texas RCPT/EXPN
    (``_get_field_value`` -> None); ``is_anonymous`` keeps its model default (False).
  * amount / date / description are copied from the parent transaction row
    (``transaction.amount`` / ``.transaction_date`` / ``.description``), so they are
    re-derived here from the same source columns the flat_txns family uses.

Linkage (id-joins, NEVER surrogate guesses):
  * Transaction id  <- (state_id, transaction_type, transaction_id) natural key.
  * Person id       <- (lower(first), lower(last), lower(org)) within the state.
  * Person-entity id<- (entity_type, normalized_name) within the state.
  * Committee-entity<- (COMMITTEE, normalize_entity_name(committee name)) within state.
All id-maps are read via SQLAlchemy core ``select`` (parameterized) over the already
-written tables, then attached with Polars joins. Pure column expressions only —
no per-row Python UDF (no element-wise map, no row apply).
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import polars as pl
from sqlalchemy import select

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.models import (
    UnifiedContribution,
    UnifiedEntity,
    UnifiedExpenditure,
    UnifiedPerson,
    UnifiedTransaction,
    UnifiedTransactionPerson,
)
from app.logger import Logger

_logger = Logger(__name__)


# ---------------------------------------------------------------------------
# Source column whitelists (kept in lockstep with flat_txns / flat_txns_dims).
# ---------------------------------------------------------------------------

_RCPT_COLS = (
    "recordType", "formTypeCd", "schedFormTypeCd", "reportInfoIdent",
    "receivedDt", "infoOnlyFlag", "filerIdent", "filerTypeCd", "filerName",
    "contributionInfoId", "contributionDt", "contributionAmount", "contributionDescr",
    "itemizeFlag", "travelFlag",
    "contributorPersentTypeCd", "contributorNameOrganization",
    "contributorNameLast", "contributorNameSuffixCd", "contributorNameFirst",
    "contributorNamePrefixCd", "contributorNameShort",
    "contributorStreetCity", "contributorStreetStateCd", "contributorStreetCountyCd",
    "contributorStreetCountryCd", "contributorStreetPostalCode", "contributorStreetRegion",
    "contributorEmployer", "contributorOccupation", "contributorJobTitle",
    "contributorPacFein", "contributorOosPacFlag",
    "contributorLawFirmName", "contributorSpouseLawFirmName",
    "contributorParent1LawFirmName", "contributorParent2LawFirmName",
)

_EXPN_COLS = (
    "recordType", "formTypeCd", "schedFormTypeCd", "reportInfoIdent",
    "receivedDt", "infoOnlyFlag", "filerIdent", "filerTypeCd", "filerName",
    "expendInfoId", "expendDt", "expendAmount", "expendDescr",
    "expendCatCd", "expendCatDescr", "itemizeFlag", "travelFlag",
    "politicalExpendCd", "reimburseIntendedFlag", "srcCorpContribFlag", "capitalLivingexpFlag",
    "payeePersentTypeCd", "payeeNameOrganization",
    "payeeNameLast", "payeeNameSuffixCd", "payeeNameFirst",
    "payeeNamePrefixCd", "payeeNameShort",
    "payeeStreetAddr1", "payeeStreetAddr2",
    "payeeStreetCity", "payeeStreetStateCd", "payeeStreetCountyCd",
    "payeeStreetCountryCd", "payeeStreetPostalCode", "payeeStreetRegion",
    "creditCardIssuer", "repaymentDt",
)

# Placeholder last names that force PersonType.UNKNOWN (mirrors build_person /
# constants.PLACEHOLDER_NAMES, applied case-insensitively on the stripped last name).
_PLACEHOLDER_NAMES_UPPER = frozenset({
    "NON-ITEMIZED CONTRIBUTOR", "NON-ITEMIZED", "UNKNOWN", "ANONYMOUS",
})


# ---------------------------------------------------------------------------
# Frame IO helpers
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


# ---------------------------------------------------------------------------
# Parent-transaction column re-derivation (must match flat_txns.py exactly).
# ---------------------------------------------------------------------------

def _transaction_date_expr(date_col: str) -> pl.Expr:
    return common.builder_date(date_col).fill_null(common.builder_date("receivedDt"))


# ---------------------------------------------------------------------------
# Id-maps read back from the already-written tables (parameterized core select).
# ---------------------------------------------------------------------------

def _entity_id_map(session, state_id: int) -> pl.DataFrame:
    """Read entity id-map for the state's entities.

    Person/org entities join on (entity_type, normalized_name); committee entities
    join on their natural ``committee_id`` (= committee filer_id), NOT on name —
    the ORM resolves the recipient/payer via ``committee.entity`` (found by
    filer_id), so per-row filerName variants must all map to the single committee
    entity. entity_type is stored as the enum NAME ("PERSON"/"ORGANIZATION"/
    "COMMITTEE"); a "" normalized_name is stored as NULL by the ORM (coalesced to
    "" here for join parity).
    """
    stmt = select(
        UnifiedEntity.id,
        UnifiedEntity.entity_type,
        UnifiedEntity.normalized_name,
        UnifiedEntity.committee_id,
    ).where(UnifiedEntity.state_id == state_id)
    rows = session.execute(stmt).all()
    return pl.DataFrame(
        {
            "_ent_id": [r[0] for r in rows],
            # SQLAlchemy hands back the Enum member; take its .name for the stored form.
            "_ent_type": [getattr(r[1], "name", r[1]) for r in rows],
            # ORM stores "" normalized_name as NULL -> coalesce to "" for join parity.
            "_ent_norm": [(r[2] if r[2] is not None else "") for r in rows],
            "_ent_committee_id": [r[3] for r in rows],
        },
        schema={
            "_ent_id": pl.Int64, "_ent_type": pl.Utf8, "_ent_norm": pl.Utf8,
            "_ent_committee_id": pl.Utf8,
        },
    )


def _person_id_map(session, state_id: int) -> pl.DataFrame:
    """Read persons keyed by (lower first, lower last, lower org, dedup_addr_key), and
    ALSO carry each person's entity key (entity_type, normalized_name) derived from the
    person's STORED name (incl. middle/suffix the dim layer backfilled).

    The address dimension (``_pk_addr`` = stored ``dedup_addr_key``) is part of the
    individual key so two same-name people at distinct locations map to distinct ids
    (matching uix_persons_name_state).

    The party (contributor/payee) entity is linked via this STORED entity key —
    not the source row's recomputed name — so a person whose suffix/middle differs
    across source rows still resolves to the single entity dims built from their
    first occurrence (matching the ORM, which links via the person object).
    """
    rows = session.execute(
        select(
            UnifiedPerson.id,
            UnifiedPerson.first_name,
            UnifiedPerson.middle_name,
            UnifiedPerson.last_name,
            UnifiedPerson.suffix,
            UnifiedPerson.organization,
            UnifiedPerson.dedup_addr_key,
        ).where(UnifiedPerson.state_id == state_id)
    ).all()

    def _lower_or_null(v):
        return v.strip().lower() if isinstance(v, str) and v.strip() else None

    return pl.DataFrame(
        {
            "_pid": [r[0] for r in rows],
            "_pk_fn": [_lower_or_null(r[1]) for r in rows],
            "_pk_ln": [_lower_or_null(r[3]) for r in rows],
            "_pk_org": [_lower_or_null(r[5]) for r in rows],
            "_pk_addr": [r[6] for r in rows],
            "first_name": [r[1] for r in rows],
            "middle_name": [r[2] for r in rows],
            "last_name": [r[3] for r in rows],
            "suffix": [r[4] for r in rows],
            "organization": [r[5] for r in rows],
        },
        schema={
            "_pid": pl.Int64, "_pk_fn": pl.Utf8, "_pk_ln": pl.Utf8, "_pk_org": pl.Utf8,
            "_pk_addr": pl.Utf8,
            "first_name": pl.Utf8, "middle_name": pl.Utf8, "last_name": pl.Utf8,
            "suffix": pl.Utf8, "organization": pl.Utf8,
        },
        # Org-persons keyed on lower(org) ALONE (null fn/ln/addr) — matches
        # uix_persons_org_state and the family-side dedup key, so the id-join finds the
        # single org person.
    ).pipe(common.collapse_org_person_key).with_columns(
        pl.when(_cs("organization").is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .otherwise(pl.lit("PERSON"))
        .alias("_party_ent_type"),
        _norm_name_expr_from(
            pl.when(_cs("organization").is_not_null())
            .then(_cs("organization"))
            .otherwise(
                common.full_name_expr(
                    "first_name", "middle_name", "last_name", "suffix", "organization"
                )
            )
        ).alias("_party_ent_norm"),
    ).select(
        ["_pid", "_pk_fn", "_pk_ln", "_pk_org", "_pk_addr",
         "_party_ent_type", "_party_ent_norm"]
    )


def _transaction_id_map(session, state_id: int, transaction_type: str) -> pl.DataFrame:
    """Read transaction_id (natural) -> id for one (state, transaction_type)."""
    stmt = select(
        UnifiedTransaction.id,
        UnifiedTransaction.transaction_id,
    ).where(
        UnifiedTransaction.state_id == state_id,
        UnifiedTransaction.transaction_type == transaction_type,
    )
    rows = session.execute(stmt).all()
    return pl.DataFrame(
        {
            "_txn_id": [r[0] for r in rows],
            "_txn_nat": [(None if r[1] is None else str(r[1])) for r in rows],
        },
        schema={"_txn_id": pl.Int64, "_txn_nat": pl.Utf8},
    )


def _address_id_map(session, state_id: int) -> pl.DataFrame:
    """Read (lower street_1, lower city, lower state, zip) -> id for addresses.

    Mirrors the dims address dedup key (``_address_dedup``) — addresses are not
    state-scoped (the model has no per-row state_id used in the key), so all rows
    are read. The 4-field natural key matches the ORM's ``_find_address_by_fields``
    case-insensitive lookup (street_1/city/state lowered, zip as-is).
    """
    from app.core.models import UnifiedAddress

    stmt = select(
        UnifiedAddress.id,
        UnifiedAddress.street_1,
        UnifiedAddress.city,
        UnifiedAddress.state,
        UnifiedAddress.zip_code,
    )
    rows = session.execute(stmt).all()

    def _low(v):
        return v.lower() if isinstance(v, str) else None

    return pl.DataFrame(
        {
            "_aid": [r[0] for r in rows],
            "_ak_s1": [_low(r[1]) for r in rows],
            "_ak_city": [_low(r[2]) for r in rows],
            "_ak_state": [_low(r[3]) for r in rows],
            "_ak_zip": [r[4] for r in rows],
        },
        schema={
            "_aid": pl.Int64, "_ak_s1": pl.Utf8, "_ak_city": pl.Utf8,
            "_ak_state": pl.Utf8, "_ak_zip": pl.Utf8,
        },
    )


# ---------------------------------------------------------------------------
# Dim-FK retrofit (person.address_id, entity.person_id, entity.address_id).
#
# The dim family writes persons/entities/addresses as independent frames without
# surrogate linkage. The ORM links entity -> person -> address (and entity ->
# address). Under resolve_fks=True the gate verifies that linkage, so this family
# retrofits those FKs by real id-joins: each person takes the address from its
# FIRST occurrence (RCPT before EXPN, in id order); each PERSON/ORGANIZATION entity
# takes the person (and that person's address) from the FIRST person that created
# the entity's normalized name. COMMITTEE entities get no person and no address
# (RCPT/EXPN carry no committee street columns).
# ---------------------------------------------------------------------------

# A large offset added to EXPN sort keys so EXPN persons sort AFTER all RCPT
# persons (mirrors flat_txns_dims load order; RCPT priority 10 < EXPN within file).
_EXPN_SORT_OFFSET = 1_000_000_000_000


def _person_addr_keys(
    df: pl.DataFrame,
    *,
    org_col: str,
    first_col: str,
    last_col: str,
    suffix_col: str,
    s1_col: str | None,
    city_col: str,
    state_col: str,
    zip_col: str,
    id_col: str,
    sort_offset: int,
) -> pl.DataFrame:
    """Project per-row person key + address key + entity key + load-order sort key.

    Address fields mirror flat_txns_dims._address_frame_* (RCPT has no street_1).
    Entity key mirrors flat_txns_dims._build_entities_from_persons: entity_type is
    ORGANIZATION when org present else PERSON; entity name = org else full_name;
    normalized via normalize_entity_name.
    """
    s1 = (
        _cs(s1_col).str.to_lowercase()
        if s1_col is not None
        else pl.lit(None, dtype=pl.Utf8)
    )
    full_name = common.full_name_expr(
        first_col, "person_middle_name", last_col, suffix_col, org_col
    )
    org = _cs(org_col)
    ent_name = pl.when(org.is_not_null()).then(org).otherwise(full_name)
    ent_type = pl.when(org.is_not_null()).then(pl.lit("ORGANIZATION")).otherwise(pl.lit("PERSON"))
    # Address dimension of the individual key (NULL for org-persons so they key on
    # org alone, matching the dim layer + uix_persons_org_state).
    addr_key = (
        pl.when(org.is_not_null())
        .then(None)
        .otherwise(
            common.person_addr_key_expr(
                pl.lit(None, dtype=pl.Utf8) if s1_col is None else s1_col,
                city_col,
                common.upper_str(state_col),
                zip_col,
            )
        )
    )
    return df.with_columns(
        _cs(org_col).str.to_lowercase().alias("_pk_org"),
        _cs(first_col).str.to_lowercase().alias("_pk_fn"),
        _cs(last_col).str.to_lowercase().alias("_pk_ln"),
        addr_key.alias("_pk_addr"),
        s1.alias("_ak_s1"),
        _cs(city_col).str.to_lowercase().alias("_ak_city"),
        common.upper_str(state_col).str.to_lowercase().alias("_ak_state"),
        _cs(zip_col).alias("_ak_zip"),
        (pl.col(id_col).cast(pl.Int64) + sort_offset).alias("_sort_key"),
        # full_name length gates whether a person exists at all (ORM build_person).
        full_name.str.len_chars().alias("_full_len"),
        ent_type.alias("_ent_type"),
        _norm_name_expr_from(ent_name).alias("_ent_norm"),
    ).select(
        ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr", "_ak_s1", "_ak_city", "_ak_state",
         "_ak_zip", "_sort_key", "_full_len", "_ent_type", "_ent_norm"]
    )


def _first_per_person(rows: pl.DataFrame) -> pl.DataFrame:
    """First occurrence per (org, fn, ln, addr) by ascending sort key (load order)."""
    return (
        rows.filter(pl.col("_full_len") > 0)
        .sort("_sort_key")
        .unique(
            subset=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
            keep="first",
            maintain_order=True,
        )
    )


# ---------------------------------------------------------------------------
# Per-row participant projection (one frame: txn natural key + person/entity keys).
# ---------------------------------------------------------------------------

def _project_rcpt(df: pl.DataFrame) -> pl.DataFrame:
    """RCPT -> one row per source record with join keys + parent-txn fields.

    A participant person exists when name.full_name is non-empty (mirrors
    build_person -> None on empty name). The contributor entity is PERSON (full
    name) or ORGANIZATION (org name).
    """
    org = _cs("contributorNameOrganization")
    first = _cs("contributorNameFirst")
    last = _cs("contributorNameLast")
    full_name = common.full_name_expr(
        "contributorNameFirst", "person_middle_name", "contributorNameLast",
        "contributorNameSuffixCd", "contributorNameOrganization",
    )
    return df.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("person_middle_name"),
    ).with_columns(
        # parent-txn natural key
        pl.col("contributionInfoId").cast(pl.Utf8).alias("_txn_nat"),
        # parent-txn copied fields (must match flat_txns.py)
        common.builder_amount("contributionAmount").alias("amount"),
        _transaction_date_expr("contributionDt").alias("txn_date"),
        pl.col("contributionDescr").cast(pl.Utf8).alias("description"),
        # committee (recipient/payer) natural key — joined by filer_id, not name.
        _cs("filerIdent").alias("_filer_id"),
        # participant person key (the participant entity is resolved from the
        # STORED person row via _person_id_map, not recomputed here).
        first.str.to_lowercase().alias("_pk_fn"),
        last.str.to_lowercase().alias("_pk_ln"),
        org.str.to_lowercase().alias("_pk_org"),
        # Address dimension of the individual key (RCPT has no street_1). NULL for
        # org participants so they match the org-collapsed person_map (addr NULL).
        pl.when(org.is_not_null())
        .then(None)
        .otherwise(
            common.person_addr_key_expr(
                pl.lit(None, dtype=pl.Utf8),
                "contributorStreetCity",
                common.upper_str("contributorStreetStateCd"),
                "contributorStreetPostalCode",
            )
        )
        .alias("_pk_addr"),
        full_name.str.len_chars().alias("_full_len"),
    )


def _project_expn(df: pl.DataFrame) -> pl.DataFrame:
    """EXPN -> one row per source record with join keys + parent-txn fields."""
    org = _cs("payeeNameOrganization")
    first = _cs("payeeNameFirst")
    last = _cs("payeeNameLast")
    full_name = common.full_name_expr(
        "payeeNameFirst", "person_middle_name", "payeeNameLast",
        "payeeNameSuffixCd", "payeeNameOrganization",
    )
    return df.with_columns(
        pl.lit(None, dtype=pl.Utf8).alias("person_middle_name"),
    ).with_columns(
        pl.col("expendInfoId").cast(pl.Utf8).alias("_txn_nat"),
        common.builder_amount("expendAmount").alias("amount"),
        _transaction_date_expr("expendDt").alias("txn_date"),
        pl.col("expendDescr").cast(pl.Utf8).alias("description"),
        # committee (recipient/payer) natural key — joined by filer_id, not name.
        _cs("filerIdent").alias("_filer_id"),
        first.str.to_lowercase().alias("_pk_fn"),
        last.str.to_lowercase().alias("_pk_ln"),
        org.str.to_lowercase().alias("_pk_org"),
        # Address dimension (EXPN carries street_1). NULL for org payees so they match
        # the org-collapsed person_map (addr NULL).
        pl.when(org.is_not_null())
        .then(None)
        .otherwise(
            common.person_addr_key_expr(
                "payeeStreetAddr1",
                "payeeStreetCity",
                common.upper_str("payeeStreetStateCd"),
                "payeeStreetPostalCode",
            )
        )
        .alias("_pk_addr"),
        full_name.str.len_chars().alias("_full_len"),
    )


def _norm_name_expr_from(name_expr: pl.Expr) -> pl.Expr:
    """normalize_entity_name applied to an arbitrary string expression."""
    s = name_expr.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    s = s.str.replace_all(r"[^a-z0-9]+", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return s.fill_null("")


# ---------------------------------------------------------------------------
# Join helpers
# ---------------------------------------------------------------------------

def _attach_party_person(proj: pl.DataFrame, person_map: pl.DataFrame) -> pl.DataFrame:
    """Attach the participant person id (and a built flag) via the name + address key.

    Persons are deduped to (org, fn, ln, addr) within the state; orgs key on org alone
    (fn/ln/addr null), individuals on (fn, ln, addr) with org null — exactly how
    _person_id_map keys the read-back rows.  The participant's ``_pk_addr`` comes from
    its own contributor/payee address columns (computed in _project_*), so it resolves
    to the same address-split person the dim layer created.
    """
    return proj.join(
        person_map,
        on=["_pk_fn", "_pk_ln", "_pk_org", "_pk_addr"],
        how="left",
        join_nulls=True,
    )


def _attach_party_entity(proj: pl.DataFrame, entity_map: pl.DataFrame) -> pl.DataFrame:
    """Attach the participant (contributor/payee) entity id via (type, norm name).

    Only PERSON/ORGANIZATION entities are candidates — a participant is never a
    committee — so committee rows are excluded to avoid a normalized-name collision
    between a committee entity and a same-named org participant.
    """
    # Exclude empty-normalized-name entities from the candidate set: the ORM's
    # ``_find_entity`` returns None when ``not normalized_name`` (builders.py), so a
    # blank-name participant (``_party_ent_norm == ""``) must NOT resolve to any entity.
    # Without this guard the "" <-> "" join would spuriously link a blank participant to
    # an entity whose normalized_name was stored NULL (coalesced to "" in the id-map),
    # creating a contribution/expenditure the ORM would have skipped.
    party = (
        entity_map.filter((pl.col("_ent_type") != "COMMITTEE") & (pl.col("_ent_norm") != ""))
        .select(["_ent_id", "_ent_type", "_ent_norm"])
        .rename({"_ent_id": "_party_ent_id"})
    )
    return proj.join(
        party,
        left_on=["_party_ent_type", "_party_ent_norm"],
        right_on=["_ent_type", "_ent_norm"],
        how="left",
    )


def _attach_committee_entity(proj: pl.DataFrame, entity_map: pl.DataFrame) -> pl.DataFrame:
    """Attach the committee entity id via the committee's natural filer_id.

    The ORM links recipient/payer to ``committee.entity`` (the committee is found
    by filer_id), so per-row filerName variants all resolve to the one committee
    entity for that filer.
    """
    comm = (
        entity_map.filter(
            (pl.col("_ent_type") == "COMMITTEE") & pl.col("_ent_committee_id").is_not_null()
        )
        .select(["_ent_id", "_ent_committee_id"])
        .rename({"_ent_id": "_committee_ent_id"})
    )
    return proj.join(
        comm,
        left_on="_filer_id",
        right_on="_ent_committee_id",
        how="left",
    )


def _attach_txn(proj: pl.DataFrame, txn_map: pl.DataFrame) -> pl.DataFrame:
    return proj.join(txn_map, on="_txn_nat", how="left")


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------

class FlatTxnsDetailWorker:
    """Detail/junction rows for RCPT/EXPN with real surrogate-id linkage."""

    record_types = frozenset({"RCPT", "EXPN"})
    priority = 11  # after flat_txns_dims (9) and flat_txns (10)

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        rcpt = _read(files_by_type.get("RCPT", []))
        expn = _read(files_by_type.get("EXPN", []))
        if rcpt is not None:
            rcpt = _ensure_cols(rcpt, _RCPT_COLS)
        if expn is not None:
            expn = _ensure_cols(expn, _EXPN_COLS)

        # Id-maps from the already-populated dim + transaction tables.
        entity_map = _entity_id_map(ctx.session, ctx.state_id)
        person_map = _person_id_map(ctx.session, ctx.state_id)

        counts: dict[str, int] = {}

        if rcpt is not None and rcpt.height > 0:
            txn_map = _transaction_id_map(ctx.session, ctx.state_id, "CONTRIBUTION")
            proj = _project_rcpt(rcpt)
            proj = _attach_txn(proj, txn_map)
            proj = _attach_party_person(proj, person_map)
            proj = _attach_party_entity(proj, entity_map)
            proj = _attach_committee_entity(proj, entity_map)
            counts["contributions"] = self._write_contributions(proj, ctx)
            counts["txn_persons_rcpt"] = self._write_txn_persons(
                proj, ctx, role="CONTRIBUTOR"
            )

        if expn is not None and expn.height > 0:
            txn_map = _transaction_id_map(ctx.session, ctx.state_id, "EXPENDITURE")
            proj = _project_expn(expn)
            proj = _attach_txn(proj, txn_map)
            proj = _attach_party_person(proj, person_map)
            proj = _attach_party_entity(proj, entity_map)
            proj = _attach_committee_entity(proj, entity_map)
            counts["expenditures"] = self._write_expenditures(proj, ctx)
            counts["txn_persons_expn"] = self._write_txn_persons(
                proj, ctx, role="PAYEE"
            )

        # Retrofit dim-layer FKs (person.address_id, entity.person_id,
        # entity.address_id) that flat_txns_dims left unset — verified under
        # resolve_fks=True.
        self._retrofit_dim_fks(rcpt, expn, ctx)

        loaded = sum(counts.values())
        _logger.info(f"[vectorized.flat_txns_detail] loaded {loaded} detail rows: {counts}")
        return {"loaded": loaded, **counts}

    # -- dim-FK retrofit ----------------------------------------------------

    def _retrofit_dim_fks(
        self,
        rcpt: pl.DataFrame | None,
        expn: pl.DataFrame | None,
        ctx: FamilyContext,
    ) -> None:
        """Set person.address_id, entity.person_id, entity.address_id via id-joins.

        Each person takes the address of its FIRST occurrence (RCPT before EXPN);
        each PERSON/ORGANIZATION entity takes the FIRST person (and that person's
        address) that created its normalized name. All ids come from the already
        -written dim tables (no surrogate guessing). Updates are parameterized core
        statements (``bindparam`` + ``.values``), never string SQL.
        """
        person_map = _person_id_map(ctx.session, ctx.state_id)
        addr_map = _address_id_map(ctx.session, ctx.state_id)

        # 1. Per-person first-occurrence address key (load order: RCPT then EXPN).
        per_row_parts: list[pl.DataFrame] = []
        if rcpt is not None and rcpt.height > 0:
            r = rcpt.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("person_middle_name"),
            )
            per_row_parts.append(
                _person_addr_keys(
                    r, org_col="contributorNameOrganization",
                    first_col="contributorNameFirst", last_col="contributorNameLast",
                    suffix_col="contributorNameSuffixCd",
                    s1_col=None, city_col="contributorStreetCity",
                    state_col="contributorStreetStateCd",
                    zip_col="contributorStreetPostalCode",
                    id_col="contributionInfoId", sort_offset=0,
                )
            )
        if expn is not None and expn.height > 0:
            e = expn.with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("person_middle_name"),
            )
            per_row_parts.append(
                _person_addr_keys(
                    e, org_col="payeeNameOrganization",
                    first_col="payeeNameFirst", last_col="payeeNameLast",
                    suffix_col="payeeNameSuffixCd",
                    s1_col="payeeStreetAddr1", city_col="payeeStreetCity",
                    state_col="payeeStreetStateCd",
                    zip_col="payeeStreetPostalCode",
                    id_col="expendInfoId", sort_offset=_EXPN_SORT_OFFSET,
                )
            )
        if not per_row_parts:
            return

        per_row = pl.concat(per_row_parts, how="diagonal_relaxed")
        first = _first_per_person(per_row)

        # Attach person id and address id to each first-occurrence person.
        first = first.join(
            person_map,
            on=["_pk_fn", "_pk_ln", "_pk_org", "_pk_addr"],
            how="left",
            join_nulls=True,
        )
        first = first.join(
            addr_map, on=["_ak_s1", "_ak_city", "_ak_state", "_ak_zip"],
            how="left", join_nulls=True,
        )

        # 2. person.address_id update set (persons with a resolved address).
        #    Entity.person_id / entity.address_id are NOT set here — that is done once,
        #    deterministically, by finalize_entity_representatives after all families run
        #    (a person can map to >1 entity via suffix-variant normalized names, so a
        #    per-family entity-rep assignment violates the one-to-one person_id unique).
        person_addr = (
            first.filter(pl.col("_pid").is_not_null() & pl.col("_aid").is_not_null())
            .select(pl.col("_pid").alias("pid"), pl.col("_aid").alias("aid"))
        )
        self._apply_person_address(ctx.session, person_addr)

    @staticmethod
    def _apply_person_address(session, frame: pl.DataFrame) -> None:
        from sqlalchemy import bindparam, update

        if frame.is_empty():
            return
        stmt = (
            update(UnifiedPerson.__table__)
            .where(UnifiedPerson.__table__.c.id == bindparam("b_pid"))
            .values(address_id=bindparam("b_aid"))
        )
        params = [{"b_pid": r["pid"], "b_aid": r["aid"]} for r in frame.to_dicts()]
        session.connection().execute(stmt, params)
        session.commit()

    # -- contributions ------------------------------------------------------

    def _write_contributions(self, proj: pl.DataFrame, ctx: FamilyContext) -> int:
        """One UnifiedContribution per RCPT row that has BOTH a contributor entity
        and a recipient (committee) entity, and a resolved parent transaction.

        Mirrors _build_contribution_detail: skip when contributor_entity OR
        recipient_entity is missing.
        """
        rows = (
            proj.filter(
                pl.col("_txn_id").is_not_null()
                & pl.col("_party_ent_id").is_not_null()
                & pl.col("_committee_ent_id").is_not_null()
            )
            .unique(subset=["_txn_id"], keep="first", maintain_order=True)
            .select(
                pl.col("_txn_id").alias("transaction_id"),
                pl.col("_party_ent_id").alias("contributor_entity_id"),
                pl.col("_committee_ent_id").alias("recipient_entity_id"),
                pl.lit(ctx.state_id).alias("state_id"),
                pl.col("amount"),
                pl.col("txn_date").alias("receipt_date"),
                pl.lit(None, dtype=pl.Utf8).alias("contribution_type"),
                pl.lit(False).alias("is_anonymous"),
                pl.col("description"),
            )
        )
        return common.write_frame(ctx.session, UnifiedContribution, rows, conflict_cols=None)

    # -- expenditures -------------------------------------------------------

    def _write_expenditures(self, proj: pl.DataFrame, ctx: FamilyContext) -> int:
        """One UnifiedExpenditure per EXPN row with BOTH a payer (committee) entity
        and a payee entity, and a resolved parent transaction.

        Mirrors _build_expenditure_detail: payer = committee.entity, payee = payee
        entity; skip when either is missing.
        """
        rows = (
            proj.filter(
                pl.col("_txn_id").is_not_null()
                & pl.col("_party_ent_id").is_not_null()
                & pl.col("_committee_ent_id").is_not_null()
            )
            .unique(subset=["_txn_id"], keep="first", maintain_order=True)
            .select(
                pl.col("_txn_id").alias("transaction_id"),
                pl.col("_committee_ent_id").alias("payer_entity_id"),
                pl.col("_party_ent_id").alias("payee_entity_id"),
                pl.lit(ctx.state_id).alias("state_id"),
                pl.col("amount"),
                pl.col("txn_date").alias("expenditure_date"),
                pl.lit(None, dtype=pl.Utf8).alias("expenditure_type"),
                pl.col("description"),
            )
        )
        return common.write_frame(ctx.session, UnifiedExpenditure, rows, conflict_cols=None)

    # -- transaction_persons ------------------------------------------------

    def _write_txn_persons(self, proj: pl.DataFrame, ctx: FamilyContext, *, role: str) -> int:
        """One junction row per source row that built a participant person.

        Mirrors _attach_transaction_persons: emit only when the participant person
        exists (person_id resolved); ``entity_id = person.entity.id`` (the
        participant entity). RECIPIENT is never emitted (RCPT builds CONTRIBUTOR,
        EXPN builds PAYEE — there is no RECIPIENT participant here).

        Deduped on (transaction_id, person_id, role): the ORM appends one row per
        processed record, but repeats of the same (txn, person, role) collapse —
        and within one RCPT/EXPN row there is exactly one participant, so this only
        guards against the (rare) case where two source rows share a transaction id.
        """
        rows = (
            proj.filter(
                pl.col("_txn_id").is_not_null()
                & pl.col("_pid").is_not_null()
                & (pl.col("_full_len") > 0)
            )
            .with_columns(pl.lit(role).alias("role"))
            .unique(subset=["_txn_id", "_pid", "role"], keep="first", maintain_order=True)
            .select(
                pl.col("_txn_id").alias("transaction_id"),
                pl.col("_pid").alias("person_id"),
                pl.col("_party_ent_id").alias("entity_id"),
                pl.lit(ctx.state_id).alias("state_id"),
                pl.lit(None, dtype=pl.Int64).alias("committee_person_id"),
                pl.col("role"),
                pl.lit(None, dtype=pl.Decimal(38, 4)).alias("amount"),
                pl.lit(None, dtype=pl.Utf8).alias("notes"),
            )
        )
        return common.write_frame(
            ctx.session, UnifiedTransactionPerson, rows, conflict_cols=None
        )


register(FlatTxnsDetailWorker())
