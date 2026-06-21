"""Vectorized dim layer for RCPT/EXPN: addresses, persons, entities, and committees.

Reproduces the ORM's ``build_person``, ``build_address``, ``build_committee``,
and ``_get_or_create_entity`` paths columnar (pure Polars, no map_elements).

Gated by ``diff_snapshots`` restricted to the 4 dim tables
(unified_addresses, unified_persons, unified_entities, unified_committees) in
``tests/ingest_equivalence/test_flat_txns_family.py``.

Detail/junction tables (contributions, expenditures, transaction_persons) are
deferred to a future linkage slice that performs real surrogate-id joins.

Priority 9 ensures dim tables exist before the flat_txns worker (priority 10)
writes unified_transactions (which references committees by natural FK).
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

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
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.models import (
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.logger import Logger

_logger = Logger(__name__)


# ---------------------------------------------------------------------------
# Source column whitelists
# ---------------------------------------------------------------------------

_RCPT_COLS = (
    "recordType",
    "formTypeCd",
    "schedFormTypeCd",
    "reportInfoIdent",
    "receivedDt",
    "infoOnlyFlag",
    "filerIdent",
    "filerTypeCd",
    "filerName",
    "contributionInfoId",
    "contributionDt",
    "contributionAmount",
    "contributionDescr",
    "itemizeFlag",
    "travelFlag",
    "contributorPersentTypeCd",
    "contributorNameOrganization",
    "contributorNameLast",
    "contributorNameSuffixCd",
    "contributorNameFirst",
    "contributorNamePrefixCd",
    "contributorNameShort",
    "contributorStreetCity",
    "contributorStreetStateCd",
    "contributorStreetCountyCd",
    "contributorStreetCountryCd",
    "contributorStreetPostalCode",
    "contributorStreetRegion",
    "contributorEmployer",
    "contributorOccupation",
    "contributorJobTitle",
    "contributorPacFein",
    "contributorOosPacFlag",
    "contributorLawFirmName",
    "contributorSpouseLawFirmName",
    "contributorParent1LawFirmName",
    "contributorParent2LawFirmName",
)

_EXPN_COLS = (
    "recordType",
    "formTypeCd",
    "schedFormTypeCd",
    "reportInfoIdent",
    "receivedDt",
    "infoOnlyFlag",
    "filerIdent",
    "filerTypeCd",
    "filerName",
    "expendInfoId",
    "expendDt",
    "expendAmount",
    "expendDescr",
    "expendCatCd",
    "expendCatDescr",
    "itemizeFlag",
    "travelFlag",
    "politicalExpendCd",
    "reimburseIntendedFlag",
    "srcCorpContribFlag",
    "capitalLivingexpFlag",
    "payeePersentTypeCd",
    "payeeNameOrganization",
    "payeeNameLast",
    "payeeNameSuffixCd",
    "payeeNameFirst",
    "payeeNamePrefixCd",
    "payeeNameShort",
    "payeeStreetAddr1",
    "payeeStreetAddr2",
    "payeeStreetCity",
    "payeeStreetStateCd",
    "payeeStreetCountyCd",
    "payeeStreetCountryCd",
    "payeeStreetPostalCode",
    "payeeStreetRegion",
    "creditCardIssuer",
    "repaymentDt",
)


# ---------------------------------------------------------------------------
# Helpers
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
    """clean_str alias: strip, empty -> null."""
    return common.clean_str(col)


def _nullify_expr(e: pl.Expr) -> pl.Expr:
    """Strip a string expression; return null if empty."""
    s = e.cast(pl.Utf8).str.strip_chars()
    return pl.when(s.str.len_chars() > 0).then(s).otherwise(None)


# ---------------------------------------------------------------------------
# Address building
# ---------------------------------------------------------------------------


def _address_frame_rcpt(df: pl.DataFrame) -> pl.DataFrame:
    """Extract address rows from RCPT contributor columns.

    RCPT carries no street in the source, but ``_with_resolved_rcpt_street`` may have set
    ``contributorStreetAddr1`` from the omit-null match against an existing fuller address
    (so the contributor inherits that street, matching the ORM). Null when no match.
    """
    return df.select(
        [
            _cs("contributorStreetAddr1").alias("street_1"),
            pl.lit(None, dtype=pl.Utf8).alias("street_2"),
            _cs("contributorStreetCity").alias("city"),
            common.upper_str("contributorStreetStateCd").alias("state"),
            _cs("contributorStreetPostalCode").alias("zip_code"),
            _cs("contributorStreetCountryCd").alias("country"),
            _nullify_expr(pl.col("contributorStreetCountyCd").cast(pl.Utf8)).alias("county"),
        ]
    )


def _address_frame_expn(df: pl.DataFrame) -> pl.DataFrame:
    """Extract address rows from EXPN payee columns."""
    return df.select(
        [
            _cs("payeeStreetAddr1").alias("street_1"),
            _cs("payeeStreetAddr2").alias("street_2"),
            _cs("payeeStreetCity").alias("city"),
            common.upper_str("payeeStreetStateCd").alias("state"),
            _cs("payeeStreetPostalCode").alias("zip_code"),
            _cs("payeeStreetCountryCd").alias("country"),
            _nullify_expr(pl.col("payeeStreetCountyCd").cast(pl.Utf8)).alias("county"),
        ]
    )


def _address_valid(df: pl.DataFrame) -> pl.DataFrame:
    """Keep only rows where at least one key address anchor is present."""
    return df.filter(
        pl.col("street_1").is_not_null()
        | pl.col("city").is_not_null()
        | pl.col("state").is_not_null()
        | pl.col("zip_code").is_not_null()
    )


def _address_dedup(df: pl.DataFrame) -> pl.DataFrame:
    """Dedup addresses on the 4-field ORM key (mirrors _find_address_by_fields).

    The DB unique indexes key on (lower(street_1), lower(city), lower(state), zip_code)
    when street_1 IS NOT NULL, and on (lower(city), lower(state), zip_code) when
    street_1 IS NULL.  Both collapse to the same 4-field natural key:
    (street_1, city, state, zip_code).

    Deduping on the extra fields (street_2, country, county) causes a spurious
    second row when two records share the same (street_1,city,state,zip) but
    differ on street_2/country/county — then plain INSERT hits the unique index
    and fails.  Keeping only the first occurrence preserves the ORM's
    "first-found address wins" behaviour.
    """
    return (
        df.with_columns(
            [
                pl.col("street_1").cast(pl.Utf8).str.to_lowercase().alias("_k_s1"),
                pl.col("city").cast(pl.Utf8).str.to_lowercase().alias("_k_city"),
                pl.col("state").cast(pl.Utf8).str.to_lowercase().alias("_k_state"),
                pl.col("zip_code").alias("_k_zip"),
            ]
        )
        .unique(
            subset=["_k_s1", "_k_city", "_k_state", "_k_zip"],
            keep="first",
            maintain_order=True,
        )
        .drop(["_k_s1", "_k_city", "_k_state", "_k_zip"])
    )


# ---------------------------------------------------------------------------
# Persons: key helpers and frame builders
# ---------------------------------------------------------------------------

_PLACEHOLDER_NAMES_UPPER = frozenset(
    {
        "NON-ITEMIZED CONTRIBUTOR",
        "NON-ITEMIZED",
        "UNKNOWN",
        "ANONYMOUS",
    }
)


def _person_type_expr(last_col: str, first_col: str, org_col: str) -> pl.Expr:
    """Mirror ORM build_person person_type logic (priority order).

    1. last_name stripped.upper() in PLACEHOLDER_NAMES -> UNKNOWN
    2. organization is not null -> ORGANIZATION
    3. first AND last both non-null -> INDIVIDUAL
    4. else -> UNKNOWN
    """
    last_upper = pl.col(last_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    first_clean = _cs(first_col)
    org_clean = _cs(org_col)
    return (
        pl.when(last_upper.is_in(list(_PLACEHOLDER_NAMES_UPPER)))
        .then(pl.lit("UNKNOWN"))
        .when(org_clean.is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .when(first_clean.is_not_null() & _cs(last_col).is_not_null())
        .then(pl.lit("INDIVIDUAL"))
        .otherwise(pl.lit("UNKNOWN"))
    )


def _build_persons_frame_rcpt(df: pl.DataFrame) -> pl.DataFrame:
    """Build a person frame from RCPT contributors.

    Mirrors the ORM's build_person / _find_person_by_name_state key logic:
    - When contributorNameOrganization is non-null -> ORGANIZATION person, keyed on
      (lower(org), state_id) with organization set.  This matches uix_persons_org_state.
    - Otherwise -> INDIVIDUAL (or UNKNOWN), keyed on (lower(first), lower(last)).
      This matches uix_persons_name_state.

    Returns columns:
        first_name, last_name, middle_name, suffix, organization, employer,
        occupation, job_title, person_type, dedup_addr_key,
        _pk_fn, _pk_ln, _pk_org, _pk_addr, _sort_key
    """
    keyed = df.sort("contributionInfoId").with_columns(
        [
            _nullify_expr(pl.col("contributorNameOrganization").cast(pl.Utf8))
            .str.to_lowercase()
            .alias("_pk_org"),
            _cs("contributorNameFirst").str.to_lowercase().alias("_pk_fn"),
            _cs("contributorNameLast").str.to_lowercase().alias("_pk_ln"),
            # Address key uses the omit-null-resolved street (contributorStreetAddr1, set by
            # _with_resolved_rcpt_street) so a contributor that inherits an existing fuller
            # address keys on the full street — matching the ORM's dedup_addr_key. Null street
            # falls back to (city, state, zip), today's behavior.
            common.person_addr_key_expr(
                "contributorStreetAddr1",
                "contributorStreetCity",
                common.upper_str("contributorStreetStateCd"),
                "contributorStreetPostalCode",
            ).alias("_pk_addr"),
            pl.col("contributionInfoId").cast(pl.Int64).alias("_row_id"),
        ]
    )
    # Org-persons dedup on lower(org) ALONE (null fn/ln/addr) — matches uix_persons_org_state.
    keyed = common.collapse_org_person_key(keyed)

    # First occurrence per (org, fn, ln, addr): individuals now split by address so two
    # same-name people at distinct locations get distinct person rows (uix_persons_name_state).
    first = keyed.group_by(["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]).agg(pl.all().first())

    # Split orgs vs individuals (mirrors _build_persons_frame_expn pattern)
    org_first = first.filter(pl.col("_pk_org").is_not_null())
    ind_first = first.filter(pl.col("_pk_org").is_null())

    # Backfill: first non-null employer/occupation/suffix across all occurrences of the
    # SAME person (keyed by the 4-tuple incl. address so distinct-location persons don't
    # cross-contaminate; org-persons keep _pk_addr NULL so they still aggregate as one).
    _pk = ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]
    emp_nn = (
        keyed.filter(
            pl.col("contributorEmployer").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0
        )
        .group_by(_pk)
        .agg(pl.first("contributorEmployer").alias("emp_nn"))
    )
    occ_nn = (
        keyed.filter(
            pl.col("contributorOccupation").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0
        )
        .group_by(_pk)
        .agg(pl.first("contributorOccupation").alias("occ_nn"))
    )
    sfx_nn = (
        keyed.filter(
            pl.col("contributorNameSuffixCd").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0
        )
        .group_by(_pk)
        .agg(pl.first("contributorNameSuffixCd").alias("sfx_nn"))
    )

    parts = [p for p in (org_first, ind_first) if p.height > 0]
    if not parts:
        return pl.DataFrame(
            schema={
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "middle_name": pl.Utf8,
                "suffix": pl.Utf8,
                "organization": pl.Utf8,
                "employer": pl.Utf8,
                "occupation": pl.Utf8,
                "job_title": pl.Utf8,
                "person_type": pl.Utf8,
                "dedup_addr_key": pl.Utf8,
                "_pk_fn": pl.Utf8,
                "_pk_ln": pl.Utf8,
                "_pk_org": pl.Utf8,
                "_pk_addr": pl.Utf8,
                "_sort_key": pl.Int64,
            }
        )

    combined = pl.concat(parts, how="diagonal_relaxed")

    result = (
        combined.join(emp_nn, on=_pk, how="left")
        .join(occ_nn, on=_pk, how="left")
        .join(sfx_nn, on=_pk, how="left")
        .with_columns(
            [
                _cs("contributorNameFirst").alias("first_name"),
                _cs("contributorNameLast").alias("last_name"),
                pl.lit(None, dtype=pl.Utf8).alias("middle_name"),
                pl.coalesce(
                    [
                        pl.col("sfx_nn")
                        .cast(pl.Utf8)
                        .str.strip_chars()
                        .replace("", None),  # first non-null suffix
                        _cs("contributorNameSuffixCd"),
                    ]
                ).alias("suffix"),
                # organization: set from source column for org contributors; null for individuals
                _cs("contributorNameOrganization").alias("organization"),
                pl.coalesce(
                    [
                        pl.col("emp_nn").cast(pl.Utf8).str.strip_chars().replace("", None),
                        _cs("contributorEmployer"),
                    ]
                ).alias("employer"),
                pl.coalesce(
                    [
                        pl.col("occ_nn").cast(pl.Utf8).str.strip_chars().replace("", None),
                        _cs("contributorOccupation"),
                    ]
                ).alias("occupation"),
                pl.lit(None, dtype=pl.Utf8).alias("job_title"),
                _person_type_expr(
                    "contributorNameLast", "contributorNameFirst", "contributorNameOrganization"
                ).alias("person_type"),
                # Persisted dedup key column (NULL for org-persons via collapse).
                pl.col("_pk_addr").alias("dedup_addr_key"),
                pl.col("_row_id").alias("_sort_key"),
            ]
        )
        .select(
            [
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
                "_pk_fn",
                "_pk_ln",
                "_pk_org",
                "_pk_addr",
                "_sort_key",
            ]
        )
    )
    return result


def _build_persons_frame_expn(
    df: pl.DataFrame,
    rcpt_person_set: set[tuple],
    sort_key_offset: int = 0,
) -> pl.DataFrame:
    """Build a person frame from EXPN payees not already in rcpt_person_set.

    ``sort_key_offset`` is added to expendInfoId-derived sort keys so EXPN
    persons sort after all RCPT persons (mirrors priority 10 < 11 load order).
    """
    keyed = df.sort("expendInfoId").with_columns(
        [
            _nullify_expr(pl.col("payeeNameOrganization").cast(pl.Utf8))
            .str.to_lowercase()
            .alias("_pk_org"),
            _cs("payeeNameFirst").str.to_lowercase().alias("_pk_fn"),
            _cs("payeeNameLast").str.to_lowercase().alias("_pk_ln"),
            # EXPN carries street_1 — full (street, city, state, zip) address key.
            common.person_addr_key_expr(
                "payeeStreetAddr1",
                "payeeStreetCity",
                common.upper_str("payeeStreetStateCd"),
                "payeeStreetPostalCode",
            ).alias("_pk_addr"),
            (pl.col("expendInfoId").cast(pl.Int64) + sort_key_offset).alias("_row_id"),
        ]
    )
    # Org-persons dedup on lower(org) ALONE (null fn/ln/addr) — matches uix_persons_org_state.
    keyed = common.collapse_org_person_key(keyed)
    _pk = ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]
    first = keyed.group_by(_pk).agg(pl.all().first())

    # Split orgs vs individuals
    org_first = first.filter(pl.col("_pk_org").is_not_null())
    ind_first = first.filter(pl.col("_pk_org").is_null())

    # Exclude individuals already created by RCPT (keyed on name + address, so the same
    # name at a different address is a NEW person, matching uix_persons_name_state).
    if rcpt_person_set:
        ind_rows = ind_first.to_dicts()
        ind_new_rows = [
            r
            for r in ind_rows
            if (r.get("_pk_fn"), r.get("_pk_ln"), r.get("_pk_addr")) not in rcpt_person_set
        ]
        ind_new = (
            pl.DataFrame(ind_new_rows, schema=ind_first.schema)
            if ind_new_rows
            else ind_first.clear()
        )
    else:
        ind_new = ind_first

    parts = [p for p in (org_first, ind_new) if p.height > 0]
    if not parts:
        return pl.DataFrame(
            schema={
                "first_name": pl.Utf8,
                "last_name": pl.Utf8,
                "middle_name": pl.Utf8,
                "suffix": pl.Utf8,
                "organization": pl.Utf8,
                "employer": pl.Utf8,
                "occupation": pl.Utf8,
                "job_title": pl.Utf8,
                "person_type": pl.Utf8,
                "dedup_addr_key": pl.Utf8,
                "_pk_fn": pl.Utf8,
                "_pk_ln": pl.Utf8,
                "_pk_org": pl.Utf8,
                "_pk_addr": pl.Utf8,
                "_sort_key": pl.Int64,
            }
        )

    combined = pl.concat(parts, how="diagonal_relaxed")

    # Backfill suffix (per same person — 4-tuple incl. address).
    sfx_nn = (
        keyed.filter(
            pl.col("payeeNameSuffixCd").cast(pl.Utf8).str.strip_chars().str.len_chars() > 0
        )
        .group_by(_pk)
        .agg(pl.first("payeeNameSuffixCd").alias("sfx_nn"))
    )

    result = (
        combined.join(sfx_nn, on=_pk, how="left")
        .with_columns(
            [
                _cs("payeeNameFirst").alias("first_name"),
                _cs("payeeNameLast").alias("last_name"),
                pl.lit(None, dtype=pl.Utf8).alias("middle_name"),
                pl.coalesce(
                    [
                        pl.col("sfx_nn").cast(pl.Utf8).str.strip_chars().replace("", None),
                        _cs("payeeNameSuffixCd"),
                    ]
                ).alias("suffix"),
                _cs("payeeNameOrganization").alias("organization"),
                pl.lit(None, dtype=pl.Utf8).alias("employer"),
                pl.lit(None, dtype=pl.Utf8).alias("occupation"),
                pl.lit(None, dtype=pl.Utf8).alias("job_title"),
                _person_type_expr("payeeNameLast", "payeeNameFirst", "payeeNameOrganization").alias(
                    "person_type"
                ),
                pl.col("_pk_addr").alias("dedup_addr_key"),
                pl.col("_row_id").alias("_sort_key"),
            ]
        )
        .select(
            [
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
                "_pk_fn",
                "_pk_ln",
                "_pk_org",
                "_pk_addr",
                "_sort_key",
            ]
        )
    )
    return result


# ---------------------------------------------------------------------------
# Entity building
# ---------------------------------------------------------------------------


def _build_entities_from_persons(persons_df: pl.DataFrame) -> pl.DataFrame:
    """Build entity rows from a persons frame.

    Includes _sort_key for downstream entity dedup ordering.

    Rules (mirrors _get_or_create_entity + build_person):
    - ORGANIZATION person -> ORGANIZATION entity; name = organization
    - INDIVIDUAL/UNKNOWN person -> PERSON entity; name = full_name
    """
    full_name = common.full_name_expr(
        "first_name", "middle_name", "last_name", "suffix", "organization"
    )

    entity_type = (
        pl.when(pl.col("organization").is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .otherwise(pl.lit("PERSON"))
    )

    # name: org if org else full_name
    entity_name = (
        pl.when(pl.col("organization").is_not_null())
        .then(pl.col("organization"))
        .otherwise(full_name)
    )

    df = persons_df.with_columns(
        [
            entity_type.alias("entity_type"),
            entity_name.alias("name"),
        ]
    )

    # normalized_name: normalize the name field
    df = df.with_columns(
        [
            pl.col("name")
            .cast(pl.Utf8)
            .str.strip_chars()
            .str.to_lowercase()
            .str.replace_all(r"[^a-z0-9]+", " ")
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .fill_null("")
            .alias("normalized_name"),
            pl.lit(None, dtype=pl.Utf8).alias("committee_id"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
        ]
    )

    return df.select(
        [
            "entity_type",
            "name",
            "normalized_name",
            "committee_id",
            "notes",
            "_sort_key",
        ]
    )


def _build_committee_entity(committee_df: pl.DataFrame) -> pl.DataFrame:
    """Build the committee entity rows (one per committee)."""
    return committee_df.with_columns(
        [
            pl.lit("COMMITTEE").alias("entity_type"),
            pl.col("name").alias("name"),
            common.normalize_entity_name("name").alias("normalized_name"),
            pl.col("filer_id").alias("committee_id"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
            pl.lit(0, dtype=pl.Int64).alias("_sort_key"),  # committees sort first
        ]
    ).select(["entity_type", "name", "normalized_name", "committee_id", "notes", "_sort_key"])


# ---------------------------------------------------------------------------
# Committee building
# ---------------------------------------------------------------------------


def _build_committee_frame(rcpt: pl.DataFrame | None, expn: pl.DataFrame | None) -> pl.DataFrame:
    """Build committee rows from inline filerIdent/filerName/filerTypeCd data.

    One row per unique filerIdent. Name = first filerName seen (RCPT before EXPN).
    filer_status = None (only populated by FILER records, not inline txn data).
    """
    frames: list[pl.DataFrame] = []
    for df in (rcpt, expn):
        if df is not None and df.height > 0:
            part = df.select(
                [
                    _cs("filerIdent").alias("filer_id"),
                    _cs("filerName").alias("name"),
                    _cs("filerTypeCd").alias("committee_type"),
                ]
            ).filter(pl.col("filer_id").is_not_null())
            frames.append(part)

    if not frames:
        return pl.DataFrame(
            schema={
                "filer_id": pl.Utf8,
                "name": pl.Utf8,
                "committee_type": pl.Utf8,
                "filer_status": pl.Utf8,
            }
        )

    combined = pl.concat(frames, how="diagonal_relaxed")
    # RCPT first -> first filerName wins
    deduped = combined.unique(subset=["filer_id"], keep="first", maintain_order=True)
    return deduped.with_columns(pl.lit(None, dtype=pl.Utf8).alias("filer_status"))


# ---------------------------------------------------------------------------
# Contributions and expenditures
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Addresses
# ---------------------------------------------------------------------------


def _has_name(first_col: str, last_col: str, org_col: str) -> pl.Expr:
    """Return True for rows where the ORM's build_person would produce a person
    (i.e. name.full_name is non-empty).  Mirrors PersonName.full_name logic:
    full_name is non-empty when org is non-null OR at least one of first/last
    is non-null.  Filters the row BEFORE address-taking (M3).
    """
    org = _cs(org_col)
    first = _cs(first_col)
    last = _cs(last_col)
    return org.is_not_null() | first.is_not_null() | last.is_not_null()


def _build_addresses(
    rcpt: pl.DataFrame | None,
    expn: pl.DataFrame | None,
) -> pl.DataFrame:
    """Return the unique set of addresses that the ORM would create.

    The ORM creates one address per PERSON on their first occurrence, then
    dedupes addresses case-insensitively. RCPT loads first (priority 10 < 11):
    persons seen in RCPT suppress address creation in EXPN.

    Algorithm:
      1. For each unique person (lower-case key) in RCPT, take their first address.
         Org contributors key on (lower(org)), individuals on
         (lower(first), lower(last), addr_key) — so a same-name person at two
         addresses is two persons contributing both addresses (matching the ORM,
         which now builds an address per name+address person).
      2. For EXPN persons NOT already seen in RCPT, take their first address.
      3. Filter rows where the person has no name (M3: ORM returns None -> no address).
      4. Dedup addresses by lower-case 4-field key (M1: matches uix_addresses_* indexes).
    """
    frames: list[pl.DataFrame] = []
    rcpt_person_set: set[tuple] = set()

    if rcpt is not None and rcpt.height > 0:
        # M2: 4-key grouping (org, fn, ln, addr) matching _build_persons_frame_rcpt.
        # M3: filter rows with no name before taking addresses.
        rcpt_keyed = (
            rcpt.sort("contributionInfoId")
            .filter(
                _has_name(
                    "contributorNameFirst", "contributorNameLast", "contributorNameOrganization"
                )
            )
            .with_columns(
                [
                    _nullify_expr(pl.col("contributorNameOrganization").cast(pl.Utf8))
                    .str.to_lowercase()
                    .alias("_pk_org"),
                    _cs("contributorNameFirst").str.to_lowercase().alias("_pk_fn"),
                    _cs("contributorNameLast").str.to_lowercase().alias("_pk_ln"),
                    common.person_addr_key_expr(
                        "contributorStreetAddr1",
                        "contributorStreetCity",
                        common.upper_str("contributorStreetStateCd"),
                        "contributorStreetPostalCode",
                    ).alias("_pk_addr"),
                ]
            )
        )
        rcpt_keyed = common.collapse_org_person_key(rcpt_keyed)
        rcpt_pk = ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]
        rcpt_first = rcpt_keyed.group_by(rcpt_pk).agg(pl.all().first())

        # Split to collect individual key set for EXPN suppression
        rcpt_org_first = rcpt_first.filter(pl.col("_pk_org").is_not_null())
        rcpt_ind_first = rcpt_first.filter(pl.col("_pk_org").is_null())
        rcpt_person_set = set(
            zip(
                rcpt_ind_first["_pk_fn"].to_list(),
                rcpt_ind_first["_pk_ln"].to_list(),
                rcpt_ind_first["_pk_addr"].to_list(),
            )
        )

        for part in (rcpt_org_first, rcpt_ind_first):
            if part.height > 0:
                frames.append(_address_valid(_address_frame_rcpt(part)))

    if expn is not None and expn.height > 0:
        # M3: filter rows with no name before taking addresses.
        expn_keyed = (
            expn.sort("expendInfoId")
            .filter(_has_name("payeeNameFirst", "payeeNameLast", "payeeNameOrganization"))
            .with_columns(
                [
                    _nullify_expr(pl.col("payeeNameOrganization").cast(pl.Utf8))
                    .str.to_lowercase()
                    .alias("_pk_org"),
                    _cs("payeeNameFirst").str.to_lowercase().alias("_pk_fn"),
                    _cs("payeeNameLast").str.to_lowercase().alias("_pk_ln"),
                    common.person_addr_key_expr(
                        "payeeStreetAddr1",
                        "payeeStreetCity",
                        common.upper_str("payeeStreetStateCd"),
                        "payeeStreetPostalCode",
                    ).alias("_pk_addr"),
                ]
            )
        )
        expn_keyed = common.collapse_org_person_key(expn_keyed)
        expn_pk = ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]
        expn_first = expn_keyed.group_by(expn_pk).agg(pl.all().first())

        org_first = expn_first.filter(pl.col("_pk_org").is_not_null())
        ind_first = expn_first.filter(pl.col("_pk_org").is_null())

        if rcpt_person_set:
            ind_rows = ind_first.to_dicts()
            ind_new_rows = [
                r
                for r in ind_rows
                if (r.get("_pk_fn"), r.get("_pk_ln"), r.get("_pk_addr")) not in rcpt_person_set
            ]
            ind_new = (
                pl.DataFrame(ind_new_rows, schema=ind_first.schema)
                if ind_new_rows
                else ind_first.clear()
            )
        else:
            ind_new = ind_first

        for part in (org_first, ind_new):
            if part.height > 0:
                frames.append(_address_valid(_address_frame_expn(part)))

    if not frames:
        return pl.DataFrame(
            schema={
                "street_1": pl.Utf8,
                "street_2": pl.Utf8,
                "city": pl.Utf8,
                "state": pl.Utf8,
                "zip_code": pl.Utf8,
                "country": pl.Utf8,
                "county": pl.Utf8,
            }
        )

    return _address_dedup(pl.concat(frames, how="diagonal_relaxed"))


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class FlatTxnsDimsWorker:
    """Builds dim tables from RCPT/EXPN files ahead of flat_txns (priority 9 < 10)."""

    record_types = frozenset({"RCPT", "EXPN"})
    priority = 9  # must run before FlatTxnsWorker (priority=10)

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        rcpt = _read(files_by_type.get("RCPT", []))
        expn = _read(files_by_type.get("EXPN", []))

        # Omit-null address match: build the lookup ONCE from addresses already in the DB
        # (the FILER family, priority 0, has written committee addresses), then let each
        # street-less party inherit a fuller existing address's street — exactly as the ORM's
        # _find_address_by_fields does. Done before any dim is built so the resolved street
        # feeds both the person dedup key and the address frame. RCPT contributors carry no
        # source street (out_col is a new column); EXPN payees keep their own street and only
        # the street-less ones inherit (out_col overwrites payeeStreetAddr1 in place).
        addr_lookup = ctx.get_address_lookup()
        if rcpt is not None:
            rcpt = _ensure_cols(rcpt, _RCPT_COLS)
            rcpt = common.add_resolved_street(
                rcpt,
                addr_lookup,
                city_col="contributorStreetCity",
                state_col="contributorStreetStateCd",
                zip_col="contributorStreetPostalCode",
                out_col="contributorStreetAddr1",
            )
        if expn is not None:
            expn = _ensure_cols(expn, _EXPN_COLS)
            expn = common.add_resolved_street(
                expn,
                addr_lookup,
                city_col="payeeStreetCity",
                state_col="payeeStreetStateCd",
                zip_col="payeeStreetPostalCode",
                out_col="payeeStreetAddr1",
                own_s1_col="payeeStreetAddr1",
            )

        counts: dict[str, int] = {}

        # 1. Addresses
        #
        # FILER (priority 0) now creates committee/officer addresses, persons, and
        # entities BEFORE this worker runs. This family was originally the first dim
        # creator and wrote with conflict_cols=None and NO anti-join, so it now
        # re-inserts rows another family already created -> uix_addresses_full (and
        # uix_persons_* / uix_entities_*) violations on Postgres. Anti-join existing DB
        # rows on each table's partial-unique-index key (case-insensitive, matching the
        # index semantics) before inserting, EXACTLY like detail_children._write_dims.
        addr_df = _build_addresses(rcpt, expn)
        addr_new = self._anti_join_addresses(addr_df, ctx)
        counts["addresses"] = common.write_frame(
            ctx.session,
            UnifiedAddress,
            addr_new,
            conflict_cols=None,
        )

        # 2. Persons
        persons_df, rcpt_person_set = self._build_all_persons(rcpt, expn, ctx)
        persons_new = self._anti_join_persons(persons_df, ctx)
        counts["persons"] = common.write_frame(
            ctx.session,
            UnifiedPerson,
            persons_new.select(
                [
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
                    "state_id",
                ]
            ),
            conflict_cols=None,
        )

        # 3. Entities (from persons + committee)
        entity_df = self._build_all_entities(persons_df, rcpt, expn, ctx)
        entity_new = self._anti_join_entities(entity_df, ctx)
        counts["entities"] = common.write_frame(
            ctx.session,
            UnifiedEntity,
            entity_new.select(
                [
                    "entity_type",
                    "name",
                    "normalized_name",
                    "committee_id",
                    "notes",
                    "state_id",
                ]
            ),
            conflict_cols=["entity_type", "normalized_name", "state_id"],
            update_cols=[],
            conflict_where="state_id IS NOT NULL",
        )

        # 4. Committees
        comm_df = _build_committee_frame(rcpt, expn)
        if comm_df.height > 0:
            # DO NOTHING on conflict (update_cols=[]): a committee already created by the
            # FILER family (authoritative name/type/status/address) must NOT be clobbered
            # by an incidental transaction filerName. FILER runs first (priority 0); this
            # mirrors the ORM's find-or-create first-occurrence-wins.
            counts["committees"] = common.write_frame(
                ctx.session,
                UnifiedCommittee,
                comm_df.with_columns(pl.lit(ctx.state_id).alias("state_id")),
                conflict_cols=["filer_id"],
                update_cols=[],
            )
        else:
            counts["committees"] = 0

        # DEFERRED — contributions / expenditures / transaction_persons are NOT
        # written here. Their contributor/payee/payer/person/transaction FKs are
        # surrogate ids that the equivalence harness DROPS, so writing them now would
        # require placeholder ids that pass the gate while producing broken linkage.
        # They belong to a dedicated slice that (a) does real id-map joins (insert
        # dims -> read ids back by natural key -> join) and (b) adds FK->natural-key
        # resolution to the harness so the linkage is actually verified.

        loaded = sum(counts.values())
        _logger.info(f"[vectorized.flat_txns_dims] loaded {loaded} dim rows: {counts}")
        return {"loaded": loaded, **counts}

    # ---- anti-joins against existing DB rows (shared dims) --------------
    #
    # Keyed identically to detail_children's id-maps so the anti-join matches each
    # partial unique index's semantics case-insensitively:
    #   addresses -> (lower street_1/city/state, zip); null-street rows match on NULL
    #                via join_nulls=True (the no-street partition).
    #   persons   -> the post-#48 (_pk_org, _pk_fn, _pk_ln, _pk_addr) key.
    #   entities  -> (entity_type, normalized_name).

    @staticmethod
    def _anti_join_addresses(addr_df: pl.DataFrame, ctx: FamilyContext) -> pl.DataFrame:
        if addr_df.height == 0:
            return addr_df
        existing = _address_id_map(ctx.engine)
        keyed = addr_df.with_columns(
            pl.col("street_1").cast(pl.Utf8).str.to_lowercase().alias("_k_s1"),
            pl.col("city").cast(pl.Utf8).str.to_lowercase().alias("_k_city"),
            pl.col("state").cast(pl.Utf8).str.to_lowercase().alias("_k_state"),
            pl.col("zip_code").alias("_k_zip"),
        )
        return keyed.join(
            existing.select("_k_s1", "_k_city", "_k_state", "_k_zip"),
            on=["_k_s1", "_k_city", "_k_state", "_k_zip"],
            how="anti",
            join_nulls=True,
        ).drop("_k_s1", "_k_city", "_k_state", "_k_zip")

    @staticmethod
    def _anti_join_persons(persons_df: pl.DataFrame, ctx: FamilyContext) -> pl.DataFrame:
        if persons_df.height == 0:
            return persons_df
        existing = _person_id_map(ctx.engine, ctx.state_id)
        return persons_df.join(
            existing.select("_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"),
            on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
            how="anti",
            join_nulls=True,
        )

    @staticmethod
    def _anti_join_entities(entity_df: pl.DataFrame, ctx: FamilyContext) -> pl.DataFrame:
        if entity_df.height == 0:
            return entity_df
        existing = _entity_id_map(ctx.engine, ctx.state_id)
        return entity_df.join(
            existing.select("entity_type", "normalized_name"),
            on=["entity_type", "normalized_name"],
            how="anti",
        )

    def _build_all_persons(
        self,
        rcpt: pl.DataFrame | None,
        expn: pl.DataFrame | None,
        ctx: FamilyContext,
    ) -> tuple[pl.DataFrame, set[tuple]]:
        """Build unified persons frame for both files; return RCPT person key set."""
        frames: list[pl.DataFrame] = []
        rcpt_person_set: set[tuple] = set()

        if rcpt is not None and rcpt.height > 0:
            rcpt_persons = _build_persons_frame_rcpt(rcpt)
            # M3: filter rows where full_name is empty — mirrors ORM build_person returning
            # None when name.full_name is "" (no org, no first+last+suffix).
            full_name_col = common.full_name_expr(
                "first_name", "middle_name", "last_name", "suffix", "organization"
            )
            rcpt_persons = rcpt_persons.filter(full_name_col.str.len_chars() > 0)
            # Keyed on (fn, ln, addr) so EXPN suppression splits same-name people by
            # address (matching uix_persons_name_state).
            rcpt_person_set = set(
                zip(
                    rcpt_persons["_pk_fn"].to_list(),
                    rcpt_persons["_pk_ln"].to_list(),
                    rcpt_persons["_pk_addr"].to_list(),
                )
            )
            frames.append(rcpt_persons)

        if expn is not None and expn.height > 0:
            # Sort key offset: EXPN IDs overlap with RCPT IDs numerically, so add a
            # large offset to ensure EXPN persons always sort after RCPT persons in
            # entity dedup (RCPT loads first -> their entity names win when normalized
            # names collide, matching ORM load order).
            rcpt_max_id = (
                rcpt.select(pl.col("contributionInfoId").cast(pl.Int64).max())[
                    "contributionInfoId"
                ].item()
                if rcpt is not None
                else 0
            ) or 0
            expn_persons = _build_persons_frame_expn(
                expn, rcpt_person_set, sort_key_offset=rcpt_max_id + 10_000_000
            )
            if expn_persons.height > 0:
                # M3: filter rows where full_name is empty (mirrors ORM build_person -> None).
                full_name_col_expn = common.full_name_expr(
                    "first_name", "middle_name", "last_name", "suffix", "organization"
                )
                expn_persons = expn_persons.filter(full_name_col_expn.str.len_chars() > 0)
            if expn_persons.height > 0:
                frames.append(expn_persons)

        if not frames:
            return (
                pl.DataFrame(
                    schema={
                        "first_name": pl.Utf8,
                        "last_name": pl.Utf8,
                        "middle_name": pl.Utf8,
                        "suffix": pl.Utf8,
                        "organization": pl.Utf8,
                        "employer": pl.Utf8,
                        "occupation": pl.Utf8,
                        "job_title": pl.Utf8,
                        "person_type": pl.Utf8,
                        "dedup_addr_key": pl.Utf8,
                        "_pk_fn": pl.Utf8,
                        "_pk_ln": pl.Utf8,
                        "_pk_org": pl.Utf8,
                        "_pk_addr": pl.Utf8,
                        "_sort_key": pl.Int64,
                        "state_id": pl.Int64,
                    }
                ),
                rcpt_person_set,
            )

        all_persons = pl.concat(frames, how="diagonal_relaxed")
        all_persons = all_persons.with_columns(pl.lit(ctx.state_id).alias("state_id"))
        return all_persons, rcpt_person_set

    def _build_all_entities(
        self,
        persons_df: pl.DataFrame,
        rcpt: pl.DataFrame | None,
        expn: pl.DataFrame | None,
        ctx: FamilyContext,
    ) -> pl.DataFrame:
        """Build entity rows from person-entities + committee-entity.

        Dedup on (entity_type, normalized_name) in first-occurrence order (sort by
        _sort_key before unique) so that the first-seen entity name wins — matching
        the ORM's _get_or_create_entity lookup which returns the first-inserted row.
        """
        frames: list[pl.DataFrame] = []

        if persons_df.height > 0:
            person_entities = _build_entities_from_persons(persons_df)
            frames.append(person_entities)

        # Committee entity sorts first (sort_key=0 < any row id)
        comm_df = _build_committee_frame(rcpt, expn)
        if comm_df.height > 0:
            frames.append(_build_committee_entity(comm_df))

        if not frames:
            return pl.DataFrame(
                schema={
                    "entity_type": pl.Utf8,
                    "name": pl.Utf8,
                    "normalized_name": pl.Utf8,
                    "committee_id": pl.Utf8,
                    "notes": pl.Utf8,
                    "state_id": pl.Int64,
                }
            )

        all_entities = pl.concat(frames, how="diagonal_relaxed")

        # Sort by _sort_key (ORM insertion order), then dedup on
        # (entity_type, normalized_name) — first-seen name wins.
        all_entities = all_entities.sort("_sort_key").unique(
            subset=["entity_type", "normalized_name"],
            keep="first",
            maintain_order=True,
        )
        return all_entities.with_columns(pl.lit(ctx.state_id).alias("state_id"))


register(FlatTxnsDimsWorker())
