"""Pure Polars expression helpers for the detail_children family.

All helpers are stateless functions that operate on DataFrames or return Exprs.
Nothing in this module performs I/O or holds mutable state.
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import polars as pl

from app.core.ingest_vectorized import common

from .specs import _BASE_COLS, _PLACEHOLDER_NAMES_UPPER, TypeSpec

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
    joined = pl.concat_str([first, last, suffix], separator=" ", ignore_nulls=True)
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
            common.person_addr_key_expr(pl.lit(None, dtype=pl.Utf8), city, state, zip_code).alias(
                "_pk_addr"
            ),
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
# All id-map helpers live in app.core.ingest_vectorized.id_maps.
# Private aliases imported at the top of _legacy_worker.py keep callers unchanged.


def _norm_name(value: pl.Expr) -> pl.Expr:
    """value_objects.normalize_entity_name as a column expression."""
    s = value.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    s = s.str.replace_all(r"[^a-z0-9]+", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return s.fill_null("")


# ---------------------------------------------------------------------------
# Module-level expression helpers (used by detail builders in _legacy_worker)
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
