"""Shared primitives for the vectorized (Polars) ingest engine.

Pure-Polars column expressions that reproduce the ORM parse helpers on **real
TEC-shaped inputs** (currency strings, yyyymmdd / sep dates), plus a dialect-safe bulk
write helper. The equivalence harness (`app/core/ingest_equivalence.py`) is the gate:
divergence on real data is a bug. HARD RULE: no ``map_elements`` / ``apply`` (per-row
Python UDF) — everything here is a native expression so it runs in Polars' Rust engine.

Bounded, documented divergences from the ORM on inputs that do NOT occur in TEC data
(safe; see per-function docstrings): ``tec_amount`` rejects scientific notation /
Infinity / NaN that ``Decimal()`` would accept; ``tec_date`` returns null (not raise)
on invalid 8-digit dates where the ORM would raise and reject the row.

Two parse dialects exist in the ORM and must be kept distinct:
- ``tec_*`` mirrors ``app/core/source_models/reports_ingest.py`` (`_parse_amount`
  strips only ``,``/``$``; `_parse_date` handles 8-digit yyyymmdd + a>31/c>31 sep
  heuristic, NO year guard).
- ``builder_*`` mirrors ``app/core/builders.py`` (`_parse_amount` regex-strips
  ``[^\\d.-]``; `_parse_date` tries a format list with a 1900-2100 year guard).
"""

from __future__ import annotations

from typing import Any

import polars as pl


def clean_str(col: str) -> pl.Expr:
    """Strip; empty -> null. Mirrors ``_optional_str``."""
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    return pl.when(s.str.len_chars() > 0).then(s).otherwise(None)


# ── amounts ──────────────────────────────────────────────────────────────────
#: A string Python's Decimal() accepts in TEC data: optional sign, digits with an
#: optional fractional part, or a leading-dot fraction. Guards against Polars'
#: lenient cast (e.g. "." -> 0) so we match Decimal()'s ValueError -> None.
_DECIMAL_RE = r"^-?\d+(\.\d*)?$|^-?\.\d+$"


def tec_amount(col: str) -> pl.Expr:
    """Mirror reports_ingest._parse_amount: strip, remove ',' and '$', Decimal else null.

    Bounded divergence (safe — absent from TEC currency fields): scientific notation
    ("1e5"), "Infinity", and "NaN" — which ``Decimal()`` accepts — are rejected to null
    by the ``_DECIMAL_RE`` guard (which is also what stops Polars casting "." to 0).
    """
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    cleaned = s.str.replace_all(",", "", literal=True).str.replace_all("$", "", literal=True)
    return (
        pl.when((s.str.len_chars() > 0) & cleaned.str.contains(_DECIMAL_RE))
        .then(cleaned.cast(pl.Decimal(38, 4), strict=False))
        .otherwise(None)
    )


def builder_amount(col: str) -> pl.Expr:
    """Mirror builders._parse_amount: strip everything except [0-9.-], then Decimal."""
    s = pl.col(col).cast(pl.Utf8).str.replace_all(r"[^\d.-]", "")
    cleaned = (
        pl.when(s.is_in(["", ".", "-", "-.", "."]).not_() & s.is_not_null()).then(s).otherwise(None)
    )
    return cleaned.cast(pl.Decimal(38, 4), strict=False)


# ── dates ────────────────────────────────────────────────────────────────────
_TEC_FALLBACK_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d")


def tec_date(col: str) -> pl.Expr:
    """Mirror reports_ingest._parse_date.

    8-digit yyyymmdd first; otherwise a separator split where a>31 => (a,b,c) and
    c>31 => (c,a,b). No 1900-2100 guard (matches the source).

    Bounded divergence: an INVALID 8-digit date (e.g. "20010229", month/day out of
    range) returns null here, whereas the ORM ``_parse_date`` calls ``date(...)``
    unguarded and RAISES — which rejects that row at ingest. Callers that require a
    mandatory date should drop null-date rows so behavior matches the ORM reject.
    """
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    eight = (s.str.len_chars() == 8) & s.str.contains(r"^\d{8}$")
    d8 = s.str.to_date("%Y%m%d", strict=False)
    # Separator forms: try ISO (a>31) then US (c>31). Polars to_date handles validity.
    iso = s.str.to_date("%Y-%m-%d", strict=False).fill_null(s.str.to_date("%Y/%m/%d", strict=False))
    us = s.str.to_date("%m/%d/%Y", strict=False).fill_null(s.str.to_date("%m-%d-%Y", strict=False))
    return (
        pl.when(s.is_null() | (s.str.len_chars() == 0))
        .then(None)
        .when(eight)
        .then(d8)
        .otherwise(iso.fill_null(us))
    )


def builder_date(col: str) -> pl.Expr:
    """Mirror builders._parse_date: format list (yyyymmdd first), 1900-2100 guard."""
    s = pl.col(col).cast(pl.Utf8).str.strip_chars()
    parsed = s.str.to_date("%Y%m%d", strict=False)
    for fmt in _TEC_FALLBACK_FORMATS:
        parsed = parsed.fill_null(s.str.to_date(fmt, strict=False))
    parsed = parsed.fill_null(s.str.to_datetime("%m/%d/%Y %H:%M:%S", strict=False).dt.date())
    parsed = parsed.fill_null(s.str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False).dt.date())
    return pl.when(parsed.dt.year().is_between(1900, 2100)).then(parsed).otherwise(None)


# ── booleans ─────────────────────────────────────────────────────────────────
def bool_expr(col: str) -> pl.Expr:
    """Mirror builders._parse_boolean: true set {true,yes,y,1,t}; null/other -> False."""
    s = pl.col(col).cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    return s.is_in(["true", "yes", "y", "1", "t"]).fill_null(False)


# ── provenance ───────────────────────────────────────────────────────────────
def raw_json_expr(columns: list[str], alias: str = "raw_data") -> pl.Expr:
    """JSON-encode the original columns as provenance. Compared structurally (not
    byte-exact) by the harness, since json.dumps and Polars differ on separators."""
    return pl.struct([pl.col(c) for c in columns]).struct.json_encode().alias(alias)


# ── writes ───────────────────────────────────────────────────────────────────
def _inject_auto_columns(model: type, rows: list[dict[str, Any]]) -> None:
    """Fill ``uuid`` / ``created_at`` / ``updated_at`` that the ORM sets via
    ``default_factory`` — those Python defaults do NOT fire on a core bulk insert.
    Values are non-deterministic but are dropped by the equivalence harness
    (volatile columns); we only need them non-null to satisfy NOT NULL/unique."""
    import uuid as _uuid
    from datetime import datetime, timezone

    cols = set(model.__table__.columns.keys())  # type: ignore[attr-defined]
    now = datetime.now(timezone.utc)
    for r in rows:
        if "uuid" in cols and r.get("uuid") is None:
            r["uuid"] = str(_uuid.uuid4())
        if "created_at" in cols and r.get("created_at") is None:
            r["created_at"] = now
        if "updated_at" in cols and r.get("updated_at") is None:
            r["updated_at"] = now


def write_frame(
    session: Any,
    model: type,
    frame: pl.DataFrame,
    *,
    conflict_cols: list[str] | None,
    update_cols: list[str] | None = None,
) -> int:
    """Bulk-write a Polars frame into ``model``'s table (dialect-safe).

    With ``conflict_cols`` -> upsert via ``app.core.upsert.bulk_upsert``; with
    ``conflict_cols=None`` -> plain core insert (for tables without a natural unique
    key). Auto-fills uuid/timestamps the ORM would set. Correctness-first; the
    Postgres COPY fast-path is a separate perf follow-up.
    """
    from sqlalchemy import insert

    from app.core.upsert import bulk_upsert

    if frame.is_empty():
        return 0
    rows = frame.to_dicts()
    _inject_auto_columns(model, rows)
    if conflict_cols:
        return bulk_upsert(
            session, model, rows, conflict_cols=conflict_cols, update_cols=update_cols
        )
    # Plain insert: commit explicitly (bulk_upsert commits internally; the dispatcher
    # closes the session in finally with no commit, which would otherwise roll this back).
    session.execute(insert(model.__table__), rows)  # type: ignore[attr-defined]
    session.commit()
    return len(rows)
