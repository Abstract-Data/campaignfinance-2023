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

import enum
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


# ── dim normalization (persons / entities / addresses) ───────────────────────
def normalize_entity_name_expr(name_expr: pl.Expr) -> pl.Expr:
    """``normalize_entity_name`` applied to an arbitrary string EXPRESSION (not a column
    name). Mirror value_objects.normalize_entity_name: strip -> lower -> non-alnum to single
    spaces -> collapse spaces -> strip. Null/empty -> "" (the ORM returns "")."""
    s = name_expr.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    s = s.str.replace_all(r"[^a-z0-9]+", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return s.fill_null("")


def normalize_entity_name(col: str) -> pl.Expr:
    """Mirror value_objects.normalize_entity_name on a COLUMN by name. Null/empty -> "".
    This is the entity dedup key (uix_entities_type_name_state)."""
    return normalize_entity_name_expr(pl.col(col))


def full_name_expr(first: str, middle: str, last: str, suffix: str, organization: str) -> pl.Expr:
    """Mirror PersonName.full_name: organization if present, else first/middle/last/
    suffix joined by single spaces (skipping blanks). Parts are stripped (_strip)."""
    org = clean_str(organization)
    parts = [clean_str(first), clean_str(middle), clean_str(last), clean_str(suffix)]
    joined = pl.concat_str(parts, separator=" ", ignore_nulls=True)
    joined = pl.when(joined.str.len_chars() > 0).then(joined).otherwise(pl.lit(""))
    return pl.when(org.is_not_null()).then(org).otherwise(joined)


def upper_str(col: str) -> pl.Expr:
    """Strip -> upper, null if empty. Mirrors AddressParts.normalized() state handling."""
    return clean_str(col).str.to_uppercase()


def person_addr_key_expr(
    s1_col: str | pl.Expr,
    city_col: str | pl.Expr,
    state_col: str | pl.Expr,
    zip_col: str | pl.Expr,
) -> pl.Expr:
    """Denormalized ``dedup_addr_key`` expression: ``"street|city|state|zip"`` or null.

    Mirrors ``BuilderCache.address_key`` / ``address_key_str``: street/city/state are
    lowered, zip kept as-is, components joined by ``|`` (empty for a missing part).  The
    key is null unless ≥2 of the four fields are populated — so an addressless or
    barely-addressed individual degrades to a name-only dedup key (today's behavior),
    while two same-name people at distinct locations get distinct keys.

    Inputs may be column names (cleaned via ``clean_str``) or already-built expressions
    (e.g. ``upper_str`` for state); pass expressions when the caller normalizes a field.
    """
    def _expr(c: str | pl.Expr) -> pl.Expr:
        return clean_str(c) if isinstance(c, str) else c

    s1 = _expr(s1_col).str.to_lowercase()
    city = _expr(city_col).str.to_lowercase()
    state = _expr(state_col).str.to_lowercase()
    zip_ = _expr(zip_col)  # zip is stored as-is (matches address_key)
    populated = (
        s1.is_not_null().cast(pl.Int32)
        + city.is_not_null().cast(pl.Int32)
        + state.is_not_null().cast(pl.Int32)
        + zip_.is_not_null().cast(pl.Int32)
    )
    joined = pl.concat_str(
        [
            s1.fill_null(""),
            city.fill_null(""),
            state.fill_null(""),
            zip_.fill_null(""),
        ],
        separator="|",
    )
    return pl.when(populated >= 2).then(joined).otherwise(None)


def collapse_org_person_key(frame: pl.DataFrame) -> pl.DataFrame:
    """Null ``_pk_fn``/``_pk_ln``/``_pk_addr`` on rows where ``_pk_org`` is set, so an
    org-person is deduped/looked-up on ``lower(org)`` ALONE.

    Mirrors the ORM ``BuilderCache.person_key`` (org present -> ``("org", lower(org),
    state)``, ignoring first/last/address) and the partial index ``uix_persons_org_state``
    on ``(lower(organization), state_id) WHERE organization IS NOT NULL``. Without this,
    two org rows with the same org but different incidental first/last names survive the
    engine's dedup yet collide on the org-only unique index on Postgres.

    Apply once, right after building ``_pk_org``/``_pk_fn``/``_pk_ln`` (and ``_pk_addr``
    where present) and before any group_by / unique / id-map join on those keys, so every
    downstream use is consistent. The stored ``first_name``/``last_name`` columns are
    untouched — only the dedup KEY. ``_pk_addr`` is nulled only when the frame carries it
    (the participant-projection frames in flat_txns_detail key on name alone).
    """
    has_org = pl.col("_pk_org").is_not_null()
    updates = [
        pl.when(has_org).then(None).otherwise(pl.col("_pk_fn")).alias("_pk_fn"),
        pl.when(has_org).then(None).otherwise(pl.col("_pk_ln")).alias("_pk_ln"),
    ]
    if "_pk_addr" in frame.columns:
        updates.append(
            pl.when(has_org).then(None).otherwise(pl.col("_pk_addr")).alias("_pk_addr")
        )
    return frame.with_columns(updates)


# ── provenance ───────────────────────────────────────────────────────────────
def raw_json_expr(columns: list[str], alias: str = "raw_data") -> pl.Expr:
    """JSON-encode the original columns as provenance. Compared structurally (not
    byte-exact) by the harness, since json.dumps and Polars differ on separators."""
    return pl.struct([pl.col(c) for c in columns]).struct.json_encode().alias(alias)


# ── writes ───────────────────────────────────────────────────────────────────
def _python_default_factories(model: type) -> list[tuple[str, Any]]:
    """``(column_name, () -> value)`` for every column with a Python-side default
    (``default=`` / ``default_factory=`` → uuid, created_at/updated_at/last_modified_at,
    scalar flags like ``is_forgiven=False``). SQLAlchemy fires these on a core insert, so
    the bulk_upsert path gets them for free; the raw COPY path bypasses SQLAlchemy and must
    materialize them itself, or NOT-NULL columns the families don't compute would fail."""
    out: list[tuple[str, Any]] = []
    for col in model.__table__.columns:  # type: ignore[attr-defined]
        d = col.default
        if d is None or not getattr(d, "is_scalar", False) and not getattr(d, "is_callable", False):
            continue  # no default, or a server/clause default the DB resolves itself
        if getattr(d, "is_scalar", False):
            out.append((col.name, lambda _v=d.arg: _v))
        else:  # callable: SQLAlchemy wraps no-arg factories to take a context arg
            out.append((col.name, lambda _f=d.arg: _f(None)))
    return out


def _inject_auto_columns(model: type, rows: list[dict[str, Any]]) -> None:
    """Materialize every Python-side column default the rows don't already provide, so the
    Postgres COPY path produces the same rows SQLAlchemy core would. Auto values (uuid,
    timestamps) are volatile and dropped by the equivalence harness; scalar defaults (flags)
    match what the ORM stores."""
    factories = _python_default_factories(model)
    if not factories:
        return
    for r in rows:
        for name, make in factories:
            if r.get(name) is None:
                r[name] = make()


def _copy_columns(model: type, rows: list[dict[str, Any]]) -> list[str]:
    """Stable COPY column order: the model's declared columns that appear in the rows
    (after auto-injection), in table-definition order. Restricting to declared columns
    keeps the COPY/INSERT column list aligned with the target table."""
    declared = list(model.__table__.columns.keys())  # type: ignore[attr-defined]
    present = set().union(*(r.keys() for r in rows)) if rows else set()
    return [c for c in declared if c in present]


# COPY CSV NULL sentinel. Must be distinct from "" so an empty STRING round-trips as an
# empty string (matching bulk_upsert), not NULL — Postgres treats an unquoted empty field
# as "" only when the NULL marker is non-empty. ``\N`` as a bare unquoted field is the
# idiomatic COPY null and effectively never occurs as a real (quote-free) data value here.
_COPY_NULL = "\\N"


def _csv_cell(value: Any) -> Any:
    """One CSV field for COPY: None -> the NULL sentinel; Enum -> its NAME (Postgres native
    enums use member names, and ``str(enum)`` on a str-mixin enum is the lowercase value
    which would be rejected); everything else as-is (Decimal/date/datetime/bool/str all
    round-trip through Postgres input parsing)."""
    if value is None:
        return _COPY_NULL
    if isinstance(value, enum.Enum):
        return value.name
    return value


def _rows_to_csv_buffer(rows: list[dict[str, Any]], columns: list[str]):
    """Serialize *rows* to an in-memory CSV buffer for ``COPY ... (FORMAT csv, NULL '\\N')``.

    None -> ``\\N`` (NULL); empty string -> "" (preserved, distinct from NULL). QUOTE_MINIMAL
    quotes JSON/text containing commas/quotes/newlines so COPY parses them intact.
    """
    import csv
    import io

    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for r in rows:
        writer.writerow([_csv_cell(r.get(c)) for c in columns])
    buf.seek(0)
    return buf


def _write_frame_postgres(
    session: Any,
    model: type,
    rows: list[dict[str, Any]],
    *,
    conflict_cols: list[str] | None,
    update_cols: list[str] | None,
) -> int:
    """Postgres COPY fast-path. Direct COPY when there is no conflict key; otherwise COPY
    into an ``ON COMMIT DROP`` staging table then ``INSERT ... SELECT ... ON CONFLICT``.

    SQL is composed with ``psycopg2.sql`` — only Table-metadata identifiers are injected
    (never values; row data rides STDIN), so it is injection-safe by construction.
    """
    from psycopg2 import sql

    table_name = model.__table__.name  # type: ignore[attr-defined]
    columns = _copy_columns(model, rows)
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    buf = _rows_to_csv_buffer(rows, columns)
    raw = session.connection().connection.driver_connection
    cur = raw.cursor()
    try:
        if not conflict_cols:
            copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')").format(
                sql.Identifier(table_name), col_idents
            )
            cur.copy_expert(copy_stmt.as_string(cur), buf)
        else:
            stg = f"_stg_{table_name}"
            cur.execute(
                sql.SQL(
                    "CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP"
                ).format(sql.Identifier(stg), sql.Identifier(table_name))
            )
            copy_stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')").format(
                sql.Identifier(stg), col_idents
            )
            cur.copy_expert(copy_stmt.as_string(cur), buf)
            targets = update_cols or [c for c in columns if c not in conflict_cols]
            if targets:
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in targets
                )
                action = sql.SQL("DO UPDATE SET {}").format(set_clause)
            else:
                action = sql.SQL("DO NOTHING")
            cur.execute(
                sql.SQL(
                    "INSERT INTO {tbl} ({cols}) SELECT {cols} FROM {stg} "
                    "ON CONFLICT ({conf}) {act}"
                ).format(
                    tbl=sql.Identifier(table_name),
                    cols=col_idents,
                    stg=sql.Identifier(stg),
                    conf=sql.SQL(", ").join(sql.Identifier(c) for c in conflict_cols),
                    act=action,
                )
            )
    finally:
        cur.close()
    session.commit()
    return len(rows)


def write_frame(
    session: Any,
    model: type,
    frame: pl.DataFrame,
    *,
    conflict_cols: list[str] | None,
    update_cols: list[str] | None = None,
) -> int:
    """Bulk-write a Polars frame into ``model``'s table (dialect-safe).

    With ``conflict_cols`` -> upsert; with ``conflict_cols=None`` -> plain insert (for
    tables without a natural unique key). Auto-fills uuid/timestamps the ORM would set.

    On PostgreSQL this uses a COPY fast-path (direct COPY, or COPY-to-staging +
    INSERT...ON CONFLICT for upserts) — orders of magnitude faster than executemany at
    ingest scale. On sqlite / other dialects it uses ``bulk_upsert`` / core insert. The
    COPY path is proven row-identical to bulk_upsert by the equivalence harness. Set
    ``VECTORIZED_DISABLE_COPY=1`` to force the legacy path (used to diff COPY vs upsert).
    """
    import os

    from sqlalchemy import insert

    from app.core.upsert import bulk_upsert

    if frame.is_empty():
        return 0
    rows = frame.to_dicts()
    _inject_auto_columns(model, rows)

    is_postgres = session.get_bind().dialect.name == "postgresql"
    if is_postgres and not os.environ.get("VECTORIZED_DISABLE_COPY"):
        return _write_frame_postgres(
            session, model, rows, conflict_cols=conflict_cols, update_cols=update_cols
        )

    if conflict_cols:
        return bulk_upsert(
            session, model, rows, conflict_cols=conflict_cols, update_cols=update_cols
        )
    # Plain insert: commit explicitly (bulk_upsert commits internally; the dispatcher
    # closes the session in finally with no commit, which would otherwise roll this back).
    session.execute(insert(model.__table__), rows)  # type: ignore[attr-defined]
    session.commit()
    return len(rows)
