"""Ingest equivalence harness — snapshot + diff the unified/canonical tables.

Foundation (P0) for the vectorized ingest rewrite (see
``docs/design/vectorized-ingest-plan.md``).  Lets any new ingest engine be proven
**row-for-row equivalent** to the current ORM loader before it replaces it:

    orm_snap = snapshot_unified(orm_engine)
    vec_snap = snapshot_unified(vectorized_engine)
    assert diff_snapshots(orm_snap, vec_snap) == []

Comparison excludes columns that legitimately differ between runs/engines:
surrogate primary keys, surrogate foreign keys (FK -> <parent>.id), uuids and
timestamps.  Natural-key columns are kept — including natural-key FKs such as
``unified_transactions.committee_id`` (-> ``unified_committees.filer_id``), which is
how linkage is still compared without depending on surrogate ids.

LIMITATION (v1): junction rows whose only non-volatile columns are surrogate FKs
(e.g. ``unified_transaction_persons.transaction_id``/``person_id``) are compared on
their remaining natural columns (role, amount) only — gross differences are caught,
fine-grained mislinkage is not. P1 may add FK->natural-key resolution.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from sqlalchemy import MetaData, Table, inspect, select
from sqlalchemy.engine import Engine

# Columns that differ between runs/engines regardless of logical content.
VOLATILE_COLUMNS: frozenset[str] = frozenset(
    {
        "id",
        "uuid",
        "created_at",
        "updated_at",
        "last_modified_at",
        "last_modified_by",
        "download_date",
        "change_reason",
        "amendment_details",
        "last_run_id",
    }
)


def _target_tables(inspector: Any) -> list[str]:
    """Unified source-layer + canonical resolve-layer tables present in the DB."""
    names = set(inspector.get_table_names())
    return sorted(
        t for t in names if t.startswith("unified_") or t.startswith("canonical_")
    )


def _surrogate_fk_columns(inspector: Any, table: str) -> set[str]:
    """Columns that are a FK to a parent's surrogate ``id`` (not a natural key)."""
    drop: set[str] = set()
    for fk in inspector.get_foreign_keys(table):
        if fk.get("referred_columns") == ["id"]:
            drop.update(fk.get("constrained_columns", []))
    return drop


def _comparable_columns(inspector: Any, table: str) -> list[str]:
    cols = [c["name"] for c in inspector.get_columns(table)]
    drop = set(VOLATILE_COLUMNS) | _surrogate_fk_columns(inspector, table)
    return [c for c in cols if c not in drop]


def _row_key(row: dict[str, Any]) -> tuple[tuple[str, str | None], ...]:
    """A hashable, order-independent key for one row (None distinct from '')."""
    return tuple(
        sorted((k, None if v is None else str(v)) for k, v in row.items())
    )


def _sort_key(row: dict[str, Any]) -> tuple[str, ...]:
    """None-safe ordering key (sort is only for stable/readable output; equality
    is decided by the Counter of _row_key, which is order-independent)."""
    return tuple("" if v is None else str(v) for _k, v in sorted(row.items()))


def snapshot_unified(engine: Engine) -> dict[str, list[dict[str, Any]]]:
    """Return {table: [normalized row dicts]} for every unified/canonical table.

    Volatile + surrogate-FK columns are dropped; rows are deterministically sorted.
    Built with SQLAlchemy core ``select`` over reflected tables (no string SQL).
    """
    inspector = inspect(engine)
    snapshot: dict[str, list[dict[str, Any]]] = {}
    md = MetaData()
    for table in _target_tables(inspector):
        keep = _comparable_columns(inspector, table)
        if not keep:
            continue
        tbl = Table(table, md, autoload_with=engine)
        stmt = select(*[tbl.c[name] for name in keep])
        with engine.connect() as conn:
            rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
        rows.sort(key=_sort_key)
        snapshot[table] = rows
    return snapshot


def diff_snapshots(
    left: dict[str, list[dict[str, Any]]],
    right: dict[str, list[dict[str, Any]]],
    *,
    max_examples: int = 3,
) -> list[str]:
    """Multiset-compare two snapshots. Returns [] when equal, else human-readable
    diff lines (table counts + a few example offending rows per side)."""
    diffs: list[str] = []
    for table in sorted(set(left) | set(right)):
        if table not in left:
            diffs.append(f"{table}: present only in right ({len(right[table])} rows)")
            continue
        if table not in right:
            diffs.append(f"{table}: present only in left ({len(left[table])} rows)")
            continue
        ca = Counter(_row_key(r) for r in left[table])
        cb = Counter(_row_key(r) for r in right[table])
        if ca == cb:
            continue
        only_left = ca - cb
        only_right = cb - ca
        diffs.append(
            f"{table}: {len(left[table])} (left) vs {len(right[table])} (right) rows; "
            f"{sum(only_left.values())} left-only, {sum(only_right.values())} right-only"
        )
        for key, _n in list(only_left.items())[:max_examples]:
            diffs.append(f"    left-only:  {dict(key)}")
        for key, _n in list(only_right.items())[:max_examples]:
            diffs.append(f"    right-only: {dict(key)}")
    return diffs
