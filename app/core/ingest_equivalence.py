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


# Provenance/JSON columns compared STRUCTURALLY (parse + canonical re-dump), not
# byte-exact: json.dumps (ORM) and Polars json_encode differ on key order/separators,
# but the logical content must match.
JSON_COLUMNS: frozenset[str] = frozenset(
    {"raw_data", "provenance_json", "metadata_json", "explanation_json", "raw_json", "config_json"}
)


def _canonicalize_json(value: Any) -> Any:
    """Parse a JSON string and re-dump it canonically (sorted keys, no spaces) so two
    engines that emit equivalent-but-differently-formatted JSON compare equal. Returns
    the original value unchanged if it is not parseable JSON."""
    import json

    if value is None:
        return None
    try:
        return json.dumps(json.loads(value), sort_keys=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):
        return value


# Snapshot-target tables that don't match the unified_/canonical_ prefix but are
# still part of the ingest output we gate (e.g. the loan/debt guarantor child).
_EXTRA_TARGET_TABLES: frozenset[str] = frozenset({"loan_guarantors"})


def _target_tables(inspector: Any) -> list[str]:
    """Unified source-layer + canonical resolve-layer tables present in the DB."""
    names = set(inspector.get_table_names())
    return sorted(
        t
        for t in names
        if t.startswith("unified_")
        or t.startswith("canonical_")
        or t in _EXTRA_TARGET_TABLES
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
    return tuple(sorted((k, None if v is None else str(v)) for k, v in row.items()))


def _sort_key(row: dict[str, Any]) -> tuple[str, ...]:
    """None-safe ordering key (sort is only for stable/readable output; equality
    is decided by the Counter of _row_key, which is order-independent)."""
    return tuple("" if v is None else str(v) for _k, v in sorted(row.items()))


def _intra_fk_parents(inspector: Any, table: str, targets: set[str]) -> dict[str, str]:
    """{column: parent_table} for surrogate FKs (-> parent.id) whose parent is itself
    a snapshot target (unified_/canonical_). These get RESOLVED to the parent's natural
    key in resolve_fks mode (instead of dropped). FKs to non-target tables
    (file_origins, states) stay dropped — they are provenance, not logical linkage."""
    out: dict[str, str] = {}
    for fk in inspector.get_foreign_keys(table):
        if fk.get("referred_columns") == ["id"] and fk.get("referred_table") in targets:
            for cc in fk.get("constrained_columns", []):
                out[cc] = fk["referred_table"]
    return out


# Surrogate FKs whose *target* is non-deterministic between equivalent runs and so
# must be compared as presence-only, not resolved to the parent's natural key.
#
# ``unified_entities.person_id`` / ``unified_entities.address_id`` are the entity's
# REPRESENTATIVE person and that person's address. When two source rows describe the same
# party with trivial spelling variants ("Angela F" vs "Angela F.") they keep distinct
# person rows but collapse to ONE entity (same normalized_name); which variant the entity
# points to — and hence which address rides along — is decided by a flush/hash-seed-ordering
# dependent ``person.entity = entity`` reassignment, so ORM-vs-ORM itself diverges run to
# run on BOTH columns. They are reduced to "<set>" / None (presence only).
#
# What stays strictly compared: the entity's OWN identity (type, name, normalized_name);
# the participant person rows themselves (in unified_persons); and the junction's DIRECT
# participant ``unified_transaction_persons.person_id`` (a different column, fully resolved).
# ACCEPTED BLIND SPOT: because the representative person/address are presence-only, this
# gate cannot distinguish an entity linked to the *right* representative from one linked to
# a structurally *wrong* person/address — only "linked vs not". Participant-level linkage
# (the thing that actually matters for a contribution/expenditure) is still caught via the
# junction's direct person_id and the person rows; the representative pointer is metadata.
_PRESENCE_ONLY_FKS: frozenset[tuple[str, str]] = frozenset(
    {("unified_entities", "person_id"), ("unified_entities", "address_id")}
)


def _snapshot_resolved(engine: Engine) -> dict[str, list[dict[str, Any]]]:
    """snapshot_unified with surrogate FKs RESOLVED to parent natural keys.

    Lets the gate verify relational LINKAGE (e.g. a contribution's contributor entity)
    instead of dropping surrogate ids. Recursive (entity -> person -> address), memoized,
    cycle-guarded. Only intra-target FKs are resolved; provenance FKs and volatile cols
    are dropped exactly as in the default snapshot. FKs listed in ``_PRESENCE_ONLY_FKS``
    are reduced to a deterministic presence marker (their target is non-deterministic).
    """
    inspector = inspect(engine)
    targets = set(_target_tables(inspector))
    md = MetaData()
    raw: dict[str, dict[Any, dict[str, Any]]] = {}
    intra: dict[str, dict[str, str]] = {}
    drop_cols: dict[str, set[str]] = {}
    cols_by_table: dict[str, list[str]] = {}
    has_surrogate_id: dict[str, bool] = {}
    for table in targets:
        tbl = Table(table, md, autoload_with=engine)
        cols = [c.name for c in tbl.columns]
        cols_by_table[table] = cols
        # Some target tables key on a natural column (e.g. unified_committees.filer_id)
        # and have no surrogate 'id'. They can never be an intra-target FK parent
        # (FKs resolve to '.id'), so index their rows positionally for output only.
        has_surrogate_id[table] = "id" in cols
        intra[table] = _intra_fk_parents(inspector, table, targets)
        # Drop: volatile + surrogate FKs to NON-target parents (provenance). Intra-target
        # FK columns are kept here and resolved; the 'id' PK is dropped from output.
        ext_fk = _surrogate_fk_columns(inspector, table) - set(intra[table])
        drop_cols[table] = set(VOLATILE_COLUMNS) | ext_fk
        with engine.connect() as conn:
            mappings = conn.execute(select(tbl)).mappings().all()
        if has_surrogate_id[table]:
            raw[table] = {m["id"]: dict(m) for m in mappings}
        else:
            raw[table] = {f"_pos_{i}": dict(m) for i, m in enumerate(mappings)}

    memo: dict[tuple[str, Any], Any] = {}

    def resolve(table: str, fk_id: Any, stack: frozenset) -> Any:
        if fk_id is None:
            return None
        key = (table, fk_id)
        if key in memo:
            return memo[key]
        if key in stack or table not in raw or fk_id not in raw[table]:
            return f"<unresolved:{table}:{fk_id}>"
        nat = natural(table, raw[table][fk_id], stack | {key})
        memo[key] = nat
        return nat

    def natural(table: str, row: dict[str, Any], stack: frozenset) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for col in cols_by_table[table]:
            if col == "id" or col in drop_cols[table]:
                continue
            if (table, col) in _PRESENCE_ONLY_FKS:
                out[col] = "<set>" if row.get(col) is not None else None
            elif col in intra[table]:
                out[col] = resolve(intra[table][col], row.get(col), stack)
            elif col in JSON_COLUMNS:
                out[col] = _canonicalize_json(row.get(col))
            else:
                v = row.get(col)
                out[col] = None if v is None else str(v)
        return out

    snapshot: dict[str, list[dict[str, Any]]] = {}
    for table in targets:
        rows = [natural(table, r, frozenset()) for r in raw[table].values()]
        rows.sort(key=_sort_key)
        snapshot[table] = rows
    return snapshot


def snapshot_unified(
    engine: Engine, *, resolve_fks: bool = False
) -> dict[str, list[dict[str, Any]]]:
    """Return {table: [normalized row dicts]} for every unified/canonical table.

    Volatile + surrogate-FK columns are dropped; rows are deterministically sorted.
    Built with SQLAlchemy core ``select`` over reflected tables (no string SQL).

    ``resolve_fks=True`` resolves intra-target surrogate FKs to the parent's natural key
    (recursively) so the gate can verify relational linkage — opt-in, since it is
    strictly stricter than the default drop-surrogate-FK behavior.
    """
    if resolve_fks:
        return _snapshot_resolved(engine)
    inspector = inspect(engine)
    snapshot: dict[str, list[dict[str, Any]]] = {}
    md = MetaData()
    for table in _target_tables(inspector):
        keep = _comparable_columns(inspector, table)
        if not keep:
            continue
        tbl = Table(table, md, autoload_with=engine)
        stmt = select(*[tbl.c[name] for name in keep])
        json_cols = [c for c in keep if c in JSON_COLUMNS]
        with engine.connect() as conn:
            rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
        for row in rows:
            for col in json_cols:
                row[col] = _canonicalize_json(row[col])
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
