"""Natural-key → surrogate-id maps for the vectorized ingest pipeline.

Extracted from ``app.core.ingest_vectorized.families.detail_children`` so that
multiple families (detail_children, flat_txns_dims, flat_txns_detail) can share
these reads without a circular import.
"""

from __future__ import annotations

from typing import Any

import polars as pl
from sqlalchemy import MetaData, Table, select

from app.core.ingest_vectorized import common


def _enum_name(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "name", str(value))


def _lower_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s.lower() if s else None


def reflect(engine: Any, name: str) -> Table:
    """Reflect a database table by name."""
    return Table(name, MetaData(), autoload_with=engine)


def entity_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{entity_type, normalized_name} -> entity id for this state."""
    tbl = reflect(engine, "unified_entities")
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


def address_id_map(engine: Any) -> pl.DataFrame:
    """4-field lower-cased address key -> address surrogate id."""
    tbl = reflect(engine, "unified_addresses")
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


def person_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{_pk_org, _pk_fn, _pk_ln, _pk_addr} -> person id for this state (lower-cased keys;
    org-persons keyed on lower(org) ALONE via collapse_org_person_key, matching
    uix_persons_org_state; individuals split by dedup_addr_key per uix_persons_name_state)."""
    tbl = reflect(engine, "unified_persons")
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


def txn_id_map(
    engine: Any,
    state_id: int,
    transaction_types: frozenset[str],
) -> pl.DataFrame:
    """{transaction_id, transaction_type} -> txn surrogate id for this state.

    *transaction_types* is a frozenset of transaction-type enum names (e.g.
    ``frozenset({"LOAN", "DEBT"})``) used to filter the result set.
    """
    tbl = reflect(engine, "unified_transactions")
    stmt = select(tbl.c.id, tbl.c.transaction_id, tbl.c.transaction_type).where(
        tbl.c.state_id == state_id
    )
    with engine.connect() as conn:
        rows = [dict(m) for m in conn.execute(stmt).mappings().all()]
    keep = [r for r in rows if _enum_name(r["transaction_type"]) in transaction_types]
    return pl.DataFrame(
        {
            "txn_pk": [r["id"] for r in keep],
            "transaction_id": [
                None if r["transaction_id"] is None else str(r["transaction_id"]) for r in keep
            ],
            "transaction_type": [_enum_name(r["transaction_type"]) for r in keep],
        },
        schema={"txn_pk": pl.Int64, "transaction_id": pl.Utf8, "transaction_type": pl.Utf8},
    )


def committee_entity_map(engine: Any, state_id: int) -> dict[str, int]:
    """committee filer_id -> committee entity id."""
    tbl = reflect(engine, "unified_entities")
    stmt = select(tbl.c.id, tbl.c.committee_id).where(
        tbl.c.state_id == state_id, tbl.c.committee_id.is_not(None)
    )
    with engine.connect() as conn:
        return {m["committee_id"]: m["id"] for m in conn.execute(stmt).mappings().all()}


def loan_pk_map(engine: Any, table: str) -> dict[int, int]:
    """parent transaction_id (surrogate) -> detail surrogate id, for loan/debt."""
    tbl = reflect(engine, table)
    stmt = select(tbl.c.id, tbl.c.transaction_id)
    with engine.connect() as conn:
        return {m["transaction_id"]: m["id"] for m in conn.execute(stmt).mappings().all()}
