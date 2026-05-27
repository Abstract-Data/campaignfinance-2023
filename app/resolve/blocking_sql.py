"""Postgres set-based blocking for stage 2.

Generates ``candidate_pairs`` via ``INSERT … SELECT`` self-joins inside the
database instead of materializing combinations in Python.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlmodel import delete, func, select

from app.resolve.blocking import (
    BlockingRule,
    CandidatePair,
    _rules_from_config,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

LOGGER = logging.getLogger(__name__)

_UNIQUE_INDEX = "uq_candidate_pairs_run_sources"


def _block_key_expression(rule: BlockingRule) -> str:
    """Return a SQL expression for a blocking key (NULL when incomplete)."""
    parts: list[str] = []
    for field_name in rule.fields:
        if field_name == "zip3":
            parts.append(
                "CASE WHEN ri.zip5 IS NOT NULL AND length(trim(ri.zip5)) >= 3 "
                "THEN lower(substr(trim(ri.zip5), 1, 3)) END"
            )
        elif field_name == "first_initial":
            parts.append(
                "CASE WHEN ri.first_name IS NOT NULL AND trim(ri.first_name) <> '' "
                "THEN lower(substr(trim(ri.first_name), 1, 1)) END"
            )
        elif field_name == "last_name_phonetic":
            parts.append(
                "CASE WHEN ri.last_name_phonetic IS NOT NULL "
                "AND trim(ri.last_name_phonetic) <> '' "
                "THEN lower(trim(ri.last_name_phonetic)) END"
            )
        elif field_name == "normalized_org":
            parts.append(
                "CASE WHEN ri.normalized_org IS NOT NULL AND trim(ri.normalized_org) <> '' "
                "THEN lower(trim(ri.normalized_org)) END"
            )
        else:
            parts.append(
                f"CASE WHEN ri.{field_name} IS NOT NULL AND trim(ri.{field_name}) <> '' "
                f"THEN lower(trim(ri.{field_name})) END"
            )

    if not parts:
        raise ValueError(f"Blocking rule '{rule.name}' has no fields")

    if len(parts) == 1:
        return parts[0]

    expr = parts[0]
    for part in parts[1:]:
        expr = (
            f"CASE WHEN {expr} IS NOT NULL AND {part} IS NOT NULL "
            f"THEN {expr} || '|' || {part} END"
        )
    return expr


def ensure_candidate_pair_unique_index(session: Session) -> None:
    """Create the pair-level unique index required for ``ON CONFLICT DO NOTHING``."""
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    session.execute(
        text(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {_UNIQUE_INDEX}
            ON candidate_pairs (
                run_id,
                source_a_type,
                source_a_id,
                source_b_type,
                source_b_id
            )
            """
        )
    )
    session.commit()


def _log_oversized_blocks(
    session: Session,
    *,
    run_id: int,
    rule: BlockingRule,
    block_key_sql: str,
    max_block_size: int,
) -> None:
    rows = session.execute(
        text(
            f"""
            WITH keyed AS (
                SELECT
                    {block_key_sql} AS block_key
                FROM resolution_input ri
                WHERE ri.run_id = :run_id
            )
            SELECT block_key, COUNT(*) AS block_size
            FROM keyed
            WHERE block_key IS NOT NULL
            GROUP BY block_key
            HAVING COUNT(*) > :max_block_size
            """
        ),
        {"run_id": run_id, "max_block_size": max_block_size},
    ).fetchall()
    for block_key, block_size in rows:
        LOGGER.warning(
            "Skipping oversized block rule=%s key=%s size=%s max=%s",
            rule.name,
            block_key,
            block_size,
            max_block_size,
        )


def _insert_rule_pairs(
    session: Session,
    *,
    run_id: int,
    rule: BlockingRule,
    block_key_sql: str,
    max_block_size: int,
) -> int:
    _log_oversized_blocks(
        session,
        run_id=run_id,
        rule=rule,
        block_key_sql=block_key_sql,
        max_block_size=max_block_size,
    )
    result = session.execute(
        text(
            f"""
            WITH keyed AS (
                SELECT
                    ri.source_type,
                    ri.source_id,
                    {block_key_sql} AS block_key
                FROM resolution_input ri
                WHERE ri.run_id = :run_id
            ),
            eligible_blocks AS (
                SELECT block_key
                FROM keyed
                WHERE block_key IS NOT NULL
                GROUP BY block_key
                HAVING COUNT(*) <= :max_block_size
            )
            INSERT INTO candidate_pairs (
                run_id,
                source_a_type,
                source_a_id,
                source_b_type,
                source_b_id,
                rule_name
            )
            SELECT
                :run_id,
                a.source_type,
                a.source_id,
                b.source_type,
                b.source_id,
                :rule_name
            FROM keyed a
            INNER JOIN keyed b
                ON a.block_key = b.block_key
                AND (a.source_type, a.source_id) < (b.source_type, b.source_id)
            INNER JOIN eligible_blocks eb ON a.block_key = eb.block_key
            ON CONFLICT (
                run_id,
                source_a_type,
                source_a_id,
                source_b_type,
                source_b_id
            ) DO NOTHING
            """
        ),
        {
            "run_id": run_id,
            "rule_name": rule.name,
            "max_block_size": max_block_size,
        },
    )
    return int(result.rowcount or 0)


def _apply_pair_cap(session: Session, *, run_id: int, max_pairs_per_run: int) -> None:
    total = session.exec(
        select(func.count())
        .select_from(CandidatePair)
        .where(CandidatePair.run_id == run_id)
    ).one()
    if total <= max_pairs_per_run:
        return

    LOGGER.warning(
        "Capping candidate pairs from %s to max_pairs_per_run=%s (golden-set tuning guardrail)",
        total,
        max_pairs_per_run,
    )
    session.execute(
        text(
            """
            WITH ranked AS (
                SELECT
                    id,
                    ROW_NUMBER() OVER (
                        ORDER BY
                            source_a_type,
                            source_a_id,
                            source_b_type,
                            source_b_id,
                            rule_name
                    ) AS rn
                FROM candidate_pairs
                WHERE run_id = :run_id
            )
            DELETE FROM candidate_pairs cp
            USING ranked r
            WHERE cp.id = r.id
              AND r.rn > :max_pairs_per_run
            """
        ),
        {"run_id": run_id, "max_pairs_per_run": max_pairs_per_run},
    )


def run_blocking_stage_sql(session: Session, run_id: int, config: dict) -> dict:
    """Postgres blocking backend: set-based inserts with cross-rule dedupe."""
    max_block_size = int(config.get("max_block_size", 500))
    max_pairs_raw = config.get("max_pairs_per_run")
    max_pairs_per_run = int(max_pairs_raw) if max_pairs_raw is not None else None
    rules = _rules_from_config(config)

    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("SQL blocking backend requires PostgreSQL")

    ensure_candidate_pair_unique_index(session)
    session.exec(delete(CandidatePair).where(CandidatePair.run_id == run_id))
    session.commit()

    for rule in rules:
        block_key_sql = _block_key_expression(rule)
        inserted = _insert_rule_pairs(
            session,
            run_id=run_id,
            rule=rule,
            block_key_sql=block_key_sql,
            max_block_size=max_block_size,
        )
        session.commit()
        LOGGER.info(
            "Blocking rule=%s inserted_rows=%s (includes ON CONFLICT skips)",
            rule.name,
            inserted,
        )

    if max_pairs_per_run is not None:
        _apply_pair_cap(session, run_id=run_id, max_pairs_per_run=max_pairs_per_run)
        session.commit()

    pair_count = session.exec(
        select(func.count())
        .select_from(CandidatePair)
        .where(CandidatePair.run_id == run_id)
    ).one()
    return {"pairs_compared": int(pair_count)}
