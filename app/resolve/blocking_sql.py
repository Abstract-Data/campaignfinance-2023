"""Postgres set-based blocking for stage 2.

Materializes blocking keys into a temp table, then inserts pairs in batches of
block keys so Postgres never self-joins the full ``resolution_input`` set at once.
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

_BLOCK_KEY_BATCH_SIZE = 2_000
_PAIR_CAP_DELETE_BATCH = 500_000
_BLOCK_KEY_PLACEHOLDER = "__BLOCK_KEY_EXPR__"

# Static SQL fragments per default rule (no runtime SQL string assembly).
_RULE_BLOCK_KEY_SQL: dict[str, str] = {
    "person_last_phonetic_zip3": (
        "CASE WHEN ri.last_name_phonetic IS NOT NULL "
        "AND trim(ri.last_name_phonetic) <> '' "
        "AND ri.zip5 IS NOT NULL AND length(trim(ri.zip5)) >= 3 "
        "THEN lower(trim(ri.last_name_phonetic)) || '|' || "
        "lower(substr(trim(ri.zip5), 1, 3)) END"
    ),
    "person_first_initial_last_phonetic": (
        "CASE WHEN ri.first_name IS NOT NULL AND trim(ri.first_name) <> '' "
        "AND ri.last_name_phonetic IS NOT NULL AND trim(ri.last_name_phonetic) <> '' "
        "THEN lower(substr(trim(ri.first_name), 1, 1)) || '|' || "
        "lower(trim(ri.last_name_phonetic)) END"
    ),
    "org_normalized_zip3": (
        "CASE WHEN ri.normalized_org IS NOT NULL AND trim(ri.normalized_org) <> '' "
        "AND ri.zip5 IS NOT NULL AND length(trim(ri.zip5)) >= 3 "
        "THEN lower(trim(ri.normalized_org)) || '|' || "
        "lower(substr(trim(ri.zip5), 1, 3)) END"
    ),
}

_DROP_TEMP_SQL = "DROP TABLE IF EXISTS blocking_keyed_stage"

_CREATE_TEMP_SQL = """
CREATE TEMP TABLE blocking_keyed_stage (
    source_type varchar(64) NOT NULL,
    source_id varchar(128) NOT NULL,
    block_key text NOT NULL
) ON COMMIT PRESERVE ROWS
"""

_MATERIALIZE_KEYED_SQL = """
INSERT INTO blocking_keyed_stage (source_type, source_id, block_key)
SELECT
    ri.source_type,
    ri.source_id,
    __BLOCK_KEY_EXPR__ AS block_key
FROM resolution_input ri
WHERE ri.run_id = :run_id
  AND __BLOCK_KEY_EXPR__ IS NOT NULL
"""

_CREATE_TEMP_INDEX_SQL = (
    "CREATE INDEX ix_blocking_keyed_stage_block_key ON blocking_keyed_stage (block_key)"
)

_OVERSIZED_BLOCKS_SQL = """
SELECT block_key, COUNT(*) AS block_size
FROM blocking_keyed_stage
GROUP BY block_key
HAVING COUNT(*) > :max_block_size
"""

_ELIGIBLE_BLOCK_KEYS_SQL = """
SELECT block_key
FROM blocking_keyed_stage
GROUP BY block_key
HAVING COUNT(*) <= :max_block_size
   AND COUNT(*) >= 2
ORDER BY block_key
"""

_INSERT_PAIRS_BATCH_SQL = """
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
FROM blocking_keyed_stage a
INNER JOIN blocking_keyed_stage b
    ON a.block_key = b.block_key
    AND (a.source_type, a.source_id) < (b.source_type, b.source_id)
WHERE a.block_key = ANY(:batch_keys)
ON CONFLICT (
    run_id,
    source_a_type,
    source_a_id,
    source_b_type,
    source_b_id
) DO NOTHING
"""


def block_key_sql_for_rule(rule: BlockingRule) -> str:
    """Return a whitelisted SQL expression for a blocking key."""
    expr = _RULE_BLOCK_KEY_SQL.get(rule.name)
    if expr is None:
        raise ValueError(
            f"SQL blocking has no static key expression for rule '{rule.name}'. "
            "Add it to _RULE_BLOCK_KEY_SQL or use blocking_backend=python."
        )
    return expr


def _sql_with_block_key(template: str, block_key_sql: str) -> str:
    return template.replace(_BLOCK_KEY_PLACEHOLDER, block_key_sql)


def ensure_blocking_indexes(session: Session) -> None:
    """Indexes to speed key materialization for large runs."""
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ix_resolution_input_run_id
            ON resolution_input (run_id)
            """
        )
    )
    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ix_resolution_input_run_phonetic_zip5
            ON resolution_input (run_id, last_name_phonetic, zip5)
            WHERE last_name_phonetic IS NOT NULL AND zip5 IS NOT NULL
            """
        )
    )
    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ix_resolution_input_run_org_zip5
            ON resolution_input (run_id, normalized_org, zip5)
            WHERE normalized_org IS NOT NULL AND zip5 IS NOT NULL
            """
        )
    )
    session.commit()


def ensure_candidate_pair_unique_index(session: Session) -> None:
    """Create the pair-level unique index required for ``ON CONFLICT DO NOTHING``."""
    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        return
    session.execute(
        text(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS uq_candidate_pairs_run_sources
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


def _log_oversized_blocks_from_temp(
    session: Session,
    *,
    rule: BlockingRule,
    max_block_size: int,
) -> None:
    rows = session.execute(
        text(_OVERSIZED_BLOCKS_SQL),
        {"max_block_size": max_block_size},
    ).fetchall()
    for block_key, block_size in rows:
        LOGGER.warning(
            "Skipping oversized block rule=%s key=%s size=%s max=%s",
            rule.name,
            block_key,
            block_size,
            max_block_size,
        )


def _materialize_keyed_rows(
    session: Session,
    *,
    run_id: int,
    block_key_sql: str,
) -> int:
    session.execute(text(_DROP_TEMP_SQL))
    session.execute(text(_CREATE_TEMP_SQL))
    materialize_sql = _sql_with_block_key(_MATERIALIZE_KEYED_SQL, block_key_sql)
    result = session.execute(
        text(materialize_sql),
        {"run_id": run_id},
    )
    session.execute(text(_CREATE_TEMP_INDEX_SQL))
    session.commit()
    return int(result.rowcount or 0)


def _iter_eligible_block_key_batches(
    session: Session,
    *,
    max_block_size: int,
    batch_size: int,
):
    """Yield block-key batches without loading every key into memory."""
    result = session.execute(
        text(_ELIGIBLE_BLOCK_KEYS_SQL),
        {"max_block_size": max_block_size},
    )
    batch: list[str] = []
    while True:
        rows = result.fetchmany(batch_size)
        if not rows:
            if batch:
                yield batch
            return
        for row in rows:
            batch.append(str(row[0]))
            if len(batch) >= batch_size:
                yield batch
                batch = []


def _insert_rule_pairs_batched(
    session: Session,
    *,
    run_id: int,
    rule: BlockingRule,
    block_key_sql: str,
    max_block_size: int,
    block_key_batch_size: int,
) -> int:
    keyed_rows = _materialize_keyed_rows(
        session,
        run_id=run_id,
        block_key_sql=block_key_sql,
    )
    LOGGER.info(
        "Blocking rule=%s materialized %s keyed rows into temp table",
        rule.name,
        keyed_rows,
    )

    _log_oversized_blocks_from_temp(session, rule=rule, max_block_size=max_block_size)

    total_inserted = 0
    batch_index = 0
    for batch_keys in _iter_eligible_block_key_batches(
        session,
        max_block_size=max_block_size,
        batch_size=block_key_batch_size,
    ):
        batch_index += 1
        result = session.execute(
            text(_INSERT_PAIRS_BATCH_SQL),
            {
                "run_id": run_id,
                "rule_name": rule.name,
                "batch_keys": batch_keys,
            },
        )
        inserted = int(result.rowcount or 0)
        total_inserted += inserted
        session.commit()
        LOGGER.info(
            "Blocking rule=%s batch %s block_keys=%s new_pairs=%s",
            rule.name,
            batch_index,
            len(batch_keys),
            inserted,
        )

    session.execute(text(_DROP_TEMP_SQL))
    session.commit()
    return total_inserted


_PAIR_CAP_DELETE_SQL = """
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
),
doomed AS (
    SELECT id
    FROM ranked
    WHERE rn > :max_pairs_per_run
    LIMIT :delete_batch
)
DELETE FROM candidate_pairs AS cp
USING doomed AS d
WHERE cp.id = d.id
"""


def _apply_pair_cap(
    session: Session,
    *,
    run_id: int,
    max_pairs_per_run: int,
    delete_batch_size: int = _PAIR_CAP_DELETE_BATCH,
) -> None:
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
    deleted_total = 0
    while True:
        result = session.execute(
            text(_PAIR_CAP_DELETE_SQL),
            {
                "run_id": run_id,
                "max_pairs_per_run": max_pairs_per_run,
                "delete_batch": delete_batch_size,
            },
        )
        deleted = int(result.rowcount or 0)
        if deleted == 0:
            break
        deleted_total += deleted
        session.commit()
        remaining = session.exec(
            select(func.count())
            .select_from(CandidatePair)
            .where(CandidatePair.run_id == run_id)
        ).one()
        LOGGER.info(
            "Pair cap deleted batch=%s remaining_pairs=%s",
            deleted,
            remaining,
        )
        if remaining <= max_pairs_per_run:
            break
    LOGGER.info("Pair cap complete deleted_total=%s", deleted_total)


def run_blocking_stage_sql(session: Session, run_id: int, config: dict) -> dict:
    """Postgres blocking backend: batched temp-table joins with cross-rule dedupe."""
    max_block_size = int(config.get("max_block_size", 500))
    max_pairs_raw = config.get("max_pairs_per_run")
    max_pairs_per_run = int(max_pairs_raw) if max_pairs_raw is not None else None
    block_key_batch_size = int(
        config.get("blocking_block_key_batch_size", _BLOCK_KEY_BATCH_SIZE)
    )
    rules = _rules_from_config(config)

    for rule in rules:
        if rule.name not in _RULE_BLOCK_KEY_SQL:
            raise ValueError(
                f"SQL blocking has no static key expression for rule '{rule.name}'. "
                "Add it to _RULE_BLOCK_KEY_SQL or set blocking_backend=python."
            )

    bind = session.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("SQL blocking backend requires PostgreSQL")

    ensure_blocking_indexes(session)
    ensure_candidate_pair_unique_index(session)
    session.exec(delete(CandidatePair).where(CandidatePair.run_id == run_id))
    session.commit()

    for rule in rules:
        block_key_sql = block_key_sql_for_rule(rule)
        inserted = _insert_rule_pairs_batched(
            session,
            run_id=run_id,
            rule=rule,
            block_key_sql=block_key_sql,
            max_block_size=max_block_size,
            block_key_batch_size=block_key_batch_size,
        )
        LOGGER.info(
            "Blocking rule=%s new_pairs_this_rule=%s (ON CONFLICT rows not counted)",
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
