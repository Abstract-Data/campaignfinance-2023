"""Postgres set-based blocking for stage 2.

Per rule, materializes blocking keys into a temp table and self-joins within each
eligible (size 2..max) block, appending pairs to an UNLOGGED, index-free staging
table. A single sorted ``DISTINCT ON`` insert then promotes the staged pairs to
``candidate_pairs`` — collapsing cross-rule duplicates and feeding the unique
index in key order. Staging avoids the per-row ON CONFLICT probe into a
multi-million-row unique index (the old batched path's quadratic wall) without
ever dropping the unique constraint.
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

_PAIR_CAP_DELETE_BATCH = 500_000
_BLOCK_KEY_PLACEHOLDER = "__BLOCK_KEY_EXPR__"

# Static SQL fragments per default rule (no runtime SQL string assembly).
# Each rule is scoped to its entity_type.  Org names are sometimes mis-parsed by
# probablepeople into person name parts (so an organization can carry a
# last_name_phonetic); without the entity_type guard the person rules generated
# organization candidate pairs that the per-entity-type Splink scorer could not
# reproduce in bulk predict, forcing a slow per-pair fallback.
_RULE_BLOCK_KEY_SQL: dict[str, str] = {
    "person_last_phonetic_zip3": (
        "CASE WHEN ri.entity_type = 'person' "
        "AND ri.last_name_phonetic IS NOT NULL "
        "AND trim(ri.last_name_phonetic) <> '' "
        "AND ri.zip5 IS NOT NULL AND length(trim(ri.zip5)) >= 3 "
        "THEN lower(trim(ri.last_name_phonetic)) || '|' || "
        "lower(substr(trim(ri.zip5), 1, 3)) END"
    ),
    "person_first_last_phonetic": (
        "CASE WHEN ri.entity_type = 'person' "
        "AND ri.first_name_phonetic IS NOT NULL AND trim(ri.first_name_phonetic) <> '' "
        "AND ri.last_name_phonetic IS NOT NULL AND trim(ri.last_name_phonetic) <> '' "
        "THEN lower(trim(ri.first_name_phonetic)) || '|' || "
        "lower(trim(ri.last_name_phonetic)) END"
    ),
    "org_normalized_zip3": (
        "CASE WHEN ri.entity_type IN ('organization', 'committee') "
        "AND ri.normalized_org IS NOT NULL AND trim(ri.normalized_org) <> '' "
        "AND ri.zip5 IS NOT NULL AND length(trim(ri.zip5)) >= 3 "
        "THEN lower(trim(ri.normalized_org)) || '|' || "
        "lower(substr(trim(ri.zip5), 1, 3)) END"
    ),
    # Cross-role org blocking: connects the same normalized org across different
    # addresses (ZIPs) within a state — the D-org case the zip3 rule misses. The
    # entity-type guard keeps person records out of org candidate pairs. Blocks on
    # EXACT normalized_org + state, NOT a first-word phonetic: measured on real
    # spike data, (org_name_phonetic, state) exploded pairs ~9,200x (34.1M vs
    # 3.7k) — org_name_phonetic is only the first token's phonetic and state is
    # TX-dominated; (normalized_org, state) is ~1.2x (4.4k).
    # LOCK-STEP: also in blocking.default_blocking_rules() and
    # organization.PREDICTION_BLOCKING_RULES — update all three together.
    "org_normalized_state": (
        "CASE WHEN ri.entity_type IN ('organization', 'committee') "
        "AND ri.normalized_org IS NOT NULL "
        "AND trim(ri.normalized_org) <> '' "
        "AND ri.state IS NOT NULL "
        "AND trim(ri.state) <> '' "
        "THEN lower(trim(ri.normalized_org)) || '|' || "
        "lower(trim(ri.state)) END"
    ),
}

_DROP_TEMP_SQL = "DROP TABLE IF EXISTS blocking_keyed_stage"

_CREATE_TEMP_SQL = """
CREATE TEMP TABLE blocking_keyed_stage (
    source_type varchar(64) NOT NULL,
    source_id varchar(128) NOT NULL,
    entity_type varchar(64) NOT NULL,
    block_key text NOT NULL
) ON COMMIT PRESERVE ROWS
"""

_MATERIALIZE_KEYED_SQL = """
INSERT INTO blocking_keyed_stage (source_type, source_id, entity_type, block_key)
SELECT
    ri.source_type,
    ri.source_id,
    ri.entity_type,
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

# --- Pair staging (bulk-load path) ------------------------------------------
# Pairs are first appended to an UNLOGGED, index-free staging table (fast: no
# per-row unique-index maintenance, no WAL), then promoted to candidate_pairs in
# a single sorted DISTINCT-ON insert. This avoids the per-row ON CONFLICT probe
# into a multi-million-row unique index that made the old batched path scale
# quadratically — without ever dropping the unique constraint (so an interrupted
# run never loses the dedup guarantee, unlike a drop/rebuild trick).

_DROP_PAIR_STAGE_SQL = "DROP TABLE IF EXISTS candidate_pairs_stage"

_CREATE_PAIR_STAGE_SQL = """
CREATE UNLOGGED TABLE candidate_pairs_stage (
    run_id integer NOT NULL,
    source_a_type varchar(64) NOT NULL,
    source_a_id varchar(128) NOT NULL,
    source_b_type varchar(64) NOT NULL,
    source_b_id varchar(128) NOT NULL,
    rule_name text NOT NULL
)
"""

# One statement per rule: self-join the keyed temp table within each eligible
# (size 2..max) block and append every within-block pair to the staging table.
# (a.source_type, a.source_id) < (b.source_type, b.source_id) emits each
# unordered pair once, so a single rule never self-duplicates.
_INSERT_RULE_PAIRS_TO_STAGE_SQL = """
INSERT INTO candidate_pairs_stage (
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
    AND a.entity_type = b.entity_type
    AND (a.source_type, a.source_id) < (b.source_type, b.source_id)
INNER JOIN (
    SELECT block_key
    FROM blocking_keyed_stage
    GROUP BY block_key
    HAVING COUNT(*) BETWEEN 2 AND :max_block_size
) eligible ON eligible.block_key = a.block_key
"""

# Promote staged pairs to the real table. DISTINCT ON collapses cross-rule
# duplicates (a pair found by >1 rule) to one row, so no ON CONFLICT is needed;
# the ORDER BY both drives DISTINCT ON and feeds the unique index sorted rows.
_PROMOTE_PAIR_STAGE_SQL = """
INSERT INTO candidate_pairs (
    run_id,
    source_a_type,
    source_a_id,
    source_b_type,
    source_b_id,
    rule_name
)
SELECT DISTINCT ON (
    run_id, source_a_type, source_a_id, source_b_type, source_b_id
)
    run_id,
    source_a_type,
    source_a_id,
    source_b_type,
    source_b_id,
    rule_name
FROM candidate_pairs_stage
WHERE run_id = :run_id
ORDER BY
    run_id, source_a_type, source_a_id, source_b_type, source_b_id, rule_name
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
    session.execute(
        text(
            """
            CREATE INDEX IF NOT EXISTS ix_resolution_input_run_first_last_phonetic
            ON resolution_input (run_id, first_name_phonetic, last_name_phonetic)
            WHERE first_name_phonetic IS NOT NULL AND last_name_phonetic IS NOT NULL
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


# The promote insert is the one step that touches the pair unique index. Dropping
# the uniqueness object for just that insert turns ~25M incremental index probes
# into a plain append + one set-based index build on re-add. Safe because the
# promote source is already deduped (DISTINCT ON), so the rebuild never finds a
# conflict, and it runs inside a try/finally that always restores the constraint.
_DROP_PAIR_UNIQUE_CONSTRAINT_SQL = (
    "ALTER TABLE candidate_pairs "
    "DROP CONSTRAINT IF EXISTS uq_candidate_pairs_run_sources"
)
_DROP_PAIR_UNIQUE_INDEX_SQL = "DROP INDEX IF EXISTS uq_candidate_pairs_run_sources"
_ADD_PAIR_UNIQUE_CONSTRAINT_SQL = (
    "ALTER TABLE candidate_pairs "
    "ADD CONSTRAINT uq_candidate_pairs_run_sources "
    "UNIQUE (run_id, source_a_type, source_a_id, source_b_type, source_b_id)"
)


def _drop_candidate_pair_unique(session: Session) -> None:
    """Drop the pair uniqueness object (table constraint or bare index) if present."""
    session.execute(text(_DROP_PAIR_UNIQUE_CONSTRAINT_SQL))
    session.execute(text(_DROP_PAIR_UNIQUE_INDEX_SQL))
    session.commit()


def _restore_candidate_pair_unique(session: Session) -> None:
    """Re-add the pair unique constraint (single set-based build; revalidates dedup).

    Normalizes the uniqueness object back to a table constraint (matching the
    SQLModel definition) regardless of whether it started as a constraint or a
    bare index. A no-op-safe ``ensure_candidate_pair_unique_index`` fallback would
    only re-create an index, so this is the canonical restore.
    """
    # Clear any aborted transaction (e.g. a failed promote) so the DDL can run.
    session.rollback()
    session.execute(text(_ADD_PAIR_UNIQUE_CONSTRAINT_SQL))
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


def _stage_rule_pairs(
    session: Session,
    *,
    run_id: int,
    rule: BlockingRule,
    block_key_sql: str,
    max_block_size: int,
) -> int:
    """Append every eligible within-block pair for one rule to the staging table.

    A single set-based INSERT...SELECT into the index-free UNLOGGED staging
    table — no per-row index maintenance, no ON CONFLICT probe. Returns the
    number of staged rows (pre cross-rule dedup).
    """
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

    result = session.execute(
        text(_INSERT_RULE_PAIRS_TO_STAGE_SQL),
        {
            "run_id": run_id,
            "rule_name": rule.name,
            "max_block_size": max_block_size,
        },
    )
    staged = int(result.rowcount or 0)
    session.execute(text(_DROP_TEMP_SQL))
    session.commit()
    LOGGER.info("Blocking rule=%s staged %s pairs", rule.name, staged)
    return staged


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
    """Postgres blocking backend: stage pairs index-free, then promote deduped.

    Each rule's eligible within-block pairs are appended to an UNLOGGED,
    index-free staging table (fast set-based INSERT...SELECT, no per-row unique
    index maintenance). A single sorted ``DISTINCT ON`` insert then promotes
    them to ``candidate_pairs``, collapsing cross-rule duplicates and feeding the
    unique index in key order. The unique constraint is never dropped, so an
    interrupted run cannot leave the table without its dedup guarantee.
    """
    max_block_size = int(config.get("max_block_size", 100))
    max_pairs_raw = config.get("max_pairs_per_run")
    max_pairs_per_run = int(max_pairs_raw) if max_pairs_raw is not None else None
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

    # Fresh index-free staging table for this run's pairs.
    session.execute(text(_DROP_PAIR_STAGE_SQL))
    session.execute(text(_CREATE_PAIR_STAGE_SQL))
    session.commit()

    try:
        staged_total = 0
        for rule in rules:
            block_key_sql = block_key_sql_for_rule(rule)
            staged_total += _stage_rule_pairs(
                session,
                run_id=run_id,
                rule=rule,
                block_key_sql=block_key_sql,
                max_block_size=max_block_size,
            )

        LOGGER.info(
            "Blocking staged %s pairs across %s rules; promoting (deduped)",
            staged_total,
            len(rules),
        )
        # Drop the pair unique object for just the promote insert, then restore it
        # with one set-based build. The finally guarantees the constraint comes
        # back even on error, so the table never persists without its guarantee.
        _drop_candidate_pair_unique(session)
        try:
            promoted = session.execute(
                text(_PROMOTE_PAIR_STAGE_SQL),
                {"run_id": run_id},
            )
            session.commit()
            LOGGER.info(
                "Blocking promoted %s deduped pairs to candidate_pairs",
                int(promoted.rowcount or 0),
            )
        finally:
            _restore_candidate_pair_unique(session)
    finally:
        session.execute(text(_DROP_PAIR_STAGE_SQL))
        session.commit()

    if max_pairs_per_run is not None:
        _apply_pair_cap(session, run_id=run_id, max_pairs_per_run=max_pairs_per_run)
        session.commit()

    pair_count = session.exec(
        select(func.count())
        .select_from(CandidatePair)
        .where(CandidatePair.run_id == run_id)
    ).one()
    return {"pairs_compared": int(pair_count)}
