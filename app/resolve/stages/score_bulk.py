"""Bulk Postgres COPY helpers for the score stage.

Extracted from ``score.py`` as part of the score-decomposition refactor.
Callers in the still-monolithic ``score.py`` import these public functions;
``_copy_scored_postgres`` and the COPY SQL constant are private to this module.

Task: score-decomposition Task 2 | Plan: 2026-06-20-score-decomposition.md
"""

from __future__ import annotations

import csv
import io
from typing import Any

from sqlalchemy import insert, text
from sqlmodel import Session

from app.resolve.stages.scored_pair import SCORED_COLS, ScoredPair

__all__ = [
    "COPY_SCORED_SQL",
    "bulk_insert_scored",
    "create_scored_indexes",
    "drop_scored_indexes",
    "ensure_scored_unlogged",
]

# Static COPY statement — no interpolation (identifiers are literal).
COPY_SCORED_SQL = (
    "COPY scored_pairs "
    "(run_id, source_a_type, source_a_id, source_b_type, source_b_id, "
    "entity_type, score, explanation_json) "
    "FROM STDIN WITH (FORMAT csv)"
)


def _copy_scored_postgres(session: Session, rows: list[dict[str, Any]]) -> None:
    """Fast-path bulk load of ``scored_pairs`` via PostgreSQL COPY (psycopg2)."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    for r in rows:
        writer.writerow([r[c] for c in SCORED_COLS])
    buf.seek(0)
    raw = session.connection().connection.driver_connection
    cur = raw.cursor()
    try:
        cur.copy_expert(COPY_SCORED_SQL, buf)
    finally:
        cur.close()


def bulk_insert_scored(session: Session, rows: list[dict[str, Any]]) -> None:
    """Persist a batch of ``scored_pairs`` rows.

    On Postgres this uses COPY (orders of magnitude faster than executemany at
    25M+ rows); on other backends (sqlite in tests) it falls back to a Core
    executemany.
    """
    if not rows:
        return
    if session.get_bind().dialect.name == "postgresql":
        _copy_scored_postgres(session, rows)
    else:
        session.execute(insert(ScoredPair.__table__), rows)
    session.commit()


def drop_scored_indexes(session: Session) -> None:
    """Drop scored_pairs secondary indexes before a bulk load (Postgres)."""
    bind = session.connection()
    for ix in list(ScoredPair.__table__.indexes):
        ix.drop(bind, checkfirst=True)
    session.commit()


def create_scored_indexes(session: Session) -> None:
    """Rebuild scored_pairs secondary indexes after a bulk load (Postgres)."""
    bind = session.connection()
    for ix in list(ScoredPair.__table__.indexes):
        ix.create(bind, checkfirst=True)
    session.commit()


# Static DDL — make scored_pairs UNLOGGED (no WAL) for fast bulk loads.
#
# scored_pairs is a fully-regenerable resolve intermediate (re-run the score stage
# to rebuild it), so WAL durability buys nothing here. UNLOGGED COPY/INSERT is ~3x
# faster (measured 509k vs 163k rows/s). It is left UNLOGGED PERMANENTLY: restoring
# LOGGED afterwards rewrites the entire table to WAL, which measured NET SLOWER
# (0.6x) than never going unlogged at all. The conditional check avoids any table
# rewrite on re-runs (SET UNLOGGED only when currently permanent). On an unclean
# Postgres shutdown the table is truncated — acceptable, and consistent with the
# UNLOGGED candidate_pairs_stage the blocking stage already uses.
_SET_UNLOGGED_SQL = text("ALTER TABLE scored_pairs SET UNLOGGED")
_RELPERSISTENCE_SQL = text("SELECT relpersistence FROM pg_class WHERE relname = 'scored_pairs'")


def ensure_scored_unlogged(session: Session) -> None:
    """Make scored_pairs UNLOGGED once (Postgres); no-op if already unlogged."""
    row = session.exec(_RELPERSISTENCE_SQL).first()
    if row is None:
        return
    persistence = row[0] if isinstance(row, (tuple, list)) else row
    if persistence == "p":  # 'p' = permanent (logged); 'u' = unlogged
        session.exec(_SET_UNLOGGED_SQL)
        session.commit()
