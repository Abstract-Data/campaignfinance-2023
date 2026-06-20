"""ScoredPair SQLModel and column constants for the score stage.

Extracted from ``score.py`` as part of the score-decomposition refactor.
All callers that previously imported from ``app.resolve.stages.score`` continue
to work via re-exports in that module.
"""

from __future__ import annotations

from sqlalchemy import Column, Float, String, Text
from sqlmodel import Field, SQLModel

from app.resolve.models.resolution import SOURCE_ID_MAX_LENGTH

__all__ = ["SCORED_COLS", "SCORED_PAIR_BATCH_SIZE", "ScoredPair"]

# Bulk-insert scored pairs in chunks. On Postgres this is one COPY per chunk
# (psycopg2 executemany degrades to ~per-row INSERTs — ~500 rows/s, hours at
# 25M+; COPY is ~100-1000x faster), so a large chunk amortises COPY overhead.
SCORED_PAIR_BATCH_SIZE = 50_000

# Column order for the scored_pairs COPY / executemany.
SCORED_COLS = (
    "run_id",
    "source_a_type",
    "source_a_id",
    "source_b_type",
    "source_b_id",
    "entity_type",
    "score",
    "explanation_json",
)


class ScoredPair(SQLModel, table=True):
    """One Splink-scored candidate pair.

    Columns conform to the Phase 2 README inter-stage contract:
      run_id, source_a_{type,id}, source_b_{type,id}, entity_type,
      score, explanation_json.
    """

    __tablename__ = "scored_pairs"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_a_type: str = Field(sa_column=Column(String(64), nullable=False))
    source_a_id: str = Field(sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False))
    source_b_type: str = Field(sa_column=Column(String(64), nullable=False))
    source_b_id: str = Field(sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False))
    entity_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    score: float = Field(sa_column=Column(Float, nullable=False))
    explanation_json: str = Field(sa_column=Column(Text, nullable=False))
