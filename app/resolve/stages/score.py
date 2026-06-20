"""Stage 4: probabilistic record-linkage scoring with Splink.

Reads the ``candidate_pairs`` and ``resolution_input`` staging tables for a
run, scores every candidate pair using a per-entity-type Splink model trained
with EM on the run's own data, and writes results to ``scored_pairs``.

Address comparisons use term-frequency (TF) adjustment so that shared-hub
addresses (registered-agent buildings, large PO Box addresses) contribute
near-zero Bayes weight to the overall score.

Task: 2a | Branch: resolve/phase-2/task-2a-splink-scoring
"""

from __future__ import annotations

import logging
from typing import Any

from sqlmodel import Session, delete, select

from app.resolve.stages.score_bulk import (
    create_scored_indexes,
    drop_scored_indexes,
    ensure_scored_unlogged,
)
from app.resolve.stages.score_splink import score_entity_type
from app.resolve.stages.scored_pair import ScoredPair
from app.resolve.standardize.staging import ResolutionInput

__all__ = ["ScoredPair", "run_score_stage"]

LOGGER = logging.getLogger(__name__)


def run_score_stage(session: Session, run_id: int, config: dict[str, Any]) -> dict[str, Any]:
    """Run Stage 4 probabilistic scoring for one match run."""
    seed: int = int(config.get("seed", 42))

    entity_types = list(
        session.exec(
            select(ResolutionInput.entity_type).where(ResolutionInput.run_id == run_id).distinct()
        ).all()
    )

    swap_indexes = session.get_bind().dialect.name == "postgresql"
    if swap_indexes:
        drop_scored_indexes(session)
        ensure_scored_unlogged(session)

    total_pairs = 0
    try:
        session.exec(delete(ScoredPair).where(ScoredPair.run_id == run_id))
        session.commit()

        for entity_type in entity_types:
            total_pairs += score_entity_type(session, run_id, entity_type, seed)
    finally:
        if swap_indexes:
            create_scored_indexes(session)
    return {"pairs_compared": total_pairs}
