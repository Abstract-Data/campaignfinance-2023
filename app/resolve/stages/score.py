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
    bulk_insert_scored,
    create_scored_indexes,
    drop_scored_indexes,
    ensure_scored_unlogged,
)
from app.resolve.stages.score_splink import (
    _iter_type_pairs,
    _load_type_uids,
    _scored_row,
    load_entity_config,
    score_entity_type_streaming,
)
from app.resolve.stages.scored_pair import ScoredPair, _SCORED_PAIR_BATCH_SIZE
from app.resolve.standardize.staging import ResolutionInput

__all__ = ["ScoredPair", "run_score_stage"]

LOGGER = logging.getLogger(__name__)

# Fallback score when a pair falls outside Splink's blocking coverage.
_FALLBACK_SCORE = 0.0


def _score_unconfigured_type(session: Session, run_id: int, entity_type: str) -> int:
    """Write zero-score ``no_config`` rows for an entity_type with no Splink config."""
    type_uids = _load_type_uids(session, run_id, entity_type)
    if not type_uids:
        return 0
    # Buffer first: we cannot write to ``session`` while its streaming cursor is
    # open. Unconfigured types are rare and small, so full buffering is fine.
    rows = [
        _scored_row(
            run_id, a_type, a_id, b_type, b_id, entity_type, _FALLBACK_SCORE, {"note": "no_config"}
        )
        for a_type, a_id, b_type, b_id, _uid_l, _uid_r in _iter_type_pairs(
            session, run_id, type_uids
        )
    ]
    written = 0
    for offset in range(0, len(rows), _SCORED_PAIR_BATCH_SIZE):
        batch = rows[offset : offset + _SCORED_PAIR_BATCH_SIZE]
        bulk_insert_scored(session, batch)
        written += len(batch)
    return written


def run_score_stage(session: Session, run_id: int, config: dict[str, Any]) -> dict[str, Any]:
    """Run Stage 4 probabilistic scoring for one match run.

    Scales to tens of millions of candidate pairs: each entity_type is processed
    independently, streaming its records and candidate pairs through an on-disk
    DuckDB Splink model and streaming scores back out — nothing proportional to
    the run size is held in Python memory (see ``score_entity_type_streaming``).

    Parameters
    ----------
    session:
        Active SQLModel session connected to the resolve schema.
    run_id:
        The ``match_run.id`` being processed.
    config:
        Run configuration dict.  Optional key ``seed`` (int, default 42)
        seeds Splink's random sampling for deterministic EM.

    Returns
    -------
    dict
        ``{"pairs_compared": <n>}`` where *n* is the total number of
        candidate pairs scored across all entity types.
    """
    seed: int = int(config.get("seed", 42))

    # Entity types present for this run (drives per-type partitioning).
    entity_types = list(
        session.exec(
            select(ResolutionInput.entity_type).where(ResolutionInput.run_id == run_id).distinct()
        ).all()
    )

    # On Postgres, drop the scored_pairs secondary indexes around the whole
    # bulk load (publish-style): index maintenance per COPY otherwise dominates
    # at 25M+ rows, and it also makes the idempotent delete-of-prior-rows below
    # far cheaper on re-runs. Rebuilt in finally so an interrupt never leaves the
    # table unindexed.
    swap_indexes = session.get_bind().dialect.name == "postgresql"
    if swap_indexes:
        drop_scored_indexes(session)
        # WAL-free bulk load: scored_pairs is left UNLOGGED (no WAL, ~3x faster
        # COPY/INSERT). Regenerable intermediate, so durability is not needed.
        ensure_scored_unlogged(session)

    total_pairs = 0
    try:
        # Clear any previous scored_pairs for this run (indexes already dropped).
        session.exec(delete(ScoredPair).where(ScoredPair.run_id == run_id))
        session.commit()

        for entity_type in entity_types:
            cfg = load_entity_config(entity_type)
            if cfg is None:
                total_pairs += _score_unconfigured_type(session, run_id, entity_type)
            else:
                total_pairs += score_entity_type_streaming(
                    session, run_id, entity_type, cfg, seed
                )
    finally:
        if swap_indexes:
            create_scored_indexes(session)
    return {"pairs_compared": total_pairs}
