"""Import stability tests for the score stage decomposition.

Verifies:
  - Backward-compatible imports from the legacy ``score`` module continue to work.
  - New dedicated modules are importable directly.

Task: score-decomposition Task 1 | Plan: 2026-06-20-score-decomposition.md
"""

from __future__ import annotations


def test_scored_pair_importable_from_legacy_path():
    from app.resolve.stages.score import ScoredPair, run_score_stage

    assert ScoredPair.__tablename__ == "scored_pairs"
    assert callable(run_score_stage)


def test_scored_pair_importable_from_dedicated_module():
    from app.resolve.stages.scored_pair import ScoredPair

    assert ScoredPair.__tablename__ == "scored_pairs"


def test_score_bulk_exports_insert_helper():
    from app.resolve.stages.score_bulk import bulk_insert_scored

    assert callable(bulk_insert_scored)


def test_score_orchestrator_under_200_loc():
    from pathlib import Path

    loc = len(Path("app/resolve/stages/score.py").read_text().splitlines())
    assert loc <= 200, f"score.py still {loc} lines; target ~150 orchestrator"


def test_score_splink_module_exports_public_api():
    from app.resolve.stages.score_splink import (
        load_entity_config,
        score_entity_type,
        score_entity_type_streaming,
    )

    assert callable(load_entity_config)
    assert callable(score_entity_type)
    assert callable(score_entity_type_streaming)
