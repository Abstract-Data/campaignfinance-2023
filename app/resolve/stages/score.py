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

import importlib
import json
import logging
import types
from typing import Any

import pandas as pd
from sqlalchemy import Column, Float, String, Text
from sqlmodel import Field, Session, SQLModel, delete, select

from app.resolve.blocking import CandidatePair
from app.resolve.models.resolution import SOURCE_ID_MAX_LENGTH
from app.resolve.standardize.staging import ResolutionInput

LOGGER = logging.getLogger(__name__)

# Minimum records per entity_type needed for EM estimation to be meaningful.
_MIN_RECORDS_FOR_EM = 4

# Fallback score when a pair falls outside Splink's blocking coverage.
_FALLBACK_SCORE = 0.0

# ---------------------------------------------------------------------------
# ScoredPair staging table (written by this stage; read by stage-5 classify)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_uid(source_type: str, source_id: str) -> str:
    """Stable unique identifier combining source type and source id."""
    return f"{source_type}:{source_id}"


def _row_to_dict(row: ResolutionInput) -> dict[str, Any]:
    """Flatten a ResolutionInput row to a Splink-compatible dict."""
    return {
        "unique_id": _build_uid(row.source_type, row.source_id),
        "source_id": row.source_id,
        "source_type": row.source_type,
        "first_name": row.first_name or "",
        "last_name": row.last_name or "",
        "last_name_phonetic": row.last_name_phonetic or "",
        "normalized_org": row.normalized_org or "",
        "line_1": row.line_1 or "",
        "city": row.city or "",
        "zip5": row.zip5 or "",
    }


def _load_entity_config(entity_type: str) -> types.ModuleType | None:
    """Import ``app.resolve.splink_config.<entity_type>`` dynamically."""
    try:
        return importlib.import_module(f"app.resolve.splink_config.{entity_type}")
    except ModuleNotFoundError:
        LOGGER.warning(
            "No splink_config module for entity_type=%r; skipping scoring",
            entity_type,
        )
        return None


def _linker_settings_obj(linker: Any) -> Any | None:
    """Return Splink ``Settings`` for trained comparison metadata, if available."""
    public = getattr(linker, "settings", None)
    if public is not None:
        return public
    try:
        # SPLINK-INTERNAL: splink==4.0.16 — Linker has no public settings accessor
        return linker._settings_obj
    except AttributeError:
        LOGGER.warning(
            "Could not read Splink settings from linker; explanation metadata omitted",
        )
        return None


def _extract_comp_meta(settings_obj: Any) -> dict[str, list[dict[str, Any]]]:
    """Extract per-level m/u/bf metadata from a trained Splink settings object."""
    meta: dict[str, list[dict[str, Any]]] = {}
    for comp in settings_obj.comparisons:
        col = comp.output_column_name
        levels = []
        for lvl in comp.comparison_levels:
            if lvl.is_null_level:
                continue
            try:
                m: float | None = float(lvl.m_probability)
            except (ValueError, TypeError):
                m = None
            try:
                u: float | None = float(lvl.u_probability)
            except (ValueError, TypeError):
                u = None
            bf: float | None = (m / u) if (m is not None and u is not None and u > 0) else None
            levels.append(
                {
                    "vector_value": lvl.comparison_vector_value,
                    "label": lvl.label_for_charts,
                    "m": m,
                    "u": u,
                    "bf": bf,
                }
            )
        meta[col] = levels
    return meta


def _build_explanation(
    pred_row: pd.Series,
    comp_meta: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Serialise a per-comparison contribution breakdown from a prediction row."""
    explanation: dict[str, Any] = {}
    for col, levels in comp_meta.items():
        gamma_col = f"gamma_{col}"
        if gamma_col not in pred_row.index:
            continue
        gamma = int(pred_row[gamma_col])
        matched = next(
            (lvl for lvl in levels if lvl["vector_value"] == gamma),
            None,
        )
        entry: dict[str, Any] = {"gamma": gamma}
        if matched:
            entry.update(
                {
                    "label": matched["label"],
                    "m": matched["m"],
                    "u": matched["u"],
                    "bf": matched["bf"],
                }
            )
        # Capture TF-adjusted Bayes factor when present.
        tf_adj_col = f"bf_tf_adj_{col}"
        if tf_adj_col in pred_row.index:
            entry["bf_tf_adj"] = float(pred_row[tf_adj_col])
        explanation[col] = entry
    return explanation


def _train_and_score_pair(
    linker: Any,
    rec_a: dict[str, Any],
    rec_b: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    """Score a single pair via ``compare_two_records`` and return (score, explanation)."""
    result_df = linker.inference.compare_two_records(rec_a, rec_b).as_pandas_dataframe()
    if result_df.empty:
        return _FALLBACK_SCORE, {"note": "no_comparison_result"}
    row = result_df.iloc[0]
    score = float(row.get("match_probability", _FALLBACK_SCORE))
    settings_obj = _linker_settings_obj(linker)
    comp_meta = _extract_comp_meta(settings_obj) if settings_obj is not None else {}
    explanation = _build_explanation(row, comp_meta)
    return max(0.0, min(1.0, score)), explanation


def _score_entity_type(
    pairs: list[CandidatePair],
    records: list[ResolutionInput],
    entity_type: str,
    config: types.ModuleType,
    seed: int,
) -> list[ScoredPair]:
    """Train a per-entity Splink model and score every candidate pair."""
    from splink import DuckDBAPI, Linker, SettingsCreator

    if not pairs:
        return []

    # Build a lookup of uid -> record dict for fast pair joining.
    rec_by_uid: dict[str, dict[str, Any]] = {
        _build_uid(r.source_type, r.source_id): _row_to_dict(r) for r in records
    }

    df = pd.DataFrame(list(rec_by_uid.values()))
    if df.empty:
        return _fallback_scored(pairs, entity_type, "no_records")

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=config.COMPARISONS,
        blocking_rules_to_generate_predictions=config.PREDICTION_BLOCKING_RULES,
        unique_id_column_name="unique_id",
    )
    db_api = DuckDBAPI()
    linker = Linker(df, settings, db_api)

    # EM estimation — skip gracefully when there is not enough data.
    if len(df) >= _MIN_RECORDS_FOR_EM:
        try:
            linker.training.estimate_u_using_random_sampling(
                max_pairs=min(len(df) * (len(df) - 1) // 2, 1_000_000),
                seed=seed,
            )
            linker.training.estimate_parameters_using_expectation_maximisation(
                config.TRAINING_BLOCKING_RULE,
                estimate_without_term_frequencies=True,
            )
        except Exception:
            LOGGER.warning(
                "EM estimation failed for entity_type=%r; using default priors",
                entity_type,
                exc_info=True,
            )

    # Score each candidate pair via compare_two_records.
    scored: list[ScoredPair] = []
    for pair in pairs:
        uid_a = _build_uid(pair.source_a_type, pair.source_a_id)
        uid_b = _build_uid(pair.source_b_type, pair.source_b_id)
        rec_a = rec_by_uid.get(uid_a)
        rec_b = rec_by_uid.get(uid_b)

        if rec_a is None or rec_b is None:
            LOGGER.warning(
                "Missing resolution_input record for pair (%s, %s)",
                uid_a,
                uid_b,
            )
            score = _FALLBACK_SCORE
            explanation: dict[str, Any] = {"note": "missing_input_record"}
        else:
            score, explanation = _train_and_score_pair(linker, rec_a, rec_b)

        scored.append(
            ScoredPair(
                run_id=pair.run_id,
                source_a_type=pair.source_a_type,
                source_a_id=pair.source_a_id,
                source_b_type=pair.source_b_type,
                source_b_id=pair.source_b_id,
                entity_type=entity_type,
                score=score,
                explanation_json=json.dumps(explanation),
            )
        )

    return scored


def _fallback_scored(
    pairs: list[CandidatePair],
    entity_type: str,
    note: str,
) -> list[ScoredPair]:
    """Return zero-scored rows when Splink cannot be run."""
    return [
        ScoredPair(
            run_id=p.run_id,
            source_a_type=p.source_a_type,
            source_a_id=p.source_a_id,
            source_b_type=p.source_b_type,
            source_b_id=p.source_b_id,
            entity_type=entity_type,
            score=_FALLBACK_SCORE,
            explanation_json=json.dumps({"note": note}),
        )
        for p in pairs
    ]


def _entity_type_for_pair(
    pair: CandidatePair,
    input_map: dict[str, ResolutionInput],
) -> str:
    """Look up entity_type by checking either side of the pair."""
    for uid in (
        _build_uid(pair.source_a_type, pair.source_a_id),
        _build_uid(pair.source_b_type, pair.source_b_id),
    ):
        rec = input_map.get(uid)
        if rec is not None:
            return rec.entity_type
    return "unknown"


# ---------------------------------------------------------------------------
# Public stage entry-point
# ---------------------------------------------------------------------------


def run_score_stage(session: Session, run_id: int, config: dict) -> dict:
    """Run Stage 4 probabilistic scoring for one match run.

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

    # Load staging tables.
    pairs: list[CandidatePair] = list(
        session.exec(select(CandidatePair).where(CandidatePair.run_id == run_id)).all()
    )
    inputs: list[ResolutionInput] = list(
        session.exec(select(ResolutionInput).where(ResolutionInput.run_id == run_id)).all()
    )

    input_map: dict[str, ResolutionInput] = {
        _build_uid(r.source_type, r.source_id): r for r in inputs
    }

    # Group candidate pairs by entity_type.
    pairs_by_type: dict[str, list[CandidatePair]] = {}
    for pair in pairs:
        etype = _entity_type_for_pair(pair, input_map)
        pairs_by_type.setdefault(etype, []).append(pair)

    # Group resolution_input records by entity_type.
    inputs_by_type: dict[str, list[ResolutionInput]] = {}
    for rec in inputs:
        inputs_by_type.setdefault(rec.entity_type, []).append(rec)

    # Clear any previous scored_pairs for this run.
    session.exec(delete(ScoredPair).where(ScoredPair.run_id == run_id))
    session.commit()

    total_pairs = 0
    for entity_type, type_pairs in pairs_by_type.items():
        cfg = _load_entity_config(entity_type)
        if cfg is None:
            scored = _fallback_scored(type_pairs, entity_type, "no_config")
        else:
            type_records = inputs_by_type.get(entity_type, [])
            scored = _score_entity_type(type_pairs, type_records, entity_type, cfg, seed)

        session.add_all(scored)
        total_pairs += len(type_pairs)

    session.commit()
    return {"pairs_compared": total_pairs}
