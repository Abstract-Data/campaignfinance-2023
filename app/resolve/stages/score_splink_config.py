"""Splink config loading, training, and explanation helpers."""

from __future__ import annotations

import importlib
import logging
import types
from collections.abc import Mapping
from typing import Any

import pandas as pd

from app.resolve.stages.score_splink_common import (
    FALLBACK_SCORE,
    MIN_RECORDS_FOR_EM,
    build_uid,
    row_to_dict,
    scored_row,
)

LOGGER = logging.getLogger(__name__)

__all__ = [
    "build_explanation",
    "extract_comp_meta",
    "linker_settings_obj",
    "load_entity_config",
    "train_and_score_pair",
    "train_linker",
]


def load_entity_config(entity_type: str) -> types.ModuleType | None:
    """Import ``app.resolve.splink_config.<entity_type>`` dynamically."""
    try:
        return importlib.import_module(f"app.resolve.splink_config.{entity_type}")
    except ModuleNotFoundError:
        LOGGER.warning(
            "No splink_config module for entity_type=%r; skipping scoring",
            entity_type,
        )
        return None


def linker_settings_obj(linker: Any) -> Any | None:
    """Return Splink ``Settings`` for trained comparison metadata, if available."""
    public = getattr(linker, "settings", None)
    if public is not None:
        return public
    try:
        return linker._settings_obj
    except AttributeError:
        LOGGER.warning(
            "Could not read Splink settings from linker; explanation metadata omitted",
        )
        return None


def extract_comp_meta(settings_obj: Any) -> dict[str, list[dict[str, Any]]]:
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


def build_explanation(
    pred_row: Mapping[str, Any] | pd.Series,
    comp_meta: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Serialise a per-comparison contribution breakdown from a prediction row."""
    explanation: dict[str, Any] = {}
    for col, levels in comp_meta.items():
        gamma_col = f"gamma_{col}"
        if gamma_col not in pred_row or pred_row[gamma_col] is None:
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
        tf_adj_col = f"bf_tf_adj_{col}"
        if tf_adj_col in pred_row and pred_row[tf_adj_col] is not None:
            entry["bf_tf_adj"] = float(pred_row[tf_adj_col])
        explanation[col] = entry
    return explanation


def train_and_score_pair(
    linker: Any,
    rec_a: dict[str, Any],
    rec_b: dict[str, Any],
) -> tuple[float, dict[str, Any]]:
    """Score a single pair via ``compare_two_records`` and return (score, explanation)."""
    result_df = linker.inference.compare_two_records(rec_a, rec_b).as_pandas_dataframe()
    if result_df.empty:
        return FALLBACK_SCORE, {"note": "no_comparison_result"}
    row = result_df.iloc[0]
    score = float(row.get("match_probability", FALLBACK_SCORE))
    settings_obj = linker_settings_obj(linker)
    comp_meta = extract_comp_meta(settings_obj) if settings_obj is not None else {}
    explanation = build_explanation(row, comp_meta)
    return max(0.0, min(1.0, score)), explanation


def train_linker(
    df: pd.DataFrame,
    config: types.ModuleType,
    seed: int,
    db_api: Any,
) -> Any:
    """Build and train a Splink linker for one entity type on ``db_api``."""
    from splink import Linker, SettingsCreator

    settings = SettingsCreator(
        link_type="dedupe_only",
        comparisons=config.COMPARISONS,
        blocking_rules_to_generate_predictions=config.PREDICTION_BLOCKING_RULES,
        unique_id_column_name="unique_id",
        retain_intermediate_calculation_columns=True,
    )
    linker = Linker(df, settings, db_api)

    if len(df) >= MIN_RECORDS_FOR_EM:
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
                config.__name__.rsplit(".", maxsplit=1)[-1],
                exc_info=True,
            )
    return linker


# Re-export row helpers used by duckdb/streaming modules.
__all__ += ["build_uid", "row_to_dict", "scored_row"]
