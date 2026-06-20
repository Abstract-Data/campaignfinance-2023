"""Splink/DuckDB scoring harness for the score stage.

Public API:
  - ``load_entity_config`` — dynamically import a per-entity Splink config module.
  - ``score_entity_type`` — score one entity type (handles missing config).
  - ``score_entity_type_streaming`` — train and score one configured entity type at scale.

Task: score-decomposition Task 3 | Plan: 2026-06-20-score-decomposition.md
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import types
from typing import Any

import pandas as pd
from sqlmodel import Session, select

from app.resolve.stages.score_bulk import bulk_insert_scored
from app.resolve.stages.score_splink_common import (
    FALLBACK_SCORE,
    PAIR_STREAM_SIZE,
    PREDICT_STREAM_SIZE,
    scored_row,
)
from app.resolve.stages.score_splink_config import (
    build_explanation,
    build_uid,
    extract_comp_meta,
    linker_settings_obj,
    load_entity_config,
    row_to_dict,
    train_and_score_pair,
    train_linker,
)
from app.resolve.stages.score_splink_duckdb import (
    DUCKDB_MEMORY_LIMIT,
    cand_pair_cols,
    create_cand_pairs_sql,
    duckdb_tmp_root,
    iter_type_pairs,
    join_scores_sql,
    load_type_uids,
    predict_exact_pairs,
    write_scored_via_pg,
)
from app.resolve.stages.scored_pair import SCORED_PAIR_BATCH_SIZE
from app.resolve.standardize.staging import ResolutionInput

LOGGER = logging.getLogger(__name__)

__all__ = [
    "load_entity_config",
    "score_entity_type",
    "score_entity_type_streaming",
    "score_unconfigured_type",
]


def score_unconfigured_type(session: Session, run_id: int, entity_type: str) -> int:
    """Write zero-score ``no_config`` rows for an entity_type with no Splink config."""
    type_uids = load_type_uids(session, run_id, entity_type)
    if not type_uids:
        return 0
    rows = [
        scored_row(
            run_id, a_type, a_id, b_type, b_id, entity_type, FALLBACK_SCORE, {"note": "no_config"}
        )
        for a_type, a_id, b_type, b_id, _uid_l, _uid_r in iter_type_pairs(
            session, run_id, type_uids
        )
    ]
    written = 0
    for offset in range(0, len(rows), SCORED_PAIR_BATCH_SIZE):
        batch = rows[offset : offset + SCORED_PAIR_BATCH_SIZE]
        bulk_insert_scored(session, batch)
        written += len(batch)
    return written


def score_entity_type(
    session: Session,
    run_id: int,
    entity_type: str,
    seed: int,
) -> int:
    """Score one entity type, including the no-config fallback path."""
    cfg = load_entity_config(entity_type)
    if cfg is None:
        return score_unconfigured_type(session, run_id, entity_type)
    return score_entity_type_streaming(session, run_id, entity_type, cfg, seed)


def score_entity_type_streaming(
    session: Session,
    run_id: int,
    entity_type: str,
    config: types.ModuleType,
    seed: int,
) -> int:
    """Train a per-entity Splink model and score its candidate pairs at scale."""
    import duckdb
    from splink import DuckDBAPI

    rec_dicts: list[dict[str, Any]] = []
    type_uids: set[str] = set()
    rec_stmt = (
        select(ResolutionInput)
        .where(
            ResolutionInput.run_id == run_id,
            ResolutionInput.entity_type == entity_type,
        )
        .execution_options(yield_per=PAIR_STREAM_SIZE)
    )
    for rec in session.exec(rec_stmt):
        d = row_to_dict(rec)
        rec_dicts.append(d)
        type_uids.add(d["unique_id"])

    if not rec_dicts:
        return 0
    df = pd.DataFrame(rec_dicts)
    if "employer" in df.columns:
        df["employer"] = df["employer"].astype("string")
    rec_by_uid = {d["unique_id"]: d for d in rec_dicts}
    del rec_dicts

    tmp_dir = tempfile.mkdtemp(prefix=f"resolve_score_{entity_type}_", dir=duckdb_tmp_root())
    con: Any = None
    written = 0
    try:
        con = duckdb.connect(
            os.path.join(tmp_dir, "score.duckdb"),
            config={
                "memory_limit": DUCKDB_MEMORY_LIMIT,
                "temp_directory": tmp_dir,
                "preserve_insertion_order": "false",
            },
        )
        db_api = DuckDBAPI(con)

        linker = train_linker(df, config, seed, db_api)
        settings_obj = linker_settings_obj(linker)
        comp_meta = extract_comp_meta(settings_obj) if settings_obj is not None else {}

        con.execute(create_cand_pairs_sql)
        n_pairs = 0
        stage_buf: list[tuple[str, str, str, str, str, str]] = []
        for a_type, a_id, b_type, b_id, uid_l, uid_r in iter_type_pairs(
            session, run_id, type_uids
        ):
            stage_buf.append((uid_l, uid_r, a_type, a_id, b_type, b_id))
            if len(stage_buf) >= PAIR_STREAM_SIZE:
                con.append("cand_pairs", pd.DataFrame(stage_buf, columns=cand_pair_cols))
                n_pairs += len(stage_buf)
                stage_buf.clear()
        if stage_buf:
            con.append("cand_pairs", pd.DataFrame(stage_buf, columns=cand_pair_cols))
            n_pairs += len(stage_buf)
        if n_pairs == 0:
            return 0

        try:
            pred = predict_exact_pairs(linker, con)
        except Exception:  # pragma: no cover - version-drift safety net
            LOGGER.warning(
                "Exact-pair scoring unavailable for entity_type=%r; "
                "falling back to full predict() (uncapped re-blocking)",
                entity_type,
                exc_info=True,
            )
            pred = linker.inference.predict(threshold_match_probability=0.0)
        con.register("pred_out", pred.as_duckdbpyrelation())

        if comp_meta and session.get_bind().dialect.name == "postgresql":
            return write_scored_via_pg(
                con, session, linker, comp_meta, rec_by_uid, run_id, entity_type, n_pairs
            )

        rel = con.sql(join_scores_sql)
        idx = {name: i for i, name in enumerate(rel.columns)}
        meta_cols = [
            c for col in comp_meta for c in (f"gamma_{col}", f"bf_tf_adj_{col}") if c in idx
        ]
        i_prob = idx["match_probability"]
        i_at, i_ai = idx["a_type"], idx["a_id"]
        i_bt, i_bi = idx["b_type"], idx["b_id"]
        meta_idx = [(c, idx[c]) for c in meta_cols]

        buf: list[dict[str, Any]] = []
        miss: list[tuple[str, str, str, str]] = []
        while True:
            rows = rel.fetchmany(PREDICT_STREAM_SIZE)
            if not rows:
                break
            for row in rows:
                prob = row[i_prob]
                if prob is None:
                    miss.append((row[i_at], row[i_ai], row[i_bt], row[i_bi]))
                    continue
                row_map = {c: row[pos] for c, pos in meta_idx}
                explanation = build_explanation(row_map, comp_meta)
                buf.append(
                    scored_row(
                        run_id,
                        row[i_at],
                        row[i_ai],
                        row[i_bt],
                        row[i_bi],
                        entity_type,
                        max(0.0, min(1.0, float(prob))),
                        explanation,
                    )
                )
                if len(buf) >= SCORED_PAIR_BATCH_SIZE:
                    bulk_insert_scored(session, buf)
                    written += len(buf)
                    buf.clear()
        if buf:
            bulk_insert_scored(session, buf)
            written += len(buf)
            buf.clear()

        if miss:
            LOGGER.info(
                "predict() missed %s/%s pairs for entity_type=%r; using compare_two_records",
                len(miss),
                n_pairs,
                entity_type,
            )
            for a_type, a_id, b_type, b_id in miss:
                rec_a = rec_by_uid.get(build_uid(a_type, a_id))
                rec_b = rec_by_uid.get(build_uid(b_type, b_id))
                if rec_a is None or rec_b is None:
                    score, explanation = FALLBACK_SCORE, {"note": "missing_input_record"}
                else:
                    score, explanation = train_and_score_pair(linker, rec_a, rec_b)
                buf.append(
                    scored_row(run_id, a_type, a_id, b_type, b_id, entity_type, score, explanation)
                )
                if len(buf) >= SCORED_PAIR_BATCH_SIZE:
                    bulk_insert_scored(session, buf)
                    written += len(buf)
                    buf.clear()
            if buf:
                bulk_insert_scored(session, buf)
                written += len(buf)
        return written
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:  # pragma: no cover
                LOGGER.debug("Failed to close scoring DuckDB connection", exc_info=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)
