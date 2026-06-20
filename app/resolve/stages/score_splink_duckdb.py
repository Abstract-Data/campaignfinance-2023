"""DuckDB staging, exact-pair prediction, and Postgres COPY write path."""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Iterator
from typing import Any

import pandas as pd
from sqlmodel import Session, select

from app.resolve.blocking import CandidatePair
from app.resolve.stages.score_bulk import COPY_SCORED_SQL, bulk_insert_scored
from app.resolve.stages.score_splink_common import (
    FALLBACK_SCORE,
    PAIR_STREAM_SIZE,
    build_uid,
    scored_row,
)
from app.resolve.stages.score_splink_config import train_and_score_pair
from app.resolve.stages.scored_pair import SCORED_PAIR_BATCH_SIZE
from app.resolve.standardize.staging import ResolutionInput

LOGGER = logging.getLogger(__name__)

DUCKDB_MEMORY_LIMIT = os.environ.get("RESOLVE_DUCKDB_MEMORY_LIMIT", "6GB")

__all__ = [
    "duckdb_tmp_root",
    "iter_type_pairs",
    "load_type_uids",
    "predict_exact_pairs",
    "write_scored_via_pg",
]

_CREATE_CAND_PAIRS_SQL = (
    "CREATE TABLE cand_pairs "
    "(uid_l VARCHAR, uid_r VARCHAR, "
    "a_type VARCHAR, a_id VARCHAR, b_type VARCHAR, b_id VARCHAR)"
)
_CAND_PAIR_COLS = ["uid_l", "uid_r", "a_type", "a_id", "b_type", "b_id"]
_JOIN_SCORES_SQL = (
    "SELECT c.a_type, c.a_id, c.b_type, c.b_id, p.* "
    "FROM cand_pairs c "
    "LEFT JOIN pred_out p "
    "ON p.unique_id_l = c.uid_l AND p.unique_id_r = c.uid_r"
)
_BLOCKED_ID_PAIRS_SQL = (
    "SELECT uid_l AS join_key_l, uid_r AS join_key_r, 0 AS match_key FROM cand_pairs"
)
_PRED_NARROW_TF_SQL = (
    "CREATE TABLE pred_narrow AS SELECT unique_id_l, unique_id_r, "
    "match_probability, COLUMNS('^gamma_'), COLUMNS('^bf_tf_adj_') FROM pred_out"
)
_PRED_NARROW_NOTF_SQL = (
    "CREATE TABLE pred_narrow AS SELECT unique_id_l, unique_id_r, "
    "match_probability, COLUMNS('^gamma_') FROM pred_out"
)
_PG_WRITE_CHUNKS = 8
_CHUNK_CSV_PATH = "/tmp/resolve_scored_chunk.csv"
_PG_BUILD_TO_CSV_SQL = (
    "COPY ("
    "WITH j AS ("
    "  SELECT c.a_type, c.a_id, c.b_type, c.b_id, "
    "         p.match_probability AS prob, to_json(p) AS pj "
    "  FROM cand_pairs c JOIN pred_narrow p "
    "    ON p.unique_id_l = c.uid_l AND p.unique_id_r = c.uid_r "
    "  WHERE abs(hash(c.uid_l)) % 8 = ?"
    "), g AS ("
    "  SELECT j.a_type, j.a_id, j.b_type, j.b_id, j.prob, "
    "         substr(key, 7) AS col, "
    "         json_extract(pj, '$.' || key) AS gamma_j, "
    "         json_extract(pj, '$.bf_tf_adj_' || substr(key, 7)) AS tf_j "
    "  FROM j, UNNEST(json_keys(pj)) AS t(key) "
    "  WHERE key LIKE 'gamma_%'"
    "), entries AS ("
    "  SELECT g.a_type, g.a_id, g.b_type, g.b_id, g.prob, g.col, "
    "    json_merge_patch("
    "      CASE WHEN m.col IS NOT NULL "
    "        THEN json_object('gamma', CAST(g.gamma_j AS BIGINT), "
    "                         'label', m.label, 'm', m.m, 'u', m.u, 'bf', m.bf) "
    "        ELSE json_object('gamma', CAST(g.gamma_j AS BIGINT)) END, "
    "      json_object('bf_tf_adj', CASE WHEN g.tf_j IS NULL OR g.tf_j = 'null' "
    "                  THEN NULL ELSE CAST(g.tf_j AS DOUBLE) END)) AS entry "
    "  FROM g LEFT JOIN comp_meta_lkp m "
    "    ON m.col = g.col "
    "   AND CAST(m.vector_value AS BIGINT) = CAST(g.gamma_j AS BIGINT) "
    "  WHERE g.gamma_j IS NOT NULL AND g.gamma_j != 'null'"
    "), grouped AS ("
    "  SELECT a_type, a_id, b_type, b_id, "
    "         greatest(0.0, least(1.0, any_value(prob))) AS score, "
    "         CAST(json_group_object(col, entry) AS VARCHAR) AS explanation_json "
    "  FROM entries GROUP BY a_type, a_id, b_type, b_id"
    ") "
    "SELECT sp.run_id, gr.a_type, gr.a_id, gr.b_type, gr.b_id, sp.entity_type, "
    "       gr.score, gr.explanation_json "
    "FROM grouped gr CROSS JOIN score_params sp"
    ") TO '/tmp/resolve_scored_chunk.csv' (FORMAT csv, HEADER false)"
)
_PG_MISSED_PAIRS_SQL = (
    "SELECT c.a_type, c.a_id, c.b_type, c.b_id "
    "FROM cand_pairs c LEFT JOIN pred_narrow p "
    "ON p.unique_id_l = c.uid_l AND p.unique_id_r = c.uid_r "
    "WHERE p.match_probability IS NULL"
)


def duckdb_tmp_root() -> str:
    """Directory for the per-entity on-disk DuckDB + its spill files."""
    return os.environ.get("RESOLVE_DUCKDB_TMP", tempfile.gettempdir())


def load_type_uids(session: Session, run_id: int, entity_type: str) -> set[str]:
    """All ``unique_id`` values for one entity_type in a run (for pair routing)."""
    rows = session.exec(
        select(ResolutionInput.source_type, ResolutionInput.source_id).where(
            ResolutionInput.run_id == run_id,
            ResolutionInput.entity_type == entity_type,
        )
    ).all()
    return {build_uid(st, sid) for (st, sid) in rows}


def iter_type_pairs(
    session: Session,
    run_id: int,
    type_uids: set[str],
) -> Iterator[tuple[str, str, str, str, str, str]]:
    """Stream candidate_pairs for a run, routing each to its entity_type."""
    stmt = (
        select(
            CandidatePair.source_a_type,
            CandidatePair.source_a_id,
            CandidatePair.source_b_type,
            CandidatePair.source_b_id,
        )
        .where(CandidatePair.run_id == run_id)
        .execution_options(yield_per=PAIR_STREAM_SIZE)
    )
    for a_type, a_id, b_type, b_id in session.exec(stmt):
        uid_a = build_uid(a_type, a_id)
        if uid_a not in type_uids:
            continue
        uid_b = build_uid(b_type, b_id)
        if uid_a <= uid_b:
            yield a_type, a_id, b_type, b_id, uid_a, uid_b
        else:
            yield a_type, a_id, b_type, b_id, uid_b, uid_a


def predict_exact_pairs(linker: Any, con: Any) -> Any:
    """Score EXACTLY the pairs in the DuckDB ``cand_pairs`` table."""
    from splink.internals.comparison_vector_values import (
        compute_comparison_vector_values_from_id_pairs_sqls,
    )
    from splink.internals.pipeline import CTEPipeline
    from splink.internals.predict import (
        predict_from_comparison_vectors_sqls_using_settings,
    )
    from splink.internals.vertically_concatenate import compute_df_concat_with_tf

    s = linker._settings_obj
    db_api = linker._db_api
    ci = s.column_info_settings

    df_concat = compute_df_concat_with_tf(linker, CTEPipeline())
    pipeline = CTEPipeline([df_concat])
    pipeline.enqueue_sql(_BLOCKED_ID_PAIRS_SQL, "__splink__blocked_id_pairs")
    blocked = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    pipeline = CTEPipeline([blocked, df_concat])
    pipeline.enqueue_list_of_sqls(
        compute_comparison_vector_values_from_id_pairs_sqls(
            s._columns_to_select_for_blocking,
            s._columns_to_select_for_comparison_vector_values,
            input_tablename_l="__splink__df_concat_with_tf",
            input_tablename_r="__splink__df_concat_with_tf",
            source_dataset_input_column=ci.source_dataset_input_column,
            unique_id_input_column=ci.unique_id_input_column,
            link_type=s._link_type,
            sql_dialect_str=linker._sql_dialect_str,
        )
    )
    pipeline.enqueue_list_of_sqls(
        predict_from_comparison_vectors_sqls_using_settings(
            s,
            0.0,
            None,
            sql_infinity_expression=linker._infinity_expression,
        )
    )
    return db_api.sql_pipeline_to_splink_dataframe(pipeline)


def _comp_meta_rows(comp_meta: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return [
        {
            "col": col,
            "vector_value": lvl["vector_value"],
            "label": lvl["label"],
            "m": lvl["m"],
            "u": lvl["u"],
            "bf": lvl["bf"],
        }
        for col, lvls in comp_meta.items()
        for lvl in lvls
    ]


def write_scored_via_pg(
    con: Any,
    session: Session,
    linker: Any,
    comp_meta: dict[str, list[dict[str, Any]]],
    rec_by_uid: dict[str, dict[str, Any]],
    run_id: int,
    entity_type: str,
    n_pairs: int,
) -> int:
    """Write scored pairs via DuckDB JSON build + Postgres COPY."""
    lkp = pd.DataFrame(_comp_meta_rows(comp_meta))
    lkp = lkp.astype(object).where(pd.notnull(lkp), None)
    con.register("comp_meta_lkp", lkp)
    con.register(
        "score_params",
        pd.DataFrame([{"run_id": run_id, "entity_type": entity_type}]),
    )

    pred_cols = [r[0] for r in con.execute("DESCRIBE pred_out").fetchall()]
    has_tf = any(c.startswith("bf_tf_adj_") for c in pred_cols)
    con.execute(_PRED_NARROW_TF_SQL if has_tf else _PRED_NARROW_NOTF_SQL)

    raw = session.connection().connection.driver_connection
    written = 0
    try:
        for bucket in range(_PG_WRITE_CHUNKS):
            con.execute(_PG_BUILD_TO_CSV_SQL, [bucket])
            with open(_CHUNK_CSV_PATH) as fh:
                cur = raw.cursor()
                try:
                    cur.copy_expert(COPY_SCORED_SQL, fh)
                    written += cur.rowcount
                finally:
                    cur.close()
            session.commit()
    finally:
        if os.path.exists(_CHUNK_CSV_PATH):
            os.remove(_CHUNK_CSV_PATH)

    missed = con.execute(_PG_MISSED_PAIRS_SQL).fetchall()
    if missed:
        LOGGER.info(
            "predict() missed %s/%s pairs for entity_type=%r; using compare_two_records",
            len(missed),
            n_pairs,
            entity_type,
        )
        buf: list[dict[str, Any]] = []
        for a_type, a_id, b_type, b_id in missed:
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


# Module-internal aliases for streaming orchestration.
create_cand_pairs_sql = _CREATE_CAND_PAIRS_SQL
cand_pair_cols = _CAND_PAIR_COLS
join_scores_sql = _JOIN_SCORES_SQL
