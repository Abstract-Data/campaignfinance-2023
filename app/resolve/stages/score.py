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

import csv
import importlib
import io
import json
import logging
import os
import shutil
import tempfile
import types
from collections.abc import Iterator, Mapping
from typing import Any

import pandas as pd
from sqlalchemy import Column, Float, String, Text, insert, text
from sqlmodel import Field, Session, SQLModel, delete, select

from app.resolve.blocking import CandidatePair
from app.resolve.models.resolution import SOURCE_ID_MAX_LENGTH
from app.resolve.standardize.staging import ResolutionInput

LOGGER = logging.getLogger(__name__)

# Minimum records per entity_type needed for EM estimation to be meaningful.
_MIN_RECORDS_FOR_EM = 4

# Fallback score when a pair falls outside Splink's blocking coverage.
_FALLBACK_SCORE = 0.0

# Bulk-insert scored pairs in chunks. On Postgres this is one COPY per chunk
# (psycopg2 executemany degrades to ~per-row INSERTs — ~500 rows/s, hours at
# 25M+; COPY is ~100-1000x faster), so a large chunk amortises COPY overhead.
_SCORED_PAIR_BATCH_SIZE = 50_000

# Column order for the scored_pairs COPY / executemany.
_SCORED_COLS = (
    "run_id",
    "source_a_type",
    "source_a_id",
    "source_b_type",
    "source_b_id",
    "entity_type",
    "score",
    "explanation_json",
)

# Stream candidate_pairs from the source DB in chunks of this size (keeps the
# Python-side working set flat regardless of run size — never list() the table).
_PAIR_STREAM_SIZE = 50_000

# Rows pulled per batch out of the DuckDB scored-pairs join.
_PREDICT_STREAM_SIZE = 50_000

# DuckDB memory ceiling for the per-entity-type linker. The predict() blocking +
# comparison spills to ``temp_directory`` once this is exceeded, so the stage's
# peak RSS stays bounded even at tens of millions of pairs.
_DUCKDB_MEMORY_LIMIT = os.environ.get("RESOLVE_DUCKDB_MEMORY_LIMIT", "6GB")

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
    zip5 = row.zip5 or ""
    first_name = row.first_name or ""
    return {
        "unique_id": _build_uid(row.source_type, row.source_id),
        "source_id": row.source_id,
        "source_type": row.source_type,
        "first_name": first_name,
        "first_initial": first_name[:1].lower() if first_name.strip() else "",
        "first_name_phonetic": row.first_name_phonetic or "",
        "last_name": row.last_name or "",
        "last_name_phonetic": row.last_name_phonetic or "",
        "normalized_org": row.normalized_org or "",
        # Employer feeds the person comparison (splink_config.person). Kept as
        # None (not "") when absent so Splink treats it as NULL — otherwise two
        # employer-less people would JaroWinkler-match "" == "" and get a
        # spurious match boost. Harmless extra column for non-person configs.
        "employer": row.employer,
        "line_1": row.line_1 or "",
        "city": row.city or "",
        "state": row.state or "",
        "zip5": zip5,
        "zip3": zip5[:3] if len(zip5) >= 3 else "",
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
    pred_row: Mapping[str, Any] | pd.Series,
    comp_meta: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Serialise a per-comparison contribution breakdown from a prediction row.

    ``pred_row`` may be a pandas ``Series`` (single-pair ``compare_two_records``
    fallback) or a plain mapping (a row streamed out of the bulk DuckDB join).
    Both support ``in`` / ``[]`` keyed by column name, so the same logic serves
    both paths without ever materialising the whole prediction frame.
    """
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
        # Capture TF-adjusted Bayes factor when present.
        tf_adj_col = f"bf_tf_adj_{col}"
        if tf_adj_col in pred_row and pred_row[tf_adj_col] is not None:
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


def _train_linker(
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
        # Bulk predict omits bf_tf_adj_* unless intermediate columns are retained.
        retain_intermediate_calculation_columns=True,
    )
    linker = Linker(df, settings, db_api)

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
                config.__name__.rsplit(".", maxsplit=1)[-1],
                exc_info=True,
            )
    return linker


def _scored_row(
    run_id: int,
    a_type: str,
    a_id: str,
    b_type: str,
    b_id: str,
    entity_type: str,
    score: float,
    explanation: dict[str, Any],
) -> dict[str, Any]:
    """Build a plain-dict ``scored_pairs`` row for Core bulk insert."""
    return {
        "run_id": run_id,
        "source_a_type": a_type,
        "source_a_id": a_id,
        "source_b_type": b_type,
        "source_b_id": b_id,
        "entity_type": entity_type,
        "score": score,
        "explanation_json": json.dumps(explanation),
    }


# Static COPY statement — no interpolation (identifiers are literal).
_COPY_SCORED_SQL = (
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
        writer.writerow([r[c] for c in _SCORED_COLS])
    buf.seek(0)
    raw = session.connection().connection.driver_connection
    cur = raw.cursor()
    try:
        cur.copy_expert(_COPY_SCORED_SQL, buf)
    finally:
        cur.close()


def _bulk_insert_scored(session: Session, rows: list[dict[str, Any]]) -> None:
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


def _drop_scored_indexes(session: Session) -> None:
    """Drop scored_pairs secondary indexes before a bulk load (Postgres)."""
    bind = session.connection()
    for ix in list(ScoredPair.__table__.indexes):
        ix.drop(bind, checkfirst=True)
    session.commit()


def _create_scored_indexes(session: Session) -> None:
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


def _ensure_scored_unlogged(session: Session) -> None:
    """Make scored_pairs UNLOGGED once (Postgres); no-op if already unlogged."""
    row = session.exec(_RELPERSISTENCE_SQL).first()
    if row is None:
        return
    persistence = row[0] if isinstance(row, (tuple, list)) else row
    if persistence == "p":  # 'p' = permanent (logged); 'u' = unlogged
        session.exec(_SET_UNLOGGED_SQL)
        session.commit()


def _duckdb_tmp_root() -> str:
    """Directory for the per-entity on-disk DuckDB + its spill files.

    Override with ``RESOLVE_DUCKDB_TMP`` to point at a volume with headroom —
    at tens of millions of pairs the predict() intermediates spill here.
    """
    return os.environ.get("RESOLVE_DUCKDB_TMP", tempfile.gettempdir())


def _load_type_uids(session: Session, run_id: int, entity_type: str) -> set[str]:
    """All ``unique_id`` values for one entity_type in a run (for pair routing)."""
    rows = session.exec(
        select(ResolutionInput.source_type, ResolutionInput.source_id).where(
            ResolutionInput.run_id == run_id,
            ResolutionInput.entity_type == entity_type,
        )
    ).all()
    return {_build_uid(st, sid) for (st, sid) in rows}


def _iter_type_pairs(
    session: Session,
    run_id: int,
    type_uids: set[str],
) -> Iterator[tuple[str, str, str, str, str, str]]:
    """Stream candidate_pairs for a run, routing each to its entity_type.

    Yields ``(a_type, a_id, b_type, b_id, uid_l, uid_r)`` where ``uid_l <= uid_r``
    is the normalised key for joining against Splink predictions (dedupe_only
    emits pairs with ``unique_id_l < unique_id_r``). Pairs are filtered to
    ``type_uids`` by the A-side uid — same-type blocking guarantees both sides
    share an entity_type, so A-side membership is sufficient and unique.

    Streams with ``yield_per`` so the pair table is never materialised in Python.
    The caller must not write to ``session`` while consuming this generator
    (a commit would invalidate the server-side cursor).
    """
    stmt = (
        select(
            CandidatePair.source_a_type,
            CandidatePair.source_a_id,
            CandidatePair.source_b_type,
            CandidatePair.source_b_id,
        )
        .where(CandidatePair.run_id == run_id)
        .execution_options(yield_per=_PAIR_STREAM_SIZE)
    )
    for a_type, a_id, b_type, b_id in session.exec(stmt):
        uid_a = _build_uid(a_type, a_id)
        if uid_a not in type_uids:
            continue
        uid_b = _build_uid(b_type, b_id)
        if uid_a <= uid_b:
            yield a_type, a_id, b_type, b_id, uid_a, uid_b
        else:
            yield a_type, a_id, b_type, b_id, uid_b, uid_a


# Static SQL — DuckDB cannot parameterise identifiers, so the prediction output
# is registered under the fixed view name ``pred_out`` and joined with ``p.*``;
# no value is interpolated into any SQL string.
_CREATE_CAND_PAIRS_SQL = (
    "CREATE TABLE cand_pairs "
    "(uid_l VARCHAR, uid_r VARCHAR, "
    "a_type VARCHAR, a_id VARCHAR, b_type VARCHAR, b_id VARCHAR)"
)
# Column order for cand_pairs — used to build the pandas batch for con.append.
# DuckDB's append() appends by position, matching the table definition above.
_CAND_PAIR_COLS = ["uid_l", "uid_r", "a_type", "a_id", "b_type", "b_id"]
_JOIN_SCORES_SQL = (
    "SELECT c.a_type, c.a_id, c.b_type, c.b_id, p.* "
    "FROM cand_pairs c "
    "LEFT JOIN pred_out p "
    "ON p.unique_id_l = c.uid_l AND p.unique_id_r = c.uid_r"
)


# SQL building __splink__blocked_id_pairs from our staged pairs — static, no
# interpolation (cand_pairs is a literal table created in this DuckDB session).
_BLOCKED_ID_PAIRS_SQL = (
    "SELECT uid_l AS join_key_l, uid_r AS join_key_r, 0 AS match_key FROM cand_pairs"
)


def _predict_exact_pairs(linker: Any, con: Any) -> Any:
    """Score EXACTLY the pairs in the DuckDB ``cand_pairs`` table.

    ``linker.inference.predict()`` re-blocks every record on the prediction
    blocking rules *uncapped*, regenerating ~3x our candidate set (e.g. ~70M vs
    25.87M for the 25% person run) and computing comparison vectors for all of
    them. This feeds our candidate pairs straight into Splink's
    comparison-vector → match-probability pipeline instead, scoring only the
    edges we actually keep.

    Uses Splink internals (pinned splink==4.0.16); the caller wraps this in a
    try/except that falls back to ``predict()`` if these private APIs shift.
    Returns a SplinkDataFrame with the same schema as ``predict()`` output
    (unique_id_l/r, match_probability, gamma_*, bf_tf_adj_*).
    """
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

    # TF-augmented record table Splink's comparison pipeline joins against.
    df_concat = compute_df_concat_with_tf(linker, CTEPipeline())

    # Materialise our candidate pairs as the blocked-id-pairs table.
    pipeline = CTEPipeline([df_concat])
    pipeline.enqueue_sql(_BLOCKED_ID_PAIRS_SQL, "__splink__blocked_id_pairs")
    blocked = db_api.sql_pipeline_to_splink_dataframe(pipeline)

    # Comparison vectors → match probabilities, for exactly those pairs.
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


# ---------------------------------------------------------------------------
# Phase 2: write scored_pairs without a Python per-row loop. DuckDB builds
# explanation_json in SQL and writes each chunk to a CSV (native C++ writer);
# Postgres COPYs that CSV in (native C++ reader). Measured attribution at 25.87M:
# JSON build ~4.5min, but DuckDB's own postgres-extension INSERT was ~8min (~10x
# slower than COPY into an UNLOGGED/index-free table) — so we build with DuckDB
# and load with COPY, getting the best of both.
# ---------------------------------------------------------------------------

# Narrow the (wide, ~40-col retain_intermediate) prediction to just what
# explanation_json needs, materialised ONCE as a real table (so predict runs once
# and the chunk loop below re-scans a slim table, not the predict pipeline). The
# COLUMNS('^…') regexes are static and select the gamma_*/bf_tf_adj_* columns
# without naming them (no per-column interpolation). Two variants: configs with a
# TF-adjusted comparison have bf_tf_adj_* columns, others do not (COLUMNS errors
# on no match, so the no-TF variant omits it).
_PRED_NARROW_TF_SQL = (
    "CREATE TABLE pred_narrow AS SELECT unique_id_l, unique_id_r, "
    "match_probability, COLUMNS('^gamma_'), COLUMNS('^bf_tf_adj_') FROM pred_out"
)
_PRED_NARROW_NOTF_SQL = (
    "CREATE TABLE pred_narrow AS SELECT unique_id_l, unique_id_r, "
    "match_probability, COLUMNS('^gamma_') FROM pred_out"
)

# The build is chunked into this many hash buckets on uid_l. A single all-pairs
# build OOMs at 25M+ because json_group_object aggregates one group per pair and
# does not spill (confirmed: even narrow + preserve_insertion_order=false OOMs at
# 16GB on 25.87M groups); chunking bounds the live group count (~n_pairs/8). The
# "% 8" literal in the build SQL must match this value.
_PG_WRITE_CHUNKS = 8

# Fixed path DuckDB writes each chunk's CSV to, then Postgres COPYs it in. Must
# match the literal in _PG_BUILD_TO_CSV_SQL. Entity types are processed serially
# so a single reused path is safe; cleaned up after the loop.
_CHUNK_CSV_PATH = "/tmp/resolve_scored_chunk.csv"

# DuckDB builds one bucket's scored rows and writes them to the chunk CSV (no
# header, so Postgres COPY reads it directly). Reconstructs explanation_json
# identically to _build_explanation: to_json(pred_narrow) → unnest gamma_* keys →
# join comp_meta_lkp → per-comparison entry (gamma always; label/m/u/bf kept,
# incl. nulls, only when a level matched; bf_tf_adj merged only when non-null via
# RFC-7396 json_merge_patch null-deletion) → json_group_object back to one object
# per pair. Column order matches _COPY_SCORED_SQL. run_id/entity_type come from the
# 1-row score_params relation; the bucket is bound as ``?`` (no interpolation).
# The "% 8" literal must match _PG_WRITE_CHUNKS. Validated byte-for-structure
# equal to the Python path.
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

# Candidate pairs predict() did not score (≈0 at scale, since the run's capped
# candidate set is a subset of predict's uncapped re-blocking) → Python fallback.
_PG_MISSED_PAIRS_SQL = (
    "SELECT c.a_type, c.a_id, c.b_type, c.b_id "
    "FROM cand_pairs c LEFT JOIN pred_narrow p "
    "ON p.unique_id_l = c.uid_l AND p.unique_id_r = c.uid_r "
    "WHERE p.match_probability IS NULL"
)


def _comp_meta_rows(comp_meta: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Flatten comp_meta to comp_meta_lkp rows (col, vector_value, label, m, u, bf)."""
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


def _write_scored_via_pg(
    con: Any,
    session: Session,
    linker: Any,
    comp_meta: dict[str, list[dict[str, Any]]],
    rec_by_uid: dict[str, dict[str, Any]],
    run_id: int,
    entity_type: str,
    n_pairs: int,
) -> int:
    """Write this type's scored pairs: DuckDB builds JSON + CSV, Postgres COPYs it.

    Per hash bucket, DuckDB builds the scored rows and writes them to a CSV with
    its native writer (``_PG_BUILD_TO_CSV_SQL``), then Postgres COPYs that file in.
    Returns the number of ``scored_pairs`` rows written. ``cand_pairs`` and
    ``pred_out`` must already exist in ``con``.
    """
    lkp = pd.DataFrame(_comp_meta_rows(comp_meta))
    # None → NULL (object dtype) so json_object emits valid JSON null, never NaN.
    lkp = lkp.astype(object).where(pd.notnull(lkp), None)
    con.register("comp_meta_lkp", lkp)
    con.register(
        "score_params",
        pd.DataFrame([{"run_id": run_id, "entity_type": entity_type}]),
    )

    # Materialise the slim prediction once (predict runs once; chunks re-scan a
    # narrow table, not the predict pipeline). Omit bf_tf_adj_* if the config has
    # none (COLUMNS('^bf_tf_adj_') errors on no match).
    pred_cols = [r[0] for r in con.execute("DESCRIBE pred_out").fetchall()]
    has_tf = any(c.startswith("bf_tf_adj_") for c in pred_cols)
    con.execute(_PRED_NARROW_TF_SQL if has_tf else _PRED_NARROW_NOTF_SQL)

    # Build + load per bucket: DuckDB writes the chunk CSV, Postgres COPYs it in.
    # Chunking bounds the (non-spilling) json_group_object group count; bucket is
    # bound as a parameter (no interpolation).
    raw = session.connection().connection.driver_connection
    written = 0
    try:
        for bucket in range(_PG_WRITE_CHUNKS):
            con.execute(_PG_BUILD_TO_CSV_SQL, [bucket])
            with open(_CHUNK_CSV_PATH) as fh:
                cur = raw.cursor()
                try:
                    cur.copy_expert(_COPY_SCORED_SQL, fh)
                    written += cur.rowcount
                finally:
                    cur.close()
            session.commit()
    finally:
        if os.path.exists(_CHUNK_CSV_PATH):
            os.remove(_CHUNK_CSV_PATH)

    # Fallback for pairs predict() did not cover (≈0 at scale).
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
            rec_a = rec_by_uid.get(_build_uid(a_type, a_id))
            rec_b = rec_by_uid.get(_build_uid(b_type, b_id))
            if rec_a is None or rec_b is None:
                score, explanation = _FALLBACK_SCORE, {"note": "missing_input_record"}
            else:
                score, explanation = _train_and_score_pair(linker, rec_a, rec_b)
            buf.append(
                _scored_row(run_id, a_type, a_id, b_type, b_id, entity_type, score, explanation)
            )
            if len(buf) >= _SCORED_PAIR_BATCH_SIZE:
                _bulk_insert_scored(session, buf)
                written += len(buf)
                buf.clear()
        if buf:
            _bulk_insert_scored(session, buf)
            written += len(buf)
    return written


def _score_entity_type_streaming(
    session: Session,
    run_id: int,
    entity_type: str,
    config: types.ModuleType,
    seed: int,
) -> int:
    """Train a per-entity Splink model and score its candidate pairs at scale.

    Memory stays flat regardless of run size: records stream into a pandas frame
    of small dicts (not ORM rows); candidate_pairs stream into an on-disk DuckDB
    table; ``predict()`` runs against the same on-disk DuckDB (spilling its
    intermediates to ``temp_directory``); and scores stream back out of a DuckDB
    LEFT JOIN in record batches — the full prediction frame is never pulled into
    Python. Pairs Splink's blocking misses (≈0 at scale, since the run's capped
    candidate set is a subset of predict()'s uncapped re-blocking) fall back to
    per-pair ``compare_two_records``.

    Returns the number of ``scored_pairs`` rows written for this entity_type.
    """
    import duckdb
    from splink import DuckDBAPI

    # 1. Stream this type's records into the linker input frame + uid set.
    rec_dicts: list[dict[str, Any]] = []
    type_uids: set[str] = set()
    rec_stmt = (
        select(ResolutionInput)
        .where(
            ResolutionInput.run_id == run_id,
            ResolutionInput.entity_type == entity_type,
        )
        .execution_options(yield_per=_PAIR_STREAM_SIZE)
    )
    for rec in session.exec(rec_stmt):
        d = _row_to_dict(rec)
        rec_dicts.append(d)
        type_uids.add(d["unique_id"])

    if not rec_dicts:
        return 0
    df = pd.DataFrame(rec_dicts)
    # Force employer to a string dtype so DuckDB types it VARCHAR even when the
    # whole batch is NULL (every org row, or a person subset with no employers).
    # An all-None object column infers as INTEGER, and Splink's person comparison
    # SQL fails to *bind* jaro_winkler_similarity(employer_l, employer_r) — even
    # though the NULL-level short-circuits at runtime. "string" keeps NA as NULL.
    if "employer" in df.columns:
        df["employer"] = df["employer"].astype("string")
    rec_by_uid = {d["unique_id"]: d for d in rec_dicts}
    del rec_dicts

    tmp_dir = tempfile.mkdtemp(prefix=f"resolve_score_{entity_type}_", dir=_duckdb_tmp_root())
    con: Any = None
    written = 0
    try:
        # On-disk DuckDB with bounded memory; spills to tmp_dir at scale.
        con = duckdb.connect(
            os.path.join(tmp_dir, "score.duckdb"),
            config={
                "memory_limit": _DUCKDB_MEMORY_LIMIT,
                "temp_directory": tmp_dir,
                # Lower peak memory of the chunked json_group_object build; row
                # order is irrelevant (scored_pairs has no ordering contract).
                "preserve_insertion_order": "false",
            },
        )
        db_api = DuckDBAPI(con)

        linker = _train_linker(df, config, seed, db_api)
        settings_obj = _linker_settings_obj(linker)
        comp_meta = _extract_comp_meta(settings_obj) if settings_obj is not None else {}

        # 2. Stage this type's candidate pairs into DuckDB (streamed; no Python
        #    list). Uses con.append from pandas batches, NOT executemany —
        #    DuckDB executemany is ~per-row (~5k rows/s; ~90min at 25.87M),
        #    append is ~150x faster (measured 738k rows/s; ~35s at 25.87M).
        con.execute(_CREATE_CAND_PAIRS_SQL)
        n_pairs = 0
        stage_buf: list[tuple[str, str, str, str, str, str]] = []
        for a_type, a_id, b_type, b_id, uid_l, uid_r in _iter_type_pairs(
            session, run_id, type_uids
        ):
            stage_buf.append((uid_l, uid_r, a_type, a_id, b_type, b_id))
            if len(stage_buf) >= _PAIR_STREAM_SIZE:
                con.append("cand_pairs", pd.DataFrame(stage_buf, columns=_CAND_PAIR_COLS))
                n_pairs += len(stage_buf)
                stage_buf.clear()
        if stage_buf:
            con.append("cand_pairs", pd.DataFrame(stage_buf, columns=_CAND_PAIR_COLS))
            n_pairs += len(stage_buf)
        if n_pairs == 0:
            return 0

        # 3. Bulk-score EXACTLY our candidate pairs (no uncapped re-blocking),
        #    then LEFT JOIN our pairs and stream out in record batches — never
        #    materialised as one frame. Falls back to the full predict() if the
        #    Splink-internals exact path is unavailable.
        try:
            pred = _predict_exact_pairs(linker, con)
        except Exception:  # pragma: no cover - version-drift safety net
            LOGGER.warning(
                "Exact-pair scoring unavailable for entity_type=%r; "
                "falling back to full predict() (uncapped re-blocking)",
                entity_type,
                exc_info=True,
            )
            pred = linker.inference.predict(threshold_match_probability=0.0)
        con.register("pred_out", pred.as_duckdbpyrelation())

        # 4. Write scored_pairs. On Postgres, INSERT…SELECT straight from DuckDB
        #    into the attached DB, building explanation_json in SQL — no Python
        #    per-row loop (Phase 2). On other backends (sqlite tests), stream the
        #    join out via fetchmany and bulk-insert.
        if comp_meta and session.get_bind().dialect.name == "postgresql":
            return _write_scored_via_pg(
                con, session, linker, comp_meta, rec_by_uid, run_id, entity_type, n_pairs
            )

        # Stream the join with DuckDB-native fetchmany (positional tuples; no
        # pyarrow dependency). Map column names to positions once.
        rel = con.sql(_JOIN_SCORES_SQL)
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
            rows = rel.fetchmany(_PREDICT_STREAM_SIZE)
            if not rows:
                break
            for row in rows:
                prob = row[i_prob]
                if prob is None:
                    miss.append((row[i_at], row[i_ai], row[i_bt], row[i_bi]))
                    continue
                row_map = {c: row[pos] for c, pos in meta_idx}
                explanation = _build_explanation(row_map, comp_meta)
                buf.append(
                    _scored_row(
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
                if len(buf) >= _SCORED_PAIR_BATCH_SIZE:
                    _bulk_insert_scored(session, buf)
                    written += len(buf)
                    buf.clear()
        if buf:
            _bulk_insert_scored(session, buf)
            written += len(buf)
            buf.clear()

        # 4. Fallback for the (rare) pairs predict() did not cover.
        if miss:
            LOGGER.info(
                "predict() missed %s/%s pairs for entity_type=%r; using compare_two_records",
                len(miss),
                n_pairs,
                entity_type,
            )
            for a_type, a_id, b_type, b_id in miss:
                rec_a = rec_by_uid.get(_build_uid(a_type, a_id))
                rec_b = rec_by_uid.get(_build_uid(b_type, b_id))
                if rec_a is None or rec_b is None:
                    score, explanation = _FALLBACK_SCORE, {"note": "missing_input_record"}
                else:
                    score, explanation = _train_and_score_pair(linker, rec_a, rec_b)
                buf.append(
                    _scored_row(run_id, a_type, a_id, b_type, b_id, entity_type, score, explanation)
                )
                if len(buf) >= _SCORED_PAIR_BATCH_SIZE:
                    _bulk_insert_scored(session, buf)
                    written += len(buf)
                    buf.clear()
            if buf:
                _bulk_insert_scored(session, buf)
                written += len(buf)
        return written
    finally:
        if con is not None:
            try:
                con.close()
            except Exception:  # pragma: no cover - best-effort cleanup
                LOGGER.debug("Failed to close scoring DuckDB connection", exc_info=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)


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
        _bulk_insert_scored(session, batch)
        written += len(batch)
    return written


# ---------------------------------------------------------------------------
# Public stage entry-point
# ---------------------------------------------------------------------------


def run_score_stage(session: Session, run_id: int, config: dict) -> dict:
    """Run Stage 4 probabilistic scoring for one match run.

    Scales to tens of millions of candidate pairs: each entity_type is processed
    independently, streaming its records and candidate pairs through an on-disk
    DuckDB Splink model and streaming scores back out — nothing proportional to
    the run size is held in Python memory (see ``_score_entity_type_streaming``).

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
        _drop_scored_indexes(session)
        # WAL-free bulk load: scored_pairs is left UNLOGGED (no WAL, ~3x faster
        # COPY/INSERT). Regenerable intermediate, so durability is not needed.
        _ensure_scored_unlogged(session)

    total_pairs = 0
    try:
        # Clear any previous scored_pairs for this run (indexes already dropped).
        session.exec(delete(ScoredPair).where(ScoredPair.run_id == run_id))
        session.commit()

        for entity_type in entity_types:
            cfg = _load_entity_config(entity_type)
            if cfg is None:
                total_pairs += _score_unconfigured_type(session, run_id, entity_type)
            else:
                total_pairs += _score_entity_type_streaming(session, run_id, entity_type, cfg, seed)
    finally:
        if swap_indexes:
            _create_scored_indexes(session)
    return {"pairs_compared": total_pairs}
