# TASK — Eliminate the ~31 min scored_pairs write (DuckDB→Postgres)

## Goal
The score stage is now write-bound: full run_id=2 (25,873,623 pairs) is 37 min,
~31 min of it writing `scored_pairs` via a per-row Python loop
(fetchmany tuple → `_build_explanation` → `json.dumps` → `csv.writerow` →
psycopg2 COPY), single-threaded, ×25.87M. Remove that Python per-row cost.

Approved sequencing: **Phase 0 profile → Phase 1 (#2 UNLOGGED) → Phase 2 (#1 DuckDB ATTACH→Postgres)**.
`#1` subsumes `#4` (JSON built in SQL) and makes `#3` (parallel Python COPY) moot.

## Validated facts (de-risked before planning)
- DuckDB 1.5.3 `postgres` extension loads; `ATTACH '' AS pg (TYPE postgres)` with
  an **empty DSN** authenticates via libpq env vars (`PGHOST/PGUSER/PGDATABASE`) —
  static SQL, no interpolation, no `.env` read.
- `INSERT INTO pg.scored_pairs (<explicit cols>) SELECT …` round-trips correctly;
  PG serial `id` auto-fills.
- A **generic, static** SQL JSON build (`to_json(pred_out)` → `json_keys` UNNEST →
  join a `comp_meta_lkp` table → `json_merge_patch` entry → `json_group_object`)
  reproduces `_build_explanation` EXACTLY: gamma always present; label/m/u/bf only
  when a comparison level matched; bf_tf_adj only when non-null (RFC-7396
  null-deletion semantics of `json_merge_patch`). No per-column interpolation →
  does not trip the sql-injection hook. `comp_meta_lkp` is loaded via `con.append`
  (pandas, no SQL).

## Phase 0 — Profile (no commit)
`/tmp` harness times the current write loop on a ~2M person slice: split
`_build_explanation`+`json.dumps` vs `csv.writerow` vs `copy_expert`. Establishes a
baseline and confirms how much #1 can buy. Evidence: printed per-phase rows/s.

## Phase 1 — #2 UNLOGGED load target (Postgres only)  ✅ DONE
- MEASUREMENT (3M-row COPY micro-benchmark, indexes dropped):
  LOGGED COPY 163k rows/s; UNLOGGED COPY 509k rows/s (**3.12x**); but `SET LOGGED`
  restore = +24.6s (full-table WAL rewrite) → swap-and-restore is **0.60x (NET
  SLOWER)**. So the only beneficial form is **permanent UNLOGGED** (no restore).
- `run_score_stage`: on `dialect == "postgresql"`, `_ensure_scored_unlogged`
  (`ALTER TABLE scored_pairs SET UNLOGGED`, conditional on `relpersistence='p'` so
  re-runs don't rewrite) before the bulk-load window. NOT restored to LOGGED.
  `scored_pairs` is a regenerable resolve intermediate (re-run score to rebuild),
  consistent with the UNLOGGED `candidate_pairs_stage` the blocking stage uses.
- Evidence: 14 score tests green (sqlite path unaffected); ruff clean. Benefit
  realized on the Postgres path (COPY today, DuckDB INSERT after Phase 2).

## Phase 2 — #1 DuckDB writes directly to Postgres  ✅ IMPLEMENTED + correctness-validated
- `run_id`/`entity_type` supplied via a 1-row `score_params` relation (not bloating
  `cand_pairs`); `comp_meta_lkp` materialized via `con.register` from
  `_extract_comp_meta` (NaN→None so json_object emits valid null, never NaN).
- `_attach_postgres`: derive `PGHOST/PGPORT/PGUSER/PGDATABASE`(+`PGPASSWORD` iff
  present) from `session.get_bind().url` into `os.environ`; `INSTALL/LOAD postgres`;
  static `ATTACH '' AS pg (TYPE postgres)`.
- One static `_PG_INSERT_SCORED_SQL` = validated CTE (cand_pairs ⋈ pred_out →
  `to_json` key-unnest → comp_meta join → CASE-based entry (keeps `bf:null` when
  matched; `bf_tf_adj` via merge_patch omit-null) → `json_group_object` → score
  `greatest(0,least(1,prob))`). `_write_scored_via_pg` runs it + the rare-miss
  Python fallback (`compare_two_records`).
- Non-postgres (sqlite tests) keeps the CURRENT streaming fetchmany path unchanged
  — the DuckDB-ATTACH path is postgres-only (`comp_meta and dialect=='postgresql'`).
- EVIDENCE: equiv harness (real person config + forced null-bf) → SQL JSON ==
  Python JSON, 0 mismatches; PG end-to-end on run_id=2 organization → 3,712
  written == 3,712 in DB, 0 misses, scores∈[0,1], full explanation incl bf_tf_adj,
  cross-connection visibility OK. 14 score + 479 resolve tests green; ruff clean.
- ⏳ PENDING: full run_id=2 perf re-run (target write ~31min → low single-digit min).

## Files in scope
- `app/resolve/stages/score.py` — primary (new postgres-only ATTACH write path;
  `comp_meta_lkp` builder; env/ATTACH helper; `run_score_stage` UNLOGGED swap).
- `TASK.md` — this file.
- No schema/model change (`ScoredPair` unchanged; UNLOGGED is runtime DDL).

## Behaviour to preserve (locked by tests/resolve/test_score.py)
- Exactly one `scored_pairs` row per candidate_pair (`pairs_compared` == count).
- `score` in `[0,1]`; `explanation_json` a non-empty dict with the SAME structure
  on both the postgres and sqlite paths.
- TF adjustment active: `line_1` explanation has `bf_tf_adj`; common address has
  lower `bf_tf_adj` than rare.
- Deterministic (same seed); idempotent re-run replaces the run's rows; empty →
  `{"pairs_compared": 0}`.

## Checks to run (evidence required before "done")
1. `uv run pytest tests/resolve/test_score.py -q` → 14 green (sqlite path untouched).
2. Full resolve suite green; `uv run ruff check app/resolve/stages/score.py` clean.
3. **Structural-equivalence**: on a bounded real slice, the ATTACH-path
   `explanation_json` parses to the same dict as the current Python path for the
   same pairs.
4. Full `run_id=2` spike re-run: `pairs_compared = 25,873,623` exact, bounded RSS,
   report new wall-clock vs the 37 min baseline (target: write ~31min → low
   single-digit min).

## Out of scope
- `#3` parallel Python COPY (made moot by #1).
- The per-entity-type full candidate_pairs scan (single-pass router) — separate.
- Downstream classify/cluster/survivorship.

## Done =
Checks 1–4 pass with recorded evidence, task-critic PASS, gate clean, and the
blocking-scale-tuning memory updated with the write-phase result.
