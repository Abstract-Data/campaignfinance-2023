# TASK — Scale the resolve score stage to millions of candidate pairs

## Goal
Rework `app/resolve/stages/score.py::run_score_stage` so it no longer OOMs at
25% TX scale (run_id=2: 3.2M persons, 25,873,623 candidate_pairs). The current
implementation materialises the entire run in Python:
- `list(session.exec(select(CandidatePair)...))` → 25.87M ORM objects (~10GB+)
- `linker.inference.predict(...).as_pandas_dataframe()` → ~25M-row pandas frame
  with retained intermediate columns (~40 cols)
- `_predictions_lookup(...)` → `.iterrows()` over 25M rows building a dict
  (catastrophically slow *and* a second copy in memory)

## Approach (validated against splink 4.0.16 / duckdb 1.5.3)
Keep Splink's `predict()` (DuckDB computes the 25M-pair comparison on-disk fine),
but **never** call `as_pandas_dataframe()` on it and **never** build an all-pairs
Python dict. Instead:
1. Partition by `entity_type` driven from `resolution_input` (few types; person
   dominates) — do NOT load all candidate_pairs into Python to group them.
2. Per entity_type: load that type's `resolution_input` rows, train the linker on
   an **on-disk** `DuckDBAPI` (spills intermediates to disk, not RAM).
3. `pred = linker.inference.predict(threshold_match_probability=0.0)` → a
   DuckDB-backed `SplinkDataFrame` (not pandas).
4. Stream this type's candidate_pairs from the DB (`yield_per`), normalise each to
   `(min,max)` uid order, bulk-insert into a DuckDB `cand_pairs` table in the
   linker's own connection.
5. `SELECT c.*, p.* FROM cand_pairs c LEFT JOIN <pred> p ON p.unique_id_l=c.uid_l
   AND p.unique_id_r=c.uid_r`, streamed via `fetch_record_batch`. Build one
   `ScoredPair` per candidate pair; write in batches. (Splink dedupe_only emits
   pairs with `unique_id_l < unique_id_r` lexically, matching the normalised key.)
6. Pairs the join misses (`match_probability IS NULL`) fall back to
   `compare_two_records` — exactly the existing fallback behaviour.

## Files in scope
- `app/resolve/stages/score.py` — the rework (primary).
- `tests/resolve/test_score.py` — must stay green; add a streaming/large-ish case
  only if needed to lock the new path (no behavioural change to existing asserts).

## Behaviour to preserve (locked by tests/resolve/test_score.py)
- Exactly **one** `scored_pairs` row per candidate_pair (`pairs_compared` == count).
- `score` in `[0,1]`; `explanation_json` a non-empty dict.
- Deterministic across repeated runs (same seed).
- TF adjustment active: `bf_tf_adj` present in the `line_1` explanation; common
  address has lower `bf_tf_adj` than rare address.
- Per-entity-type configs (person / organization / committee) used independently.
- Idempotent: re-run replaces prior `scored_pairs` for the run; runs isolated.
- Empty candidate_pairs → `{"pairs_compared": 0}`.

## Checks to run (evidence required before "done")
1. `uv run pytest tests/resolve/test_score.py -q` → all green (sqlite contract).
   ✅ DONE — 14 passed. Full resolve suite (67) also green.
2. `uv run ruff check app/resolve/stages/score.py` → clean. ✅ DONE.
3. Scaled validation on spike DB `campaignfinance_elt_spike` run_id=2: score stage
   completes **without OOM**, bounded RSS.
   ✅ DONE (bounded scale, real data) — scored the `organization` entity_type of
   run_id=2: 3,712 rows, exact DB-count match, **peak RSS ~1.2 GB (flat)**, predict
   0.08 s; exercised record streaming, the full 25.87M-pair routing scan, predict(),
   the streamed join, and Core bulk writes.
   ⚠️ DEFERRED — the full 25.87M person run was NOT executed: the dev volume had
   only ~25 GB free (98% full) and predict()'s ~70M wide intermediate could exhaust
   it, risking the system disk. To run it, free disk or set `RESOLVE_DUCKDB_TMP` to a
   volume with headroom. Code is proven correct + memory-bounded on real data.
4. Spot-check: a handful of `scored_pairs` rows have score in range and non-empty
   explanation incl. a gamma breakdown. ✅ DONE (0.9996 / 0.057 / 0.057 with
   normalized_org/line_1/city/zip5 breakdowns).

## Follow-up optimizations (after full-run revealed 15.4h, write+predict bound)
The full run_id=2 run completed correctly (25,873,623, memory/disk bounded) but
took 15.4h: ~13.6h writing scored_pairs (executemany ~526 rows/s) + ~85min
predict() on the uncapped ~70M re-blocking + ~15min EM. Two fixes implemented:

1. **Write via COPY + drop/rebuild indexes** (commits 1bf4f60 + this one).
   `_bulk_insert_scored` uses PostgreSQL COPY on the postgres dialect (executemany
   fallback on sqlite); `run_score_stage` drops the scored_pairs secondary indexes
   (entity_type, run_id) around the bulk-load loop and rebuilds them in a finally.
   Expected: ~13.6h write → ~minutes.
2. **Exact-edge-list scoring** (`_predict_exact_pairs`). Replaces predict()'s
   uncapped re-blocking by feeding our staged candidate pairs straight into
   Splink's comparison-vector → match-probability pipeline (internals, pinned
   4.0.16; try/except fallback to full predict()). Scores exactly 25.87M, not
   ~70M. Expected: ~85min predict → ~half.

Checks: full resolve suite green (479 passed); 14 score unit tests green (they
now exercise the exact-pair path); ruff clean.
RESULT — full run_id=2 re-run: **15.4h → 2.24h (6.9x)**, 25,873,623 exact, peak
RSS 5.8GB. Write 13.6h → ~31min (#1, the dominant win). #2 ran for real (0
fallbacks, exact 25.87M) and lowered memory/disk but did NOT cut the ~94min
predict wall-clock — predict isn't pair-count-bound at this scale (next lever =
trim retain_intermediate columns / TF). Both features implemented + correct.

## Out of scope (defer, note in handoff)
- `max_pairs_per_run` cap policy decision (separate; cap is moot for score cost).
- Running classify/cluster/survivorship end-to-end (next workstream once score scales).
- The Phase-1 ELT record-type expansion plan (misty-hugging-puzzle.md) — unrelated.
- EM training time (~15min, pre-existing in _train_linker; not addressed here).
- Single-pass pair routing (currently one candidate_pairs scan per entity_type).

## Done =
Checks 1–4 pass with recorded evidence (tests green, ruff clean, run_id=2 scored
without OOM at full pair count), task-critic PASS, and the blocking-scale-tuning
memory updated to mark the score stage fixed.
