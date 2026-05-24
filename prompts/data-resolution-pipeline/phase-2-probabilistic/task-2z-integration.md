# Task 2z — Phase 2 integration: wire stages 4–6, tune thresholds, run

> **Phase 2, serial — runs LAST.** Depends on 2a, 2b, 2c, 2d, 2e all merged.
> Read the pack README, the Phase 2 README, and the spec.

## Context

Round-1 and round-2 tasks delivered the probabilistic stages, the new
clustering, the survivorship update, and the golden-set harness — each in
isolation. This task wires stages 4→5→6 into the pipeline, sets the starting
thresholds, runs a full Texas pass, and flips the golden-set precision gate live.

## Dependencies

- **Depends on:** 2a, 2b, 2c, 2d, 2e (all merged to the Phase 2 branch).
- **Blocks:** Phase 3.

## Files

- **Modify:** `app/resolve/cli.py` — extend the stage list to 1→2→3→4→5→6→7.
- **Modify:** `app/resolve/stages/__init__.py` — export the new stage runners.
- **Create:** `app/resolve/config/default.toml` (or extend the existing config
  source) — starting thresholds and `max_cluster_size`.
- **Create:** `tests/resolve/test_phase2_integration.py`.

No schema migration is needed for `task-2d`'s `provenance_json` column — the
canonical tables are rebuilt from staging and atomically swapped each run, so
the column appears on the next run automatically.

## What to build

1. **Stage wiring** — assemble the full Phase 2 stage list and pass it to
   `ResolutionRun.run()`:

   ```
   1 build_resolution_input   (task 1c)
   2 run_blocking_stage       (task 1e)
   3 run_fastpath_stage       (task 1f)
   4 run_score_stage          (task 2a)
   5 run_classify_stage       (task 2b)
   6 run_cluster_stage        (task 2c)
   7 run_survivorship_stage   (task 1g + 2d update)
   ```

2. **Config** — set starting values: `auto_threshold = 0.99`,
   `review_threshold = 0.80`, `max_cluster_size` (choose from the cluster-size
   distribution observed in a dry run — start conservative, e.g. 200). All go
   into `match_run.config_json`.

3. **Full Texas pass** — run end-to-end into a scratch database; confirm
   `scored_pairs`, `match_decision`, `merge_review`, and `clusters` populate,
   the `match_run` finishes `completed`, and no auto-published cluster exceeds
   `max_cluster_size`.

4. **Flip the golden-set gate** — remove the round-1 skip-guard in
   `test_match_quality.py` (`task-2e`) so it runs against the real stages, and
   confirm precision clears `PRECISION_FLOOR`.

## Steps (TDD)

- [ ] **Step 1** — Export the new stage runners from `stages/__init__.py`;
  extend the `cli.py` stage list. Commit.
- [ ] **Step 2** — Write `tests/resolve/test_phase2_integration.py`: a failing
  end-to-end test on a seeded fixture — run all 7 stages, assert a `completed`
  `match_run`, `scored_pairs` rows, `merge_review` rows for the medium band, and
  `clusters` rows.
- [ ] **Step 3** — Add the config file with starting thresholds. Run the
  integration test; expect pass. Commit.
- [ ] **Step 4** — Remove the `task-2e` skip-guard; run `test_match_quality.py`
  against the real stages; confirm precision ≥ floor. Tune comparison configs /
  thresholds if it does not. Commit.
- [ ] **Step 5** — Run the full Texas pass into a scratch DB. Record in the
  commit message: pairs scored, auto-merges, queued for review, clusters,
  source→canonical reduction, and the largest cluster size. Commit.
- [ ] **Step 6** — Idempotency check: run twice; assert an identical crosswalk.
  Commit.

## Acceptance criteria

- [ ] `uv run pytest tests/resolve/` is fully green, including the golden-set
  precision gate against the real stages.
- [ ] `python -m app.resolve run --state texas` runs stages 1→7 end-to-end.
- [ ] `merge_review` is populated for the medium band; no auto-published cluster
  exceeds `max_cluster_size`.
- [ ] The run is idempotent (two runs → identical crosswalk).
- [ ] Thresholds and `max_cluster_size` are in `config_json`.

## Collision protocol

Branch `resolve/phase-2/task-2z-integration`, cut after 2a–2e are merged. This
task is expected to edit shared files (`cli.py`, `stages/__init__.py`, config).
If a round-1/round-2 interface gap surfaces, fix it here and note the deviation.
