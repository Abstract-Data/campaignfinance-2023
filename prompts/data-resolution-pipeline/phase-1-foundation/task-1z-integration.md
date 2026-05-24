# Task 1z — Phase 1 integration: wire the pipeline, run it end-to-end

> **Phase 1, serial — runs LAST.** Depends on 1a–1g all merged.
> Read the pack README and the source spec before starting.

## Context

Round-1 and round-2 tasks each delivered a piece — schema, standardizers, CLI
shell, and three stages — built and tested in isolation. This task wires them
into one running pipeline and proves it works end-to-end on Texas.

## Dependencies

- **Depends on:** 1a, 1b, 1c, 1d, 1e, 1f, 1g (all merged to the Phase 1 branch).
- **Blocks:** Phase 2.

## Files

- **Create:** `app/resolve/__init__.py`, `app/resolve/models/__init__.py`,
  `app/resolve/stages/__init__.py`, `app/resolve/standardize/__init__.py`.
- **Modify:** `app/resolve/cli.py` — inject the concrete Phase 1 stage list.
- **Create:** `tests/resolve/test_phase1_integration.py`.

## What to build

1. **Package `__init__.py` files** — make `app/resolve` and its subpackages
   importable; have `models/__init__.py` import the canonical and resolution
   models so `SQLModel.metadata` registers every table.

2. **Stage wiring** — in `cli.py`, assemble the Phase 1 stage list in order and
   pass it to `ResolutionRun.run()`:

   ```
   stage 1  build_resolution_input        (task 1c)
   stage 2  run_blocking_stage            (task 1e)
   stage 3  run_fastpath_stage            (task 1f)
   stage 7  run_survivorship_stage        (task 1g)
   ```

   (Stages 4–6 are Phase 2; the Phase 1 pipeline goes 1 → 2 → 3 → 7. Stage 7's
   clustering is the trivial path from `task-1g`.)

3. **End-to-end run** — `python -m app.resolve run --state texas` must open a
   `match_run`, execute all four stages, write canonical + crosswalk rows, and
   finish the run `completed` with populated counts.

## Steps (TDD)

- [ ] **Step 1** — Create the `__init__.py` files. Confirm `import app.resolve`
  and `SQLModel.metadata` containing every resolution table. Commit.
- [ ] **Step 2** — Write `tests/resolve/test_phase1_integration.py`: a failing
  end-to-end test on a small seeded fixture — run the pipeline, assert a
  `completed` `match_run`, `canonical_entity` rows, and one `entity_crosswalk`
  row per source record.
- [ ] **Step 3** — Wire the stage list into `cli.py`. Run the integration test;
  expect pass. Commit.
- [ ] **Step 4** — Write an **idempotency** test: run the pipeline twice on the
  same fixture; assert the second run's `entity_crosswalk` is identical to the
  first's (same source→canonical mapping). Fix any non-determinism. Commit.
- [ ] **Step 5** — Run the pipeline on the **full Texas** source data into a
  scratch database. Confirm: canonical rows created, every source record
  crosswalked, the `match_run` `completed` with sane counts, and a measurable
  drop from source-record count to canonical-record count (the deterministic
  duplicate reduction). Record the numbers in the commit message. Commit.

## Acceptance criteria

- [ ] `uv run pytest tests/resolve/` is fully green.
- [ ] `python -m app.resolve run --state texas` runs stages 1→2→3→7 end-to-end
  and finishes a `completed` `match_run`.
- [ ] Every source record has exactly one crosswalk row.
- [ ] The pipeline is idempotent — two runs yield an identical crosswalk.
- [ ] Source→canonical counts show real deterministic duplicate reduction.

## Collision protocol

Branch `resolve/phase-1/task-1z-integration`, cut after 1a–1g are merged. This
task is *expected* to edit shared files (`__init__.py`s, `cli.py`'s stage list).
If a round-1/round-2 interface gap surfaces, fix it here and note the deviation
rather than reopening a parallel task.
