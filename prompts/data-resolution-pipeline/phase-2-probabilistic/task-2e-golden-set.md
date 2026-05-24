# Task 2e — Golden-set fixtures + precision/recall regression harness

> **Phase 2, round 1. Parallel-safe with 2a, 2b, 2c.** Blocks `task-2z`.
> Read the pack README, the Phase 2 README, and the spec's Testing strategy.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, pytest). Probabilistic
matching can quietly degrade when a model or threshold changes. The safety net
is a hand-labeled golden set of known same/different pairs and a regression test
that fails CI if precision drops below a floor.

Reference: the spec's "Testing strategy → Golden set".

## Dependencies

- **Depends on:** Phase 1 merged.
- **Blocks:** `task-2z` (which runs the gate on a full pass).
- **Parallel-safe with:** 2a, 2b, 2c.

## Files

- **Create:** `tests/resolve/golden/person_pairs.csv`,
  `organization_pairs.csv`, `committee_pairs.csv` — labeled fixtures.
- **Create:** `tests/resolve/golden/README.md` — how to extend the set.
- **Create:** `tests/resolve/test_match_quality.py` — the regression harness.

New files only. Do **not** create any `__init__.py`.

## Interface contract

Each golden CSV holds labeled pairs: two records' standardized features plus a
`label` column (`match` / `no_match`). Hand-label a starter set drawn from real
`tmp/texas` data — include the hard cases: name variants, typos, a shared
address with different people, an organization with legal-suffix variants. Aim
for at least ~50 pairs per entity type; the set is meant to grow (the
`golden/README.md` explains how to add pairs).

`test_match_quality.py` exports a pytest test that:

1. Loads each golden CSV.
2. Runs the matching path (blocking → score → classify, calling the Phase 2
   stage functions) over the labeled pairs.
3. Computes precision and recall against the labels.
4. **Asserts precision ≥ `PRECISION_FLOOR`** (a module constant, start at 0.95)
   — the test fails if precision drops below it. Recall is reported, not gated
   (recall is expected to climb over phases).

The harness must run without a live Postgres — use an in-memory SQLite engine
and seeded fixtures so it runs in CI.

## Steps (TDD)

- [ ] **Step 1** — Create the three golden CSVs with hand-labeled starter pairs
  (include the hard cases above) and `golden/README.md`. Commit.
- [ ] **Step 2** — Write `test_match_quality.py`: initially assert against a
  trivial exact-match baseline so the harness itself is exercised before the
  Splink stages exist. Run; confirm it passes on the baseline. Commit.
- [ ] **Step 3** — Add a `precision`/`recall` computation helper with its own
  unit test (feed it known TP/FP/FN counts, assert the numbers). Run; pass;
  commit.
- [ ] **Step 4** — Wire the harness to call the real stage functions behind a
  guard that skips with a clear message if `task-2a`/`2b` are not yet merged, so
  the file is committable in round 1 and `task-2z` flips the gate live.

## Acceptance criteria

- [ ] Three labeled golden CSVs exist with the hard cases represented.
- [ ] `test_match_quality.py` computes precision/recall and asserts a precision
  floor.
- [ ] The harness runs in CI without a live database.
- [ ] `golden/README.md` documents how to grow the set.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-2/task-2e-golden-set`. New files only — all under
`tests/resolve/`. Do not import or modify the stage modules; call them only
through their public `run_*_stage` entry points (guarded as in Step 4). Do not
create any `__init__.py`.
