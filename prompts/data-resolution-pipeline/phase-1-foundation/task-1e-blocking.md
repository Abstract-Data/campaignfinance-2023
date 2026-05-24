# Task 1e — Stage 2: blocking

> **Phase 1, round 2. Parallel-safe with 1d, 1f, 1g.** Blocks `task-1z`.
> Round 2 begins after 1a/1b/1c are merged. Read the pack README and spec first.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres, Polars).
Comparing every pair of records is quadratic and infeasible. Blocking groups
records into small "blocks" so only plausible candidate pairs are ever compared.
This task builds the blocking engine — stage 2.

Reference: the spec's "The resolution pipeline → 2. Block" and the
"Address-as-shared-hub model" note that blocking must never use address alone
and must cap high-frequency values.

## Dependencies

- **Depends on:** 1c (`resolution_input` table + standardized features) merged.
- **Blocks:** `task-1z`.
- **Parallel-safe with:** 1d, 1f, 1g.

## Files

- **Create:** `app/resolve/blocking.py` — the blocking engine.
- **Create:** `tests/resolve/test_blocking.py`.

New files only. Do **not** create any `__init__.py` — `task-1z` owns those.

## Interface contract

`blocking.py` exports:

- `BlockingRule` — a named rule producing a blocking key from a
  `ResolutionInput` row (e.g. `phonetic_last_name + zip3`, `org_prefix`,
  `committee filer_id space`). Rules are data-driven and read from `config`.
- `generate_candidate_pairs(session, run_id, rules: list[BlockingRule], *,
  max_block_size: int) -> Iterable[CandidatePair]` — for each rule, groups the
  `resolution_input` rows for `run_id` by blocking key; within each block emits
  the pairwise combinations as `CandidatePair(source_a, source_b, rule_name)`.
  Pairs are de-duplicated across rules.
- **High-frequency cap:** any block larger than `max_block_size` is *skipped*
  with a logged warning (it would explode the pair count and signals a bad
  key — e.g. a registered-agent address). Never block on address alone.

Stage entry point matching the `Stage` protocol from `task-1d`:
`run_blocking_stage(session, run_id, config) -> dict` returning
`{"pairs_compared": <n>}` and persisting candidate pairs to a per-run staging
table for downstream stages.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_blocking.py`: failing tests that
  (a) records sharing a blocking key produce a candidate pair; (b) records in
  different blocks do **not**; (c) a block exceeding `max_block_size` is skipped
  and logged; (d) the same pair found by two rules is emitted once.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `BlockingRule` and `generate_candidate_pairs()`.
  Run; pass. Commit.
- [ ] **Step 4** — Implement `run_blocking_stage()` (the `Stage`-protocol entry
  point) and a test that it persists pairs tagged with `run_id` and returns the
  count. Run; pass; commit.
- [ ] **Step 5** — Add a test asserting no rule keys on address alone. Run;
  pass; commit.

## Acceptance criteria

- [ ] Candidate pairs are generated within blocks only; cross-block pairs never
  appear.
- [ ] Oversized blocks are skipped with a warning; no address-only rule exists.
- [ ] Duplicate pairs across rules are emitted once.
- [ ] `run_blocking_stage()` conforms to the `Stage` protocol from `task-1d`.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1e-blocking`. New files only. Do not create any
`__init__.py`. Do not import `task-1f`/`1g` modules — stages communicate only
through staging tables.
