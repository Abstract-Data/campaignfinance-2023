# Task 3c — Reversibility tooling: unmerge a run

> **Phase 3, round 1. Parallel-safe with 3a, 3b.** Blocks `task-3z`.
> Read the pack README, the Phase 3 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). A core
promise of the design is that every merge is reversible: the source layer is
immutable, so a resolution run can be undone by removing its resolution-layer
output and rebuilding the canonical layer from the previous run. This task
builds that tooling and the reversibility test deferred from Phase 1.

Reference: the spec's "Architecture overview" (re-runnable, reversible) and the
"Testing strategy → Reversibility" item.

## Dependencies

- **Depends on:** Phase 2 merged.
- **Blocks:** `task-3z`.
- **Parallel-safe with:** 3a, 3b.

## Files

- **Create:** `app/resolve/reverse.py` — the unmerge tooling.
- **Create:** `tests/resolve/test_reversibility.py`.

New files only. Do **not** create any `__init__.py`.

## Interface contract

`reverse.py` exports:

- `unmerge_run(session, run_id) -> RunReversal` — reverts a `match_run`:
  1. Deletes the run's `entity_crosswalk` / `address_crosswalk` /
     `campaign_crosswalk` rows, its `match_decision` rows, and the `merge_review`
     rows it surfaced (decided rows from prior runs are left untouched).
  2. Rebuilds the canonical layer from the most recent **non-reverted** prior
     run's crosswalk (re-publish from the retained prior crosswalk). If there is
     no prior run, the canonical tables are emptied.
  3. Marks the `match_run` `status="reverted"`.
  4. Returns a `RunReversal` summary (rows removed per table, canonical rows
     rebuilt).
- `can_unmerge(session, run_id) -> tuple[bool, str]` — guards: only the latest
  non-reverted run is safely reversible; reversing an older run requires
  reverting the runs after it first. Return `(False, reason)` otherwise.

Reversal must be transactional — either the whole unmerge commits or none of it.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_reversibility.py`: a failing test —
  run the pipeline on a seeded fixture, snapshot the `entity_crosswalk` and
  `canonical_entity` tables; run the pipeline a second time; `unmerge_run` the
  second run; assert both tables are byte-equal to the snapshot.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `unmerge_run` and `can_unmerge`. Run; pass. Commit.
- [ ] **Step 4** — Add tests: `can_unmerge` refuses a non-latest run; reversal
  is transactional (simulate a mid-reversal failure → nothing is deleted);
  decided `merge_review` rows from prior runs survive a reversal. Implement;
  run; pass; commit.

## Acceptance criteria

- [ ] `unmerge_run` removes a run's resolution-layer rows and rebuilds canonical
  from the prior run; the run is marked `reverted`.
- [ ] Merge → second run → unmerge restores the graph exactly (the reversibility
  test passes).
- [ ] `can_unmerge` guards against reverting a non-latest run.
- [ ] Reversal is transactional.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-3/task-3c-reversibility`. New files only. `reverse.py`
lives at `app/resolve/reverse.py` (not under `review/`). Do not create any
`__init__.py`.
