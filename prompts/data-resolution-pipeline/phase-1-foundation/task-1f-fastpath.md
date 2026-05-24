# Task 1f — Stage 3: deterministic fast-path

> **Phase 1, round 2. Parallel-safe with 1d, 1e, 1g.** Blocks `task-1z`.
> Round 2 begins after 1a/1b/1c are merged. Read the pack README and spec first.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Before any
probabilistic scoring, the certain matches should be resolved cheaply and
deterministically. This task builds stage 3 — the deterministic fast-path. In
Phase 1 (no Splink yet) this stage does the *entire* matching job.

Reference: the spec's "The resolution pipeline → 3. Deterministic fast-path".

## Dependencies

- **Depends on:** 1a (canonical schema), 1b (resolution schema), 1c
  (`resolution_input`) merged.
- **Blocks:** `task-1z`.
- **Parallel-safe with:** 1d, 1e, 1g.

## Files

- **Create:** `app/resolve/stages/fastpath.py` — the deterministic fast-path.
- **Create:** `tests/resolve/test_fastpath.py`.

New files only. Do **not** create any `__init__.py` — `task-1z` owns those.

## Interface contract

`fastpath.py` exports `run_fastpath_stage(session, run_id, config) -> dict`
(conforming to the `Stage` protocol from `task-1d`). It:

1. Reads `resolution_input` rows for `run_id`.
2. Resolves the **certain** matches by exact deterministic rules:
   - identical committee `filer_id` → same committee entity;
   - identical standardized name + standardized address → same entity;
   - identical standardized address (alone) → same `canonical_address`.
3. For each deterministic match, emits a `match_decision` row with
   `method="exact"` or `"deterministic_rule"`, `score=NULL`, `band="auto"`,
   `outcome="merged"`, and an `explanation_json` naming the rule that fired.
4. Writes the resulting merge edges to a per-run staging table that `task-1g`
   (clustering/survivorship) consumes.
5. Returns `{"auto_merges": <n>}`.

This stage **only emits merge edges and decisions** — it does not build
canonical rows or write the crosswalk. That is `task-1g`'s job. Keeping the
boundary here means stage 3 and stage 7 stay independently testable.

Determinism: the same `resolution_input` must always yield the same decisions.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_fastpath.py`: failing tests that
  (a) two committees with the same `filer_id` produce one merge edge with
  `method="exact"`; (b) two persons with identical standardized name + address
  merge; (c) two persons with the same name but different addresses do **not**
  merge on the fast-path; (d) every emitted `match_decision` has a non-empty
  `explanation_json`.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `run_fastpath_stage()`. Run; pass. Commit.
- [ ] **Step 4** — Add a determinism test: running the stage twice on the same
  `resolution_input` yields identical decisions and edges. Run; pass; commit.

## Acceptance criteria

- [ ] Exact `filer_id` and exact name+address matches merge; near-matches do not.
- [ ] Every match writes a `match_decision` with a rule-naming
  `explanation_json`.
- [ ] Merge edges are written to a staging table for `task-1g`; no canonical row
  or crosswalk row is written by this task.
- [ ] The stage is deterministic and conforms to the `Stage` protocol.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1f-fastpath`. New files only. Communicate with
stage 7 only through the staging table — do not import `task-1g`'s module. Do
not create any `__init__.py`.
