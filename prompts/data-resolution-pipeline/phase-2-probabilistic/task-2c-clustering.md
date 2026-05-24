# Task 2c — Stage 6: connected-components clustering + mega-cluster guard

> **Phase 2, round 1. Parallel-safe with 2a, 2b, 2e.** Blocks `task-2d`, `2z`.
> Read the pack README, the Phase 2 README (staging contracts), and the spec.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Stage 6 turns
merge edges into clusters: each connected component of the edge graph becomes
one canonical entity. This is what makes "A↔B and B↔C" collapse A, B, C into one
record even if A↔C was never compared. It replaces Phase 1's trivial clustering.

Reference: the spec's "The resolution pipeline → 6. Cluster" and the
mega-cluster guard under "Error handling and resilience".

## Dependencies

- **Depends on:** Phase 1 merged; builds against the `merge_edges` /
  `clusters` contracts (Phase 2 README).
- **Blocks:** `task-2d`, `task-2z`.
- **Parallel-safe with:** 2a, 2b, 2e.

## Files

- **Create:** `app/resolve/stages/cluster.py` — the stage-6 runner.
- **Create:** `tests/resolve/test_cluster.py`.

New files only. Do **not** edit `survivorship.py` (that is `task-2d`). Do **not**
create any `__init__.py` — `task-2z` owns those.

## Interface contract

`cluster.py` exports `run_cluster_stage(session, run_id, config) -> dict`
(`Stage` protocol from Phase 1 `task-1d`). It:

1. Reads the `merge_edges` staging table for `run_id` — all edges, regardless of
   `edge_source` (`deterministic`, `probabilistic`, `approved_review`).
2. Runs **connected-components** over the edge graph. Every source record is a
   node; a record with no edges is its own singleton component.
3. **Mega-cluster guard:** any component whose size exceeds
   `config["max_cluster_size"]` is **not** auto-published — its rows are written
   to `clusters` with `held_for_review=True`, the constituent pairs are routed
   to `merge_review`, and the event is logged. A common name or a bad blocking
   key must not silently collapse thousands of records into one entity.
4. Writes the `clusters` staging table (`run_id`, `cluster_id`, `source_type`,
   `source_id`, `entity_type`, `held_for_review`) per the Phase 2 README
   contract.
5. Returns `{"clusters": <n>, "held_for_review": <n>}`.

Connected-components must be deterministic — sort nodes/edges before traversal
so `cluster_id` assignment is stable across runs.

This task supersedes the trivial `cluster_edges` function that Phase 1
`task-1g` left behind a function boundary; `task-2d` rewires survivorship to
call `run_cluster_stage`'s output. Do not edit `survivorship.py` here.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_cluster.py`: failing tests that
  (a) edges A–B and B–C yield one component {A,B,C}; (b) a record with no edges
  is its own singleton; (c) `cluster_id` assignment is stable across two runs.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement connected-components + `run_cluster_stage()`. Run;
  pass. Commit.
- [ ] **Step 4** — Add a mega-cluster test: with `max_cluster_size=3`, a
  component of 5 is written `held_for_review=True`, its pairs land in
  `merge_review`, and a warning is logged. Implement; run; pass; commit.

## Acceptance criteria

- [ ] Transitive edges collapse into one component; singletons survive.
- [ ] `cluster_id` assignment is deterministic across runs.
- [ ] The mega-cluster guard holds oversized components and routes them to
  review instead of auto-publishing.
- [ ] `clusters` is written per contract; the stage conforms to the `Stage`
  protocol.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-2/task-2c-clustering`. New files only. Communicate with
`task-2d` only via the `clusters` staging table. Do not create any `__init__.py`.
