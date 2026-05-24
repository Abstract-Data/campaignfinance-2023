# Task 1g — Stage 7: clustering + survivorship + publish

> **Phase 1, round 2. Parallel-safe with 1d, 1e, 1f.** Blocks `task-1z`.
> Round 2 begins after 1a/1b/1c are merged. Read the pack README and spec first.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The final
stage turns merge edges into canonical records: it clusters the edges, builds
one golden record per cluster, records name history, and writes the crosswalk.

In Phase 1 there is no probabilistic stage 6, so this task includes a **trivial
clustering** path: each connected group of deterministic merge edges is one
cluster. Phase 2 will replace trivial clustering with full connected-components
over probabilistic edges — so structure the clustering behind a small function
boundary that Phase 2 can swap.

Reference: the spec's "The resolution pipeline → 7. Survivorship / publish",
"Survivorship rules", and the Phase 1 sequencing note ("trivial clustering
path").

## Dependencies

- **Depends on:** 1a (canonical schema), 1b (resolution schema), 1c
  (`resolution_input`) merged.
- **Blocks:** `task-1z`.
- **Parallel-safe with:** 1d, 1e, 1f.

## Files

- **Create:** `app/resolve/stages/survivorship.py` — clustering + survivorship +
  publish.
- **Create:** `tests/resolve/test_survivorship.py`.

New files only. Do **not** create any `__init__.py` — `task-1z` owns those.

## Interface contract

`survivorship.py` exports:

- `cluster_edges(edges) -> list[Cluster]` — groups merge edges into clusters.
  Phase 1: connected-components over the deterministic edges from `task-1f`'s
  staging table (a record with no edges is its own singleton cluster). Keep this
  a standalone function so Phase 2 can extend it.
- `build_golden_record(cluster, resolution_input_rows) -> CanonicalEntity` —
  applies the survivorship rules from the spec: name = most complete (ties →
  most recent); address = most recent fully-parsed; `first_seen`/`last_seen` =
  min/max; `source_record_count` = cluster size.
- `run_survivorship_stage(session, run_id, config) -> dict` — the `Stage` entry
  point: clusters this run's edges, builds/refreshes `canonical_entity` (and
  `canonical_address`, `canonical_campaign`) rows in **staging tables**, writes
  `canonical_name_history` (every distinct name in the cluster, dated), writes
  the `entity_crosswalk` / `address_crosswalk` / `campaign_crosswalk` rows, and
  returns `{"canonical_out": <n>}`. The atomic swap of staging → live is done by
  `task-1d`'s `staging.py` helpers — call them; do not reinvent.

Every source record must end up in exactly one cluster and therefore one
crosswalk row (singletons included — no record is left unlinked).

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_survivorship.py`: failing tests
  that (a) `cluster_edges` groups A–B and B–C into one cluster of {A,B,C};
  (b) a record with no edges is its own singleton cluster; (c)
  `build_golden_record` picks the most-complete name and most-recent parsed
  address; (d) every source record gets exactly one crosswalk row.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `cluster_edges` and `build_golden_record`. Run;
  pass. Commit.
- [ ] **Step 4** — Implement `run_survivorship_stage()` — write canonical +
  name-history + crosswalk rows to staging. Run; pass. Commit.
- [ ] **Step 5** — Add a test that `canonical_name_history` captures **every**
  distinct name in a multi-name cluster, each with first/last-seen dates. Run;
  pass; commit.

## Acceptance criteria

- [ ] Transitive edges (A–B, B–C) collapse into one cluster; singletons survive.
- [ ] Golden records follow the spec's survivorship rules.
- [ ] Every source record has exactly one crosswalk row.
- [ ] `canonical_name_history` records every distinct cluster name, dated.
- [ ] `run_survivorship_stage()` conforms to the `Stage` protocol; clustering is
  behind a swappable function for Phase 2.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1g-survivorship`. New files only. Consume
`task-1f`'s edges via its staging table; use `task-1d`'s `staging.py` swap
helpers. Do not create any `__init__.py`.
