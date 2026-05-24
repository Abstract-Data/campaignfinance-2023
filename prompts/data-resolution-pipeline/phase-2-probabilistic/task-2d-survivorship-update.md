# Task 2d — Stage 7 update: survivorship for probabilistic clusters

> **Phase 2, round 2 — runs after 2a/2b/2c/2e merge. Runs alone.** Blocks `2z`.
> Read the pack README, the Phase 2 README (staging contracts), and the spec.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Phase 1's
survivorship stage (`task-1g`) clusters with a trivial path and builds golden
records from deterministic groups. This task rewires it to consume the real
connected-components clusters from stage 6 (`task-2c`) and adds field-level
provenance to the golden record.

Reference: the spec's "Survivorship rules" and "The resolution pipeline → 7".

## Dependencies

- **Depends on:** 2a, 2b, 2c, 2e merged (round 1). Needs `task-2c`'s `clusters`
  staging-table contract.
- **Blocks:** `task-2z`.
- **Runs alone** in round 2 — no parallel peer.

## Files

- **Modify:** `app/resolve/stages/survivorship.py` (from Phase 1 `task-1g`).
- **Modify:** `tests/resolve/test_survivorship.py` — extend, do not delete the
  Phase 1 tests.

This is the only Phase 2 task that edits `survivorship.py`; that is why it runs
alone in round 2.

## What to change

1. **Consume real clusters.** Replace the call to Phase 1's trivial
   `cluster_edges` with a read of the `clusters` staging table produced by
   `task-2c`'s `run_cluster_stage`. Rows with `held_for_review=True` are
   **skipped** for canonical publishing (they are in the review queue) — their
   members remain crosswalked to their pre-existing canonical entity, or to a
   singleton if new.
2. **Field-level provenance.** When `build_golden_record` selects a value for a
   canonical field (name, address, dates), also record which source record that
   value came from — store provenance in a `provenance_json` column on the
   canonical row. Add the column to the `CanonicalEntity` model in
   `app/resolve/models/canonical.py`. **No migration tool is needed:** the
   canonical tables are rebuilt in staging tables and atomically swapped every
   run (Phase 1 `task-1d`'s `staging.py`), so the new column simply appears on
   the next run's freshly-created table.
3. Keep the Phase 1 survivorship rules (most-complete name, most-recent parsed
   address, min/max dates) and the `canonical_name_history` population intact.

## Steps (TDD)

- [ ] **Step 1** — Add a failing test: with a `clusters` staging fixture (some
  rows `held_for_review=True`), `run_survivorship_stage` publishes canonical
  rows only for non-held clusters and skips the held ones.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Rewire `run_survivorship_stage` to read the `clusters` table;
  drop the trivial-clustering call. Run; pass. Commit.
- [ ] **Step 4** — Add a failing test that a published canonical row carries
  `provenance_json` naming the source record each field came from.
- [ ] **Step 5** — Implement provenance capture in `build_golden_record`. Run;
  pass. Commit.
- [ ] **Step 6** — Run the existing Phase 1 survivorship tests; confirm they
  still pass (no regression). Commit.

## Acceptance criteria

- [ ] Survivorship consumes the `clusters` staging table; the trivial-clustering
  path is gone.
- [ ] `held_for_review` clusters are not auto-published.
- [ ] Canonical rows carry field-level `provenance_json`.
- [ ] All Phase 1 survivorship tests still pass.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-2/task-2d-survivorship-update`, cut after round 1 merges.
You edit `survivorship.py` — no other Phase 2 task does. Do not edit
`cluster.py` (that is `task-2c`'s).
