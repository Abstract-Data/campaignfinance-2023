# Task 4a — Resolved views / fact tables

> **Phase 4, round 1. Parallel-safe with 4b, 4c, 4d.** Blocks `task-4z`.
> Read the pack README, the Phase 4 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The
resolution layer holds canonical entities and a crosswalk, but analysts still
have to join through the crosswalk by hand. This task publishes resolved views
so contributions and expenditures can be queried directly by canonical entity.

Reference: the spec's "Rollout phases → Phase 4" (resolved views / fact table).

## Dependencies

- **Depends on:** Phase 3 merged (canonical + crosswalk are populated).
- **Blocks:** `task-4z`.
- **Parallel-safe with:** 4b, 4c, 4d.

## Files

- **Create:** `app/resolve/publish/views.py` — view definitions + a builder.
- **Create:** `tests/resolve/test_resolved_views.py`.

New files only. Do **not** create `app/resolve/publish/__init__.py` — `task-4z`
owns it.

## Interface contract

`views.py` exports `build_resolved_views(session) -> list[str]` — creates (or
replaces) these PostgreSQL views and returns their names:

- **`resolved_transactions`** — `unified_transactions` joined through
  `entity_crosswalk` → `canonical_entity` for each transaction party, plus the
  transaction's `report_id`/report. Each row exposes the canonical contributor /
  recipient / payee, not the raw source persons.
- **`resolved_contributions`** — `unified_contributions` with
  `contributor_entity_id` and `recipient_entity_id` resolved through the
  crosswalk to `canonical_entity`.
- **`resolved_expenditures`** — `unified_transactions` of type `EXPENDITURE`
  with the payee resolved to a `canonical_entity`.

Each view must resolve a source entity to its canonical entity via
`entity_crosswalk`; a source record with no crosswalk row (should not happen
after a clean run, but guard for it) still appears, with null canonical columns,
so nothing silently disappears.

Views are created idempotently (`CREATE OR REPLACE VIEW`). If a materialized
table is preferred for performance, expose a `materialized: bool` parameter —
default `False` (plain views) to keep them always-fresh.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_resolved_views.py`: a failing test
  that, on a seeded canonical + crosswalk + transaction fixture,
  `build_resolved_views()` creates the three views and `SELECT`ing
  `resolved_contributions` returns rows whose contributor resolves to the
  expected `canonical_entity`.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `build_resolved_views()`. Run; pass. Commit.
- [ ] **Step 4** — Add a test that a transaction whose party has no crosswalk
  row still appears in `resolved_transactions` with null canonical columns
  (no silent drop). Run; pass; commit.
- [ ] **Step 5** — Add a test that `build_resolved_views()` is idempotent —
  running it twice does not error and leaves the views intact. Commit.

## Acceptance criteria

- [ ] `resolved_transactions`, `resolved_contributions`, `resolved_expenditures`
  exist and resolve source parties to canonical entities through the crosswalk.
- [ ] Un-crosswalked rows are preserved with null canonical columns.
- [ ] View creation is idempotent.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-4/task-4a-resolved-views`. New files only, under
`app/resolve/publish/`. Do not create `publish/__init__.py`. These are
read-only views over existing tables — do not alter any source or canonical
table.
