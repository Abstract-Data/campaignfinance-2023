# Task 1b — Resolution-layer schema (crosswalk + audit + review)

> **Phase 1, round 1. Parallel-safe with 1a, 1c.** Blocks round 2 and `task-1z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The
resolution layer needs the tables that connect source records to canonical
records and audit every run. This task defines those SQLModels — schema only.

Authoritative definitions: the spec's "Schema design → Resolution layer".

## Dependencies

- **Depends on:** Phase 0 merged.
- **Blocks:** `task-1d`, `task-1f`, `task-1g`, `task-1z`.
- **Parallel-safe with:** 1a, 1c.

## Files

- **Create:** `app/resolve/models/resolution.py` — crosswalk + audit + review
  SQLModels.
- **Create:** `tests/resolve/test_resolution_schema.py`.

New files only. Do **not** create any `__init__.py` — `task-1z` owns those.

## Interface contract

SQLModels in `resolution.py` (see the spec for full columns; key columns below):

`EntityCrosswalk` (`entity_crosswalk`): `id` PK, `source_type` (enum
`unified_person`/`unified_committee`/`unified_entity`), `source_id` (**str** —
accommodates the string `filer_id` PK of committees), `canonical_entity_id`
(FK → `canonical_entity.id`), `match_method` (enum
`exact`/`deterministic_rule`/`probabilistic`/`manual`), `match_score` (float,
nullable), `confidence_band` (enum `auto`/`review`/`manual`), `run_id`
(FK → `match_run.id`), `decided_at`, `decided_by`.

`AddressCrosswalk` (`address_crosswalk`) and `CampaignCrosswalk`
(`campaign_crosswalk`): same shape, pointing at `canonical_address` /
`canonical_campaign` respectively.

`MatchRun` (`match_run`): `id` PK, `state_code`, `pass_type` (enum
`entity`/`address`/`campaign`), `engine_version`, `config_json` (Text),
`started_at`, `finished_at`, `status` (enum `running`/`completed`/`failed`),
and integer counters `records_in`, `pairs_compared`, `auto_merges`, `queued`,
`rejected`, `canonical_out`.

`MatchDecision` (`match_decision`): `id` PK, `run_id` FK, `source_a_type`,
`source_a_id`, `source_b_type`, `source_b_id`, `score` (float), `method`,
`band` (`auto`/`review`/`reject`), `outcome` (`merged`/`queued`/`rejected`),
`explanation_json` (Text).

`MergeReview` (`merge_review`): `id` PK, `run_id` FK, `source_a_type`,
`source_a_id`, `source_b_type`, `source_b_id`, `score`, `explanation_json`,
`status` (enum `pending`/`approved`/`rejected`), `reviewer`, `decided_at`,
`notes`.

Note: `match_score` on a crosswalk row is nullable because `exact` and
`deterministic_rule` methods carry no probabilistic score.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_resolution_schema.py`: a failing
  test that all six tables register and create via `create_all`.
- [ ] **Step 2** — Run it; expect failure.
- [ ] **Step 3** — Implement the six models in `resolution.py`.
- [ ] **Step 4** — Run; expect pass. Commit.
- [ ] **Step 5** — Add assertions: `source_id` on the crosswalks is a string
  column; `match_score` is nullable; the enum columns reject invalid values.
  Run; pass; commit.

## Acceptance criteria

- [ ] All six models match the spec; all tables create cleanly.
- [ ] Crosswalk `source_id` is a string; `match_score` is nullable.
- [ ] Models importable from `app.resolve.models.resolution`.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1b-resolution-schema`. New files only. The
`resolution_input` staging table is **not** yours — `task-1c` defines it. Do not
create any `__init__.py`.
