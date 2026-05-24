# Task 1a — Canonical-layer schema

> **Phase 1, round 1. Parallel-safe with 1b, 1c.** Blocks round 2 and `task-1z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The
resolution layer needs canonical tables — one row per real-world thing — that
the pipeline populates. This task defines those SQLModels. It defines schema
only; no pipeline logic.

Authoritative definitions: the spec's "Schema design → Canonical layer" and
"Address-as-shared-hub model" sections. Follow them exactly.

## Dependencies

- **Depends on:** Phase 0 merged.
- **Blocks:** `task-1f`, `task-1g`, `task-1z` (they read/write these tables).
- **Parallel-safe with:** 1b, 1c.

## Files

- **Create:** `app/resolve/models/canonical.py` — all four canonical SQLModels.
- **Create:** `tests/resolve/test_canonical_schema.py`.

New files only. Do **not** create `app/resolve/__init__.py` or
`app/resolve/models/__init__.py` — `task-1z` creates those.

## Interface contract

Four SQLModels in `canonical.py` (see the spec for the full column list; key
columns below):

`CanonicalEntity` (`canonical_entity`): `id` PK, `uuid`, `entity_type`
(enum `person`/`organization`/`committee`), `canonical_name`, `normalized_name`,
`canonical_address_id` (FK → `canonical_address.id`, **nullable, many-to-one** —
many entities may share one address), `state_code`, `master_entity_id`
(self-FK → `canonical_entity.id`, nullable, reserved for future cross-state
linking — unused now), `first_seen_date`, `last_seen_date`,
`source_record_count`, `last_run_id`, `created_at`, `updated_at`.

`CanonicalCampaign` (`canonical_campaign`): `id` PK, `uuid`,
`committee_entity_id` (FK → `canonical_entity.id`), `office_normalized`,
`district`, `election_cycle` (int), `candidate_entity_id` (FK →
`canonical_entity.id`, nullable), `canonical_name`, `state_code`, `last_run_id`,
timestamps. Identity tuple: `(committee_entity_id, office_normalized,
election_cycle)`.

`CanonicalAddress` (`canonical_address`): `id` PK, `uuid`,
`standardized_line_1`, `standardized_line_2` (unit/suite), `city`, `state`,
`zip5`, `zip4`, `parse_status` (enum `parsed`/`partial`/`unparsed`), `frequency`
(int — a **derived** count for display/query only; nothing in matching reads
it), `last_run_id`, timestamps.

`CanonicalNameHistory` (`canonical_name_history`): `id` PK, `subject_type`
(enum `entity`/`campaign`), `subject_id` (int), `name`, `normalized_name`,
`first_seen_date`, `last_seen_date`, `occurrence_count`, `source`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_canonical_schema.py`: a failing
  test that all four tables register in `SQLModel.metadata` and create via
  `create_all` against an in-memory SQLite engine.
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_canonical_schema.py -v`;
  expect failure.
- [ ] **Step 3** — Implement the four models in `canonical.py`.
- [ ] **Step 4** — Run; expect pass. Commit.
- [ ] **Step 5** — Add assertions: `canonical_entity.canonical_address_id` is
  nullable; `master_entity_id` is a nullable self-FK; the
  `(committee_entity_id, office_normalized, election_cycle)` combination has a
  uniqueness constraint on `canonical_campaign`. Run; pass; commit.

## Acceptance criteria

- [ ] All four models match the spec; all tables create cleanly.
- [ ] `canonical_address_id` is nullable and many-to-one (no uniqueness on it).
- [ ] `canonical_campaign` enforces the identity-tuple uniqueness.
- [ ] Models importable from `app.resolve.models.canonical`.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1a-canonical-schema`. New files only. Do not create
any `__init__.py` — `task-1z` owns those.
