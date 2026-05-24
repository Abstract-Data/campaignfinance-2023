# Task 0d — `SpacLink` model (SPAC linkage)

> **Phase 0, round 1. Parallel-safe with 0a–0c, 0e, 0f.** Blocks `task-0z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The TEC
`SPAC` record (`spacs.parquet` in `tmp/texas/`) links a **specific-purpose
committee** to the candidate or measure it supports or opposes. Nothing models
it today. This is relationship data the resolution pipeline (Phase 2+) will use,
so it must land in the source layer now.

Authoritative field layout: `tmp/texas/CFS-ReadMe.txt`, record `SpacData`
(`recordType = SPAC`).

## Dependencies

- **Depends on:** none — Phase 0 round 1.
- **Blocks:** `task-0z`.
- **Parallel-safe with:** 0a, 0b, 0c, 0e, 0f.

## Files

- **Create:** `app/core/source_models/spac.py` — the `SpacLink` SQLModel.
- **Create:** `app/core/source_models/spac_ingest.py` — `build_spac_link()`.
- **Create:** `tests/resolve/test_spac.py`.

New files only — no existing file is edited.

## Interface contract

`SpacLink` (table `spac_links`):

- `id: int` PK, `uuid: str`
- `spac_filer_id: str` FK → `unified_committees.filer_id` — the specific-purpose
  committee
- `supported_filer_id: str | None` FK → `unified_committees.filer_id` — the
  supported committee, when the SPAC record identifies one
- `supported_name: str | None` — name of the candidate/measure as filed
- `support_type: str | None` — `candidate` or `measure` (derive from the record)
- `position: str | None` — `support` or `oppose` (derive from the record)
- `state_id: int | None` FK → `states.id`
- `raw_data: str | None`, `created_at`, `updated_at`

`build_spac_link(raw: dict, *, state_id: int) -> SpacLink`.

`supported_filer_id` is a soft FK — leave it `None` when the SPAC record gives
only a name, not an identifier. (Resolving name → committee is a later-phase
concern; do not attempt fuzzy matching here.)

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_spac.py`: a failing test that
  `build_spac_link()` maps a sample SPAC dict to a `SpacLink`, including a case
  where `supported_filer_id` is absent and must be `None`.
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_spac.py -v`; expect
  failure.
- [ ] **Step 3** — Implement `app/core/source_models/spac.py`.
- [ ] **Step 4** — Implement `build_spac_link()` in `spac_ingest.py`.
- [ ] **Step 5** — Run tests; expect pass. Commit.
- [ ] **Step 6** — Add a test that `spac_links` creates via
  `SQLModel.metadata.create_all`. Run; pass; commit.

## Acceptance criteria

- [ ] `SpacLink` matches the interface contract; table creates cleanly.
- [ ] A SPAC record with no supported identifier yields `supported_filer_id =
  None` without error.
- [ ] `build_spac_link()` is covered by passing tests and importable from
  `app.core.source_models.spac_ingest`.
- [ ] No existing file is modified.

## Collision protocol

Branch `resolve/phase-0/task-0d-spac-links`. New files only — do not create
`app/core/source_models/__init__.py`.
