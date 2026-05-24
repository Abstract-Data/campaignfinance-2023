# Task 0c — `ExpenditureCategory` + `CommitteePurpose` lookup tables

> **Phase 0, round 1. Parallel-safe with 0a, 0b, 0d–0f.** Blocks `task-0z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Two TEC
record types in `tmp/texas/` are reference data that nothing currently models:

- `EXCAT` (`expn_catg.parquet`) — the expenditure category-code dictionary.
- `CVR3` (`purpose.parquet`) — a committee's stated purpose, per report.

Without these, expenditure purpose is free text and cannot be grouped or
indexed. This task adds both as small lookup tables.

Authoritative field layout: `tmp/texas/CFS-ReadMe.txt`, records `ExpendCategory`
(`recordType = EXCAT`) and `CoverSheet3Data` (`recordType = CVR3`).

## Dependencies

- **Depends on:** none — Phase 0 round 1.
- **Blocks:** `task-0z`.
- **Parallel-safe with:** 0a, 0b, 0d, 0e, 0f.

## Files

- **Create:** `app/core/source_models/lookups.py` — `ExpenditureCategory` and
  `CommitteePurpose` SQLModels.
- **Create:** `app/core/source_models/lookups_ingest.py` —
  `build_expenditure_category()` and `build_committee_purpose()`.
- **Create:** `tests/resolve/test_lookups.py`.

New files only — no existing file is edited.

## Interface contract

`ExpenditureCategory` (table `expenditure_categories`):

- `code: str` PK — the TEC category code
- `description: str | None`
- `created_at`, `updated_at`

`CommitteePurpose` (table `committee_purposes`):

- `id: int` PK, `uuid: str`
- `committee_id: str` FK → `unified_committees.filer_id` (string FK)
- `report_ident: str | None` — the `reportInfoIdent` the purpose was filed on
- `state_id: int | None` FK → `states.id`
- `purpose_text: str | None`, `form_type: str | None`
- `created_at`, `updated_at`

Functions in `lookups_ingest.py`:

- `build_expenditure_category(raw: dict) -> ExpenditureCategory`
- `build_committee_purpose(raw: dict, *, state_id: int) -> CommitteePurpose`

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_lookups.py`: failing tests that
  each `build_*` function maps a sample raw dict (real column names from
  `CFS-ReadMe.txt`) to its model.
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_lookups.py -v`; expect
  failure.
- [ ] **Step 3** — Implement `app/core/source_models/lookups.py`.
- [ ] **Step 4** — Implement both builders in `lookups_ingest.py`.
- [ ] **Step 5** — Run tests; expect pass. Commit.
- [ ] **Step 6** — Add a test that both tables create via
  `SQLModel.metadata.create_all` and that `ExpenditureCategory.code` rejects a
  duplicate insert (PK uniqueness). Run; pass; commit.

## Acceptance criteria

- [ ] Both models match the interface contract; both tables create cleanly.
- [ ] Both builders are covered by passing tests and importable from
  `app.core.source_models.lookups_ingest`.
- [ ] No existing file is modified.

## Collision protocol

Branch `resolve/phase-0/task-0c-lookups`. New files only — do not create
`app/core/source_models/__init__.py`.
