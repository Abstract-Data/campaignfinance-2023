# Task 0e — `UnifiedNotice` model (CVR2)

> **Phase 0, round 1. Parallel-safe with 0a–0d, 0f.** Blocks `task-0z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The TEC
`CVR2` record (`notices.parquet` in `tmp/texas/`) holds notices received by
candidates and officeholders, reported at the bottom of Cover Sheet page 1.
Nothing models it today. This task adds `UnifiedNotice`.

Authoritative field layout: `tmp/texas/CFS-ReadMe.txt`, record `CoverSheet2Data`
(`recordType = CVR2`).

## Dependencies

- **Depends on:** none — Phase 0 round 1.
- **Blocks:** `task-0z`.
- **Parallel-safe with:** 0a, 0b, 0c, 0d, 0f.

## Files

- **Create:** `app/core/source_models/notices.py` — the `UnifiedNotice` SQLModel.
- **Create:** `app/core/source_models/notices_ingest.py` — `build_notice()`.
- **Create:** `tests/resolve/test_notices.py`.

New files only — no existing file is edited.

## Interface contract

`UnifiedNotice` (table `unified_notices`):

- `id: int` PK, `uuid: str`
- `committee_id: str | None` FK → `unified_committees.filer_id`
- `report_ident: str | None` — the `reportInfoIdent` the notice was filed on
- `state_id: int | None` FK → `states.id`
- `notice_date: date | None`
- `notice_from: str | None` — the filer/committee the notice came from, as filed
- `description: str | None`
- `raw_data: str | None`, `created_at`, `updated_at`

Map the CVR2 columns from `CFS-ReadMe.txt` onto these fields; keep anything that
does not map cleanly in `raw_data` (JSON string). `build_notice(raw: dict, *,
state_id: int) -> UnifiedNotice`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_notices.py`: a failing test that
  `build_notice()` maps a sample CVR2 dict (real column names from
  `CFS-ReadMe.txt`) to a `UnifiedNotice` with `notice_date` parsed to a `date`.
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_notices.py -v`; expect
  failure.
- [ ] **Step 3** — Implement `app/core/source_models/notices.py`.
- [ ] **Step 4** — Implement `build_notice()` in `notices_ingest.py`.
- [ ] **Step 5** — Run tests; expect pass. Commit.
- [ ] **Step 6** — Add a test that `unified_notices` creates via
  `SQLModel.metadata.create_all`. Run; pass; commit.

## Acceptance criteria

- [ ] `UnifiedNotice` matches the interface contract; table creates cleanly.
- [ ] `build_notice()` is covered by passing tests and importable from
  `app.core.source_models.notices_ingest`.
- [ ] No existing file is modified.

## Collision protocol

Branch `resolve/phase-0/task-0e-notices`. New files only — do not create
`app/core/source_models/__init__.py`.
