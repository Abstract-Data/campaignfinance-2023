# Task 0a — `UnifiedReport` model + CVR1 ingestion + `report_id` link

> **Phase 0, round 1. Parallel-safe with 0b–0f.** Blocks `task-0z`.
> Read the pack README and the source spec before starting.

## Context

This is the `campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres).
Texas TEC cover-sheet records (`CVR1`, files `cover*.parquet` in `tmp/texas/`)
are the *reports* every transaction belongs to — they carry the filer, the
reporting period, and declared totals. Today nothing in the unified layer models
a report, and transactions keep only a scalar `filed_date`. This task adds the
report model and the link.

Authoritative field layout: `tmp/texas/CFS-ReadMe.txt`, record `CoverSheet1Data`
(`recordType = CVR1`). Use the actual TEC column names from that file.

## Dependencies

- **Depends on:** none — Phase 0 round 1.
- **Blocks:** `task-0z` (integration).
- **Parallel-safe with:** 0b, 0c, 0d, 0e, 0f.

## Files

- **Create:** `app/core/source_models/reports.py` — the `UnifiedReport` SQLModel.
- **Create:** `app/core/source_models/reports_ingest.py` — `build_report()` and
  `link_transactions_to_reports()`.
- **Modify:** `app/core/unified_sqlmodels.py` — add `report_id` column +
  `report` relationship to `UnifiedTransaction`. **This is the only Phase-0 task
  that edits this file.**
- **Create:** `tests/resolve/test_reports.py`.

## Interface contract

Other tasks and `task-0z` depend on these exact names.

`UnifiedReport` (table `unified_reports`) — key columns:

- `id: int` PK, `uuid: str`
- `state_id: int` FK → `states.id`
- `committee_id: str` FK → `unified_committees.filer_id` (string FK — holds the
  `filerIdent`; matches the existing `unified_transactions.committee_id` pattern)
- `report_ident: str` — the TEC `reportInfoIdent` (indexed)
- `form_type: str | None`, `filed_date: date | None`
- `period_start: date | None`, `period_end: date | None`
- `is_final: bool` (default `False` — set when a FINL record references the report)
- Declared totals (`Numeric(15,2)`, nullable): `total_contributions`,
  `total_unitemized_contributions`, `total_expenditures`,
  `total_unitemized_expenditures`, `loan_balance`, `contributions_maintained`,
  `cash_on_hand`
- `file_origin_id: str | None` FK → `file_origins.id`
- `raw_data: str | None`, `created_at`, `updated_at`

`UnifiedTransaction` gains: `report_id: int | None` FK → `unified_reports.id`
(indexed) and a `report` relationship.

Functions in `reports_ingest.py`:

- `build_report(raw: dict, *, state_id: int, file_origin_id: str | None) -> UnifiedReport`
- `link_transactions_to_reports(session) -> int` — sets `report_id` on
  transactions by matching `unified_transactions` to `unified_reports` on
  `(state_id, report_ident)`; returns the count linked. `task-0z` calls this
  after a load.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_reports.py`: a failing test that
  `build_report()` maps a sample CVR1 dict (use real column names from
  `CFS-ReadMe.txt`) to a `UnifiedReport` with the period dates and totals parsed
  (dates → `date`, totals → `Decimal`).
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_reports.py -v`; expect
  failure (module not found).
- [ ] **Step 3** — Implement `app/core/source_models/reports.py` (the model per
  the interface contract above).
- [ ] **Step 4** — Implement `build_report()` in `reports_ingest.py` (parse
  dates and `Decimal` amounts; reuse the parsing helpers' style from
  `app/core/unified_sqlmodels.py` — `_parse_date`, `_parse_amount`).
- [ ] **Step 5** — Run the test; expect pass. Commit.
- [ ] **Step 6** — Add a failing test for `link_transactions_to_reports()`:
  seed two transactions and one report sharing a `report_ident`, assert the
  function links both and returns `2`.
- [ ] **Step 7** — Add `report_id` + `report` relationship to
  `UnifiedTransaction` in `unified_sqlmodels.py`; implement
  `link_transactions_to_reports()`.
- [ ] **Step 8** — Run the full file `uv run pytest tests/resolve/test_reports.py -v`;
  expect pass. Commit.

## Acceptance criteria

- [ ] `UnifiedReport` matches the interface contract; `unified_reports` table
  creates cleanly via `SQLModel.metadata.create_all`.
- [ ] `UnifiedTransaction.report_id` exists and is indexed.
- [ ] `build_report()` and `link_transactions_to_reports()` are covered by
  passing tests and importable from `app.core.source_models.reports_ingest`.
- [ ] No file outside the Files list is modified.

## Collision protocol

Branch `resolve/phase-0/task-0a-reports`. Do **not** create
`app/core/source_models/__init__.py` (that is `task-0z`'s). Your only edit
outside `app/core/source_models/` is the `UnifiedTransaction` change in
`unified_sqlmodels.py` — keep it to the `report_id` column and `report`
relationship. If you need any other shared-file change, stop and flag it.
