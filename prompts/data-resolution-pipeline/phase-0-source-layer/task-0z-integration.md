# Task 0z — Phase 0 integration: registry, dispatch, full load, verification

> **Phase 0, serial — runs LAST.** Depends on 0a, 0b, 0c, 0d, 0e, 0f all merged.
> Read the pack README and the source spec before starting.

## Context

Tasks 0a–0f each delivered a model (or discovery) in isolation. This task wires
them together, runs a full `tmp/texas` load through the new paths, and verifies
the source layer is complete. It is the only Phase-0 task that crosses task
boundaries — that is by design, so the parallel tasks never collided.

## Dependencies

- **Depends on:** 0a, 0b, 0c, 0d, 0e, 0f (all merged to the Phase 0 branch).
- **Blocks:** Phase 1.

## Files

- **Create:** `app/core/source_models/__init__.py` — imports every new model so
  `SQLModel.metadata` registers them; exposes the builder registry.
- **Modify:** `scripts/loaders/production_loader.py` — wire record-type dispatch.
- **Create:** `tests/resolve/test_phase0_integration.py`.
- **Optional:** update the coverage matrix in `docs/DATA_RELATIONSHIPS.md`.

## What to build

1. **`app/core/source_models/__init__.py`** — import `UnifiedReport`,
   `UnifiedPledge`, `ExpenditureCategory`, `CommitteePurpose`, `SpacLink`,
   `UnifiedNotice` (registers their tables), and expose:

   ```
   RECORD_TYPE_BUILDERS = {
       "CVR1": build_report,          # task 0a
       "PLDG": build_pledge,          # task 0b — detail row alongside the txn
       "EXCAT": build_expenditure_category,  # task 0c
       "CVR3": build_committee_purpose,      # task 0c
       "SPAC": build_spac_link,       # task 0d
       "CVR2": build_notice,          # task 0e
   }
   ```

2. **Dispatch in `production_loader.py`** — for each file discovered by
   `discover_state_files()` (task 0f), route by `record_type`:
   transaction types (`RCPT`/`EXPN`/`LOAN`/`PLDG`/`DEBT`/`CRED`/`TRVL`/`ASSET`/
   `CAND`) keep the existing `unified_sql_processor` path; the new types
   (`CVR1`/`CVR2`/`CVR3`/`EXCAT`/`SPAC`) route to `RECORD_TYPE_BUILDERS`.
   `PLDG` also gets a `UnifiedPledge` detail row via `build_pledge()`.

3. **Post-load linking** — after a load, call
   `link_transactions_to_reports(session)` (task 0a) and log the count linked.

4. **Reconciliation check** — for a sample of reports, compare
   `UnifiedReport.total_contributions` / `total_expenditures` against the summed
   `unified_transactions` linked to that report; log mismatches beyond a
   tolerance. This is a data-quality signal, not a hard failure.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_phase0_integration.py`: a failing
  test that every model in `app.core.source_models` is registered in
  `SQLModel.metadata.tables` and that `RECORD_TYPE_BUILDERS` has a builder for
  each of `CVR1`, `CVR2`, `CVR3`, `EXCAT`, `SPAC`.
- [ ] **Step 2** — Run it; expect failure.
- [ ] **Step 3** — Create `app/core/source_models/__init__.py` with the imports
  and `RECORD_TYPE_BUILDERS`. Run; pass; commit.
- [ ] **Step 4** — Write a failing integration test that loading a small
  fixture set (a few rows per record type) routes each record to the right
  table and that `link_transactions_to_reports()` links txns to a report.
- [ ] **Step 5** — Wire the dispatch + post-load linking in
  `production_loader.py`. Run; pass; commit.
- [ ] **Step 6** — Run a full `tmp/texas` load (`uv run python
  scripts/loaders/production_loader.py` against the texas state directory) into
  a scratch database. Confirm non-zero row counts in `unified_reports`,
  `unified_pledges`, `expenditure_categories`, `committee_purposes`,
  `spac_links`, `unified_notices`, and that `unified_transactions.report_id` is
  populated for the bulk of rows. Commit.

## Acceptance criteria

- [ ] `uv run pytest tests/resolve/` is fully green.
- [ ] A full `tmp/texas` load completes; every new table has non-zero rows.
- [ ] The majority of `unified_transactions` rows have a non-null `report_id`.
- [ ] The reconciliation check runs and logs a summary (mismatches are reported,
  not silently dropped).
- [ ] No `_ss`/`_t` file is skipped.

## Collision protocol

Branch `resolve/phase-0/task-0z-integration`, cut after 0a–0f are merged. This
task is *expected* to edit shared files (`__init__.py`, `production_loader.py`) —
that is why it runs alone. If 0a–0f left any interface gap, fix it here and note
the deviation rather than reopening a parallel task.
