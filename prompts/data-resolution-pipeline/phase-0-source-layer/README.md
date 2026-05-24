# Phase 0 — Source-layer completion

**Goal:** Close the gaps in the source layer so that *all* `tmp/texas` data is
loaded, modeled, and linked to the reports it belongs to. This is a prerequisite
for the resolution pipeline (Phases 1+), which treats `unified_*` as a complete
source of truth.

**Outcome when done:** Every TEC record type in `tmp/texas` has a model and an
ingestion path; transactions carry a `report_id` linking them to their cover
sheet; the loader picks up every file (including the `_ss`/`_t` variants).

See the spec's "Phase 0 additions" section and Appendix A for the coverage
matrix and the authoritative record list (`tmp/texas/CFS-ReadMe.txt`).

## Parallel tasks

Tasks **0a–0f run concurrently** (round 1). They are collision-free because each
owns a distinct set of files. **0z runs last** (integration), after 0a–0f merge.

| Task | Delivers | Owns (creates unless noted) |
|------|----------|------------------------------|
| `task-0a` | `UnifiedReport` (CVR1) + `unified_transactions.report_id` | `app/core/source_models/reports.py`, `reports_ingest.py`; **edits `app/core/unified_sqlmodels.py`** (only 0a does) |
| `task-0b` | `UnifiedPledge` detail table (PLDG) | `app/core/source_models/pledges.py`, `pledges_ingest.py` |
| `task-0c` | `ExpenditureCategory` + `CommitteePurpose` (EXCAT, CVR3) | `app/core/source_models/lookups.py`, `lookups_ingest.py` |
| `task-0d` | `SpacLink` (SPAC) | `app/core/source_models/spac.py`, `spac_ingest.py` |
| `task-0e` | `UnifiedNotice` (CVR2) | `app/core/source_models/notices.py`, `notices_ingest.py` |
| `task-0f` | Directory-glob ingestion (incl. `_ss`/`_t`) | **edits `scripts/loaders/loader_config.py`, `production_loader.py`** (only 0f does) |
| `task-0z` | Registry wiring + full `tmp/texas` ingest + verification | `app/core/source_models/__init__.py`; edits the loader dispatch |

## Collision-freedom

- 0a–0e create new files only under `app/core/source_models/`, each a distinct
  filename. No two tasks write the same file.
- 0a is the **only** task that edits `app/core/unified_sqlmodels.py` (adds the
  `report_id` column + `report` relationship to `UnifiedTransaction`).
- 0f is the **only** task that edits the loader scripts.
- `app/core/source_models/__init__.py` is created by **0z alone** — 0a–0e must
  not create it. Each of 0a–0e exposes its model and a `build_*` function from
  its own module; 0z imports them in `__init__.py` and wires the dispatch.

## Verifying the phase

`task-0z` is done when `uv run pytest tests/resolve/` is green and a full
`tmp/texas` load produces non-zero row counts in every new table, with the
transaction → report reconciliation check (declared totals vs. summed
transactions) within tolerance. See `task-0z-integration.md`.
