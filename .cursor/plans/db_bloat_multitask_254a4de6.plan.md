# DB Bloat Remediation — Multitask Plan
# ID: db_bloat_multitask_254a4de6
# Created: 2026-06-17
# Branch: db-bloat/phase

## Overview

Cut ~90 GB DB size by removing `raw_data` provenance blobs, adding resolve-run
retention, reclaiming dead-tuple space, and state-scoping hot ingest lookups.

## Model Routing

| Task | Model |
|------|-------|
| wave-0-baseline/task-0-baseline | gpt-5.3-codex-high-fast |
| wave-1-txn-raw-data/task-1a-campaign-rewire | claude-sonnet-4-6 |
| wave-1-txn-raw-data/task-1b-drop-txn-raw-data | claude-sonnet-4-6 |
| wave-1-txn-raw-data/task-1z-integration | claude-sonnet-4-6 |
| wave-2-report-raw-data/task-2a-report-writers | claude-sonnet-4-6 |
| wave-2-report-raw-data/task-2b-drop-report-raw-data | claude-sonnet-4-6 |
| wave-2-report-raw-data/task-2z-integration | gpt-5.3-codex-high-fast |
| wave-3-retention/task-3a-resolve-prune | claude-sonnet-4-6 |
| wave-3-retention/task-3b-db-reclaim | gpt-5.3-codex-high-fast |
| wave-3-retention/task-3z-integration | claude-sonnet-4-6 |
| wave-4-perf/task-4a-address-lookup | claude-sonnet-4-6 |
| wave-4-perf/task-4b-id-map-scope | claude-sonnet-4-6 |
| wave-4-perf/task-4z-integration | gpt-5.3-codex-high-fast |
| wave-5-followup/task-5a-index-diet | gpt-5.3-codex-high-fast |
| wave-5-followup/task-5b-uuid-native | claude-opus-4-6 (separate PR) |

## Wave Order

```
PREP  → create/split brief files
Wave 0 → baseline measurement (serial, no schema changes)
Wave 1 → drop unified_transactions.raw_data (strictly serial: 1a → 1b → 1z)
Wave 2 → drop unified_reports.raw_data (serial: 2a → 2b → 2z)
Wave 3 → resolve retention + reclaim (3a ∥ 3b, then 3z)
Wave 4 → ingest performance (4a ∥ 4b, then 4z)
Wave 5 → index diet + UUID native (optional, only if 0–4 green)
```

## Branch Conventions

- Phase branch: `db-bloat/phase`
- Wave branches: `db-bloat/wave-N/task-Nx`
- Tags: `db-bloat/wave-N-complete`

## File Ownership (no two parallel tasks may edit the same file)

| Task | Owns |
|------|------|
| 0 | scripts/db_size_report.py, app/cli/db.py (new), docs/db-bloat-baseline-*.md |
| 1a | app/core/models/tables.py (campaign cols), app/core/ingest_vectorized/families/flat_txns.py, app/core/ingest_vectorized/campaigns.py |
| 1b | app/core/builders.py, app/core/ingest_vectorized/families/flat_txns_detail.py, app/core/ingest_vectorized/families/detail_children.py, app/core/models/tables.py (drop raw_data), migrations/ |
| 1z | tests/test_wave1_linkage.py |
| 2a | app/core/ingest_vectorized/families/reports.py, app/core/source_models/reports.py |
| 2b | app/core/source_models/reports_ingest.py, migrations/ |
| 2z | tests/test_wave2_report_ingest.py |
| 3a | app/cli/resolve_prune.py (new), app/cli/main.py |
| 3b | scripts/db_reclaim.py (new), docs/db-reclaim.md (new) |
| 3z | docs/db-bloat-baseline-2026-06-17.md |
| 4a | app/core/ingest_vectorized/common.py (full_address_lookup) |
| 4b | app/core/ingest_vectorized/families/filer.py, app/core/ingest_vectorized/finalize.py |
| 4z | tests/test_wave4_fk_parity.py |
| 5a | migrations/ (index drop) |
| 5b | app/core/models/tables.py (uuid), migrations/ (separate PR) |

## Status

- [ ] PREP: Split briefs
- [ ] Wave 0: Baseline
- [ ] Wave 1: Drop unified_transactions.raw_data
- [ ] Wave 2: Drop unified_reports.raw_data
- [ ] Wave 3: Resolve retention + reclaim
- [ ] Wave 4: Ingest performance
- [ ] Wave 5: Index diet + UUID (optional)
