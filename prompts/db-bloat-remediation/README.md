# DB Bloat Remediation — Wave Map

## Overview

Cut ~90 GB DB size by:
1. Dropping `raw_data` provenance blobs from `unified_transactions` and `unified_reports`
2. Adding resolve-run retention (`cf resolve prune`)
3. Reclaiming dead-tuple/TOAST space (`VACUUM FULL`)
4. State-scoping hot ingest lookups

## File Ownership

**No two parallel tasks may edit the same file.**

| Task | Owns |
|------|------|
| 0-baseline | `scripts/db_size_report.py` (new), `app/cli/db.py` (new), `docs/db-bloat-baseline-*.md` (new) |
| 1a-campaign-rewire | `app/core/models/tables.py` (add campaign cols), `app/core/ingest_vectorized/families/flat_txns.py`, `app/core/ingest_vectorized/campaigns.py` |
| 1b-drop-txn-raw-data | `app/core/builders.py`, `app/core/ingest_vectorized/families/flat_txns_detail.py`, `app/core/ingest_vectorized/families/detail_children.py`, `app/core/models/tables.py` (drop raw_data field), `migrations/versions/` |
| 1z-integration | `tests/test_wave1_linkage.py` (new) |
| 2a-report-writers | `app/core/ingest_vectorized/families/reports.py`, `app/core/source_models/reports.py` |
| 2b-drop-report-raw-data | `app/core/source_models/reports_ingest.py`, `migrations/versions/` |
| 2z-integration | `tests/test_wave2_report_ingest.py` (new) |
| 3a-resolve-prune | `app/cli/resolve_prune.py` (new), `app/cli/main.py` |
| 3b-db-reclaim | `scripts/db_reclaim.py` (new), `docs/db-reclaim.md` (new) |
| 3z-integration | `docs/db-bloat-baseline-2026-06-17.md` (update) |
| 4a-address-lookup | `app/core/ingest_vectorized/common.py` (`full_address_lookup`) |
| 4b-id-map-scope | `app/core/ingest_vectorized/families/filer.py`, `app/core/ingest_vectorized/finalize.py` |
| 4z-integration | `tests/test_wave4_fk_parity.py` (new) |
| 5a-index-diet | `migrations/versions/` (index drop only) |
| 5b-uuid-native | `app/core/models/tables.py` (uuid columns), `migrations/versions/` — **SEPARATE PR** |

## Branch Conventions

- Phase branch: `db-bloat/phase`
- Wave branches: `db-bloat/wave-N/task-Nx`
- Tags: `db-bloat/wave-N-complete`

## Wave Order

```
PREP  → split briefs (this directory)
Wave 0 → baseline measurement (serial)
Wave 1 → drop unified_transactions.raw_data (serial: 1a → 1b → 1z)
Wave 2 → drop unified_reports.raw_data (serial: 2a → 2b → 2z)
Wave 3 → resolve retention + reclaim (3a ∥ 3b, then 3z serial)
Wave 4 → ingest performance (4a ∥ 4b, then 4z serial)
Wave 5 → index diet + UUID (optional, only if 0–4 all green)
```

## Spec

Full authoritative spec: `prompts/db-bloat-remediation/current.md`
Plan: `.cursor/plans/db_bloat_multitask_254a4de6.plan.md`
