# Changelog — db-bloat-remediation

## v1.0.0 — 2026-06-17
- Initial prompt. Remediates the ~90 GB local DB bloat and upload/matching speed.
- Phase 0: measure baseline (per-table + per-index size/usage).
- Phase 1: drop `unified_transactions.raw_data` after rewiring campaign derivation
  (the only consumer) onto narrow source columns / inline finalization.
- Phase 2: drop `unified_reports.raw_data` (at-filing cols already set at insert;
  backfill legacy rows first). `IngestError.raw_data` kept.
- Phase 3: `cf resolve prune` run-retention + `VACUUM FULL` reclaim runbook.
- Phase 4: state-scope the full-table address/id-map ingest lookups.
- Phase 5 (follow-up): drop zero-scan indexes; UUID text→native `uuid` (separate PR).
