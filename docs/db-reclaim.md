# DB Reclaim Runbook

## Overview

After bulk operations (raw_data column drops, resolve-run pruning, truncate+reload),
Postgres retains dead tuples in the heap. The data is logically gone but the disk
space is not returned to the OS until a `VACUUM FULL` is run.

## When to Run

Run `scripts/db_reclaim.py` after:
- Completing Waves 1–3 of the DB Bloat Remediation plan
- Running `cf resolve prune` to remove stale resolve runs
- Any large `TRUNCATE` or bulk-row operations

## How to Run

```bash
# Full vacuum (recommended) — runs VACUUM (FULL, ANALYZE) on all large tables
uv run python scripts/db_reclaim.py

# Skip ANALYZE step
uv run python scripts/db_reclaim.py --skip-analyze

# Target specific tables only
uv run python scripts/db_reclaim.py --tables unified_transactions,unified_reports
```

## Lock Implications

**`VACUUM FULL` takes an exclusive lock** on each table. During the vacuum:
- No reads or writes are permitted on the locked table
- Other tables are unaffected (vacuum runs table-by-table)
- Lock is held for the duration of the vacuum of that table

This is **only safe on the local/dev database**. Never run `VACUUM FULL` on
a database that is serving live traffic.

## Disk Space Requirements

`VACUUM FULL` rewrites the table to a new heap file. It requires free disk space
roughly equal to the size of the largest table being vacuumed.

Check available disk space before running on a large DB:
```bash
df -h /var/lib/postgresql   # or your Postgres data directory
```

## pg_repack Alternative (for shared/online DBs)

For a shared or production database that cannot be taken offline:

```bash
# Install pg_repack extension
# Then run (no exclusive lock, online-safe):
pg_repack -t unified_transactions -d campaign_finance
pg_repack -t unified_reports -d campaign_finance
```

`pg_repack` achieves the same space reclamation as `VACUUM FULL` without the
exclusive lock, using a trigger-based approach. It requires more disk space
(the new copy of the table must exist alongside the old one during repack).

See https://github.com/reorg/pg_repack for installation and usage.

## Measuring the Result

Run the Phase 0 measurement script before and after:

```bash
# Before: save baseline (already done in Wave 0)
uv run python scripts/db_size_report.py

# ... run db_reclaim.py ...

# After: save updated sizes
uv run python scripts/db_size_report.py --output docs/db-bloat-post-reclaim-YYYY-MM-DD.md
```

Compare the `Total DB size` line between the two reports.

## Expected Outcome

After Waves 1–3 on a ~90 GB DB with raw_data blobs:
- The `unified_transactions` table should shrink by ~50–70% (raw_data was the
  largest column, storing full ~42-column JSON per row)
- The `unified_reports` table should shrink by ~20–40%
- Total DB size should drop significantly once VACUUM FULL returns dead-tuple space

The exact reduction depends on how many rows were loaded and how many re-loads
occurred (each re-load created more dead tuples via COPY + conflict resolution).
