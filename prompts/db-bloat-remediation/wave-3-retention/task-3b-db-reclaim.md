# Model: gpt-5.3-codex-high-fast

# Task 3b: VACUUM FULL Helper + Runbook

## Phase

Wave 3 — parallel with 3a (disjoint file ownership). Only after wave-2-complete.

## Branch

`db-bloat/wave-3/task-3b-db-reclaim`

## Objective

Add a `scripts/db_reclaim.py` script (and optionally `cf db reclaim` subcommand)
that runs `VACUUM (FULL, ANALYZE)` on the largest tables to reclaim dead-tuple space.
Create the runbook `docs/db-reclaim.md`.

## File Ownership

This task owns:
- `scripts/db_reclaim.py` (new)
- `docs/db-reclaim.md` (new)
- Optionally `app/cli/db.py` if adding `cf db reclaim` subcommand

Do NOT touch `app/cli/main.py` (owned by 3a) unless adding a `db` CLI group not
already modified by 3a. Coordinate to avoid conflicts.

## Critical: VACUUM FULL Requirements

`VACUUM FULL` MUST:
- Run with `AUTOCOMMIT=True` on a raw psycopg connection — NOT inside an ORM transaction
- NOT be issued inside a `BEGIN` / transaction block
- Be run largest table first (use Phase 0 report for ordering)
- Only run on local/dev DB (documents explicitly)

## Implementation

### scripts/db_reclaim.py

```python
#!/usr/bin/env python3
"""Run VACUUM FULL ANALYZE on large tables to reclaim dead-tuple space.

IMPORTANT: Only run this on the local/dev database.
For shared/online DBs, use pg_repack instead (see docs/db-reclaim.md).

Requires AUTOCOMMIT — cannot run inside a transaction block.
"""
```

Tables to vacuum (in order from Phase 0 baseline report, largest first):
```python
TABLES_TO_VACUUM = [
    "unified_transactions",
    "unified_reports",
    "match_decision",
    "scored_pairs",
    "candidate_pairs",
    "resolution_input",
    # ... remaining tables from Phase 0 report
]
```

Use psycopg2 (or psycopg3 if used) with `autocommit=True`:
```python
import psycopg2
conn = psycopg2.connect(dsn)
conn.autocommit = True
with conn.cursor() as cur:
    for table in TABLES_TO_VACUUM:
        cur.execute(f"VACUUM (FULL, ANALYZE) {table}")
```

Gate hook will ASK when this script runs VACUUM FULL — surface the ask to the user.

### docs/db-reclaim.md

Include:
- When to run (after bulk deletes, raw_data drops, resolve pruning)
- Lock implications: `VACUUM FULL` takes exclusive lock on the table
- Disk space requirement: free disk >= size of largest table being vacuumed
- How to run: `uv run python scripts/db_reclaim.py`
- `pg_repack` alternative for shared/online DBs
- Expected output and how to verify reclaim worked

## Commit

```
feat: add db_reclaim script + runbook (3b)
```

## Checklist

- [ ] Wave 2 `db-bloat/wave-2-complete` tag confirmed
- [ ] `scripts/db_reclaim.py` created with AUTOCOMMIT psycopg connection
- [ ] NOT inside ORM transaction block
- [ ] Tables in correct order (largest first)
- [ ] Gate hook ASK will trigger — document this
- [ ] `docs/db-reclaim.md` runbook created with lock/disk/pg_repack docs
- [ ] `gitnexus_detect_changes()` run before commit
