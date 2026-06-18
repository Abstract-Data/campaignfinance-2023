# Model: gpt-5.3-codex-high-fast

# Task 5a: Drop Zero-Scan Indexes

## Phase

Wave 5 — optional. Only if waves 0–4 are all green.

## Branch

`db-bloat/wave-5/task-5a-index-diet`

## Prerequisite

All `db-bloat/wave-N-complete` tags (0–4) must exist.

## Objective

Using the Phase 0 `idx_scan` report from `docs/db-bloat-baseline-2026-06-17.md`,
generate an Alembic migration that drops zero-scan indexes not backing a unique/
dedup constraint or FK.

## NEVER Drop These

**Preserve all dedup indexes:**
- All `uix_persons_*` indexes
- All `uix_addresses_*` indexes
- `uix_transactions_state_type_sourceid`
- All `uix_txperson_*` indexes
- All `uix_entities_*` indexes
- Any index backing a FK constraint
- Any `PRIMARY KEY` index

## Implementation

### Step 1: Read baseline report

Open `docs/db-bloat-baseline-2026-06-17.md` and identify indexes with `idx_scan = 0`.

For each zero-scan index:
1. Check if it is a `uix_*` dedup index (preserve)
2. Check if it backs a FK (query `pg_constraint`)
3. Check if it backs a unique constraint (query `pg_indexes`)
4. If none of the above — candidate for dropping

### Step 2: Generate conservative migration

```bash
alembic revision -m "drop zero-scan non-constraint indexes"
```

In `upgrade()`: `DROP INDEX IF EXISTS <index_name>` for each candidate.
In `downgrade()`: `CREATE INDEX <index_name> ON <table>(<col>)` to restore them.

**Be conservative.** When in doubt, keep the index.

### Step 3: Run migration

```bash
alembic upgrade head
```

Gate hook will ASK for confirmation — surface to user.

### Step 4: Verify

Re-run `scripts/db_size_report.py` and confirm DB size reduction.

## Commit

```
feat: drop zero-scan non-constraint indexes via Alembic (5a)
```

## Checklist

- [ ] All waves 0–4 `*-complete` tags confirmed
- [ ] Phase 0 baseline report read
- [ ] Zero-scan candidates identified (excluding all `uix_*` and FK-backing indexes)
- [ ] Conservative list — when in doubt, keep
- [ ] Alembic migration generated with working `downgrade()`
- [ ] `alembic upgrade head` ran (gate ASK surfaced)
- [ ] DB size reduction verified
- [ ] `gitnexus_detect_changes()` run before commit
