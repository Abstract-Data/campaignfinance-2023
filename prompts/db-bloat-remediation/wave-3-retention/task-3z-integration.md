# Model: claude-sonnet-4-6

# Task 3z: Wave 3 Integration — Run Prune + Reclaim + Record New Size

## Phase

Wave 3 — serial integration. Only start **after both 3a and 3b are merged**.

## Branch

`db-bloat/wave-3/task-3z-integration`

## Prerequisite

Tasks 3a and 3b must both be merged.

## Objective

Run the prune and reclaim commands on the dev DB, then re-measure DB size and
record the new total in the baseline doc.

## Steps

### 1. Run cf resolve prune

```bash
uv run cf resolve prune --keep 1
```

Verify:
- Stale run rows deleted
- Latest run intact (can still query resolve results)
- Print rows deleted per table

### 2. Run db_reclaim (gate ASK will trigger)

```bash
uv run python scripts/db_reclaim.py
```

Gate hook will ASK for VACUUM FULL confirmation — surface to user and wait for approval.
After approval:
- Verify VACUUM FULL completes for each table
- No errors

### 3. Re-run db_size_report

```bash
uv run python scripts/db_size_report.py
```

Record new total DB size. Update `docs/db-bloat-baseline-2026-06-17.md` with a
"After Wave 3" section comparing before/after sizes.

## Commit

```
docs: wave-3 integration — prune + reclaim + updated baseline size (3z)
```

## Tag After Completion

```
db-bloat/wave-3-complete
```

## Checklist

- [ ] Tasks 3a and 3b confirmed merged
- [ ] `cf resolve prune` ran successfully — stale rows deleted, latest intact
- [ ] `scripts/db_reclaim.py` ran (gate ASK surfaced and approved)
- [ ] `scripts/db_size_report.py` re-ran — new sizes captured
- [ ] `docs/db-bloat-baseline-2026-06-17.md` updated with before/after comparison
- [ ] DB size materially smaller than baseline
- [ ] Tagged `db-bloat/wave-3-complete`
