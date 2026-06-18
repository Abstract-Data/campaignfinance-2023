# Model: gpt-5.3-codex-high-fast

# Task 0: DB Size Baseline Measurement

## Phase

Wave 0 — no schema changes, read-only measurement.

## Branch

`db-bloat/wave-0/task-0-baseline`

## Objective

Capture a baseline DB size snapshot so the reduction is provable after later waves.

## Deliverables

1. `scripts/db_size_report.py` — runs both SQL queries below, prints results, and saves to `docs/db-bloat-baseline-YYYY-MM-DD.md`.
2. Optionally: `cf db size` Typer subcommand in `app/cli/db.py` wired into `app/cli/main.py`.
3. `docs/db-bloat-baseline-2026-06-17.md` — the actual baseline output.

## SQL Queries to Run

### Table sizes

```sql
SELECT relname AS table,
       pg_size_pretty(pg_total_relation_size(c.oid))                              AS total,
       pg_size_pretty(pg_relation_size(c.oid))                                    AS heap,
       pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid))    AS toast_plus_idx,
       s.n_dead_tup, s.n_live_tup
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relkind = 'r' AND n.nspname = 'public'
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 30;
```

### Index usage

```sql
SELECT i.relname AS index, t.relname AS table,
       pg_size_pretty(pg_relation_size(i.oid)) AS size, s.idx_scan
FROM pg_stat_user_indexes s
JOIN pg_class i ON i.oid = s.indexrelid
JOIN pg_class t ON t.oid = s.relid
ORDER BY pg_relation_size(i.oid) DESC;
```

### Total DB size

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));
```

## Implementation Notes

- Use `app/logger.py` Logger, never `print()` in `app/`.
- Connect using existing DB config from `app/core/unified_database.py` or `app/states/postgres_config.py`.
- The script should be runnable via `uv run python scripts/db_size_report.py`.
- Save output to `docs/db-bloat-baseline-YYYY-MM-DD.md` with today's date in filename.

## Commit

```
feat: add db_size_report script + baseline measurement (Wave 0)
```

## Tag After Completion

```
db-bloat/wave-0-complete
```

## Checklist

- [ ] `scripts/db_size_report.py` created and runnable
- [ ] Optionally `app/cli/db.py` with `cf db size` subcommand
- [ ] `docs/db-bloat-baseline-2026-06-17.md` created with actual output
- [ ] Total DB size recorded
- [ ] Committed and tagged `db-bloat/wave-0-complete`
