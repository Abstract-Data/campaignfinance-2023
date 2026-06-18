# Model: claude-sonnet-4-6

# Task 2b: Backfill Legacy Rows + Drop unified_reports.raw_data

## Phase

Wave 2 — serial. Only start **after task 2a is merged**.

## Branch

`db-bloat/wave-2/task-2b-drop-report-raw-data`

## Prerequisite

Task 2a must be merged (both report writers confirmed to write at-filing cols).

## Objective

Backfill legacy rows that have NULL at-filing columns but have `raw_data`,
stop writing `raw_data` in both writers, drop the column via Alembic.

## File Ownership

This task owns:
- `app/core/source_models/reports_ingest.py` — stop writing raw_data
- `migrations/versions/` — new Alembic migration

Do NOT touch `app/core/ingest_vectorized/families/reports.py` (owned by 2a).

## Context

- `app/core/source_models/reports_ingest.py` (~L95-112) — ORM report insert; sets at-filing cols already
- `app/core/source_models/reports_ingest.py` (~L160) — `backfill_report_at_filing()` exists

## Implementation

### Step 1: Stop writing raw_data in ORM writer

In `app/core/source_models/reports_ingest.py` (~L95-112), remove the line that
sets `raw_data=json.dumps(...)` or equivalent on the report insert.

Use Context7 for current SQLModel docs.

### Step 2: Generate Alembic migration

```bash
alembic revision -m "drop unified_reports.raw_data"
```

In `upgrade()`:
```python
# Backfill committee_name_at_filing and treasurer_name_at_filing from raw_data
# for any legacy rows that have NULL at-filing cols but have raw_data populated
op.execute("""
    UPDATE unified_reports
    SET committee_name_at_filing = (raw_data::jsonb ->> 'filerName'),
        treasurer_name_at_filing = (
            COALESCE(raw_data::jsonb ->> 'treasurerNameFirst', '') || ' ' ||
            COALESCE(raw_data::jsonb ->> 'treasurerNameLast', '')
        )
    WHERE raw_data IS NOT NULL
      AND (committee_name_at_filing IS NULL OR treasurer_name_at_filing IS NULL)
""")

# Now drop the column
op.drop_column('unified_reports', 'raw_data')
```

In `downgrade()`:
```python
# Re-add as nullable Text (LOSSY — original JSON data is gone)
# WARNING: downgrade restores the column as NULL for all rows.
op.add_column('unified_reports',
    sa.Column('raw_data', sa.Text(), nullable=True))
```

### Step 3: Run migration

```bash
alembic upgrade head
```

Gate hook will ASK for confirmation — surface to user.

### Step 4: Run tests

```bash
uv run pytest tests app/tests -x --ignore=tests/resolve
```

## Commit

```
feat: drop unified_reports.raw_data via Alembic migration (2b)
```

## Checklist

- [ ] Task 2a confirmed merged
- [ ] Context7 consulted for SQLModel + Alembic docs
- [ ] ORM writer in `reports_ingest.py` no longer writes `raw_data`
- [ ] Vectorized writer in `reports.py` no longer writes `raw_data` (verify 2a fix)
- [ ] Alembic migration generated with backfill in `upgrade()` and lossy `downgrade()`
- [ ] `alembic upgrade head` ran (gate ASK surfaced to user)
- [ ] Tests pass
- [ ] `gitnexus_detect_changes()` run before commit
