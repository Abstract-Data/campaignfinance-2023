# Model: claude-sonnet-4-6

# Task 1b: Stop Writing raw_data + Drop Column via Alembic

## Phase

Wave 1 — strictly serial. Only start **after task 1a is merged**.

## Branch

`db-bloat/wave-1/task-1b-drop-txn-raw-data`

## Prerequisite

Task 1a must be merged. The three `campaign_*_src` columns must exist on
`UnifiedTransaction` and campaigns.py must be reading them. Do NOT proceed
if 1a is not merged.

## Objective

Remove all writes to `unified_transactions.raw_data`, remove the SQLModel field,
and generate an Alembic migration that drops the column.

## File Ownership

This task owns:
- `app/core/builders.py` — remove raw_data from build_transaction()
- `app/core/ingest_vectorized/families/flat_txns_detail.py` — remove raw_data writes
- `app/core/ingest_vectorized/families/detail_children.py` — remove raw_data writes
- `app/core/models/tables.py` — remove raw_data field from UnifiedTransaction
- `migrations/versions/` — new Alembic migration

## Mandatory Pre-work

Run `gitnexus_detect_changes()` to confirm only expected files changed from 1a.
Use Context7 for current Alembic + SQLModel docs.

## Implementation

### Step 1: Remove raw_data population

**builders.py** (`build_transaction()` ~L84):
Remove the line that sets `raw_data=json.dumps(record)` or equivalent.

**flat_txns_detail.py** (~L996/L1009):
Remove `raw_data` from the column list / frame passed to the COPY writer for
`unified_transactions`.

**detail_children.py** (~L996/L1009):
Same — remove `raw_data` from the column list.

**common.py** (`raw_json_expr()`):
Keep this function ONLY if it is still used transiently in-memory (e.g., for
`IngestError` population). Do NOT delete it if it feeds `ingest_errors`.
Just ensure nothing routes its output to a persisted `raw_data` transaction column.

### Step 2: Remove field from model

In `app/core/models/tables.py`, remove the `raw_data` field from `UnifiedTransaction`
(~L385). Do NOT touch `IngestError.raw_data` (~L1094) — that is intentionally kept.

### Step 3: Generate Alembic migration

```bash
alembic revision -m "drop unified_transactions.raw_data + add campaign source cols"
```

In `upgrade()`:
```python
# ADD the three campaign source columns (from 1a)
op.add_column('unified_transactions',
    sa.Column('campaign_office_src', sa.String(200), nullable=True))
op.add_column('unified_transactions',
    sa.Column('campaign_district_src', sa.String(200), nullable=True))
op.add_column('unified_transactions',
    sa.Column('campaign_name_src', sa.String(200), nullable=True))
# DROP raw_data
op.drop_column('unified_transactions', 'raw_data')
```

In `downgrade()`:
```python
# Re-add raw_data as nullable Text (LOSSY — cannot reconstruct original JSON)
# WARNING: downgrade re-adds the column as empty/NULL; original data is gone.
op.add_column('unified_transactions',
    sa.Column('raw_data', sa.Text(), nullable=True))
# Drop campaign source cols
op.drop_column('unified_transactions', 'campaign_office_src')
op.drop_column('unified_transactions', 'campaign_district_src')
op.drop_column('unified_transactions', 'campaign_name_src')
```

Add docstring to the migration explaining the lossiness.

### Step 4: Run migration

```bash
alembic upgrade head
```

Gate hook will ASK for confirmation — surface this to the user, do not bypass.

### Step 5: Run tests

```bash
uv run pytest tests app/tests -x --ignore=tests/resolve
```

Must be green.

### Step 6: gitnexus_detect_changes

Run before commit.

## Commit

```
feat: drop unified_transactions.raw_data via Alembic migration (1b)
```

## Checklist

- [ ] Task 1a verified merged before starting
- [ ] `builders.py` no longer writes `raw_data` on transactions
- [ ] `flat_txns_detail.py` raw_data removed from column list
- [ ] `detail_children.py` raw_data removed from column list
- [ ] `raw_json_expr()` in `common.py` kept only if still feeding ingest_errors
- [ ] `UnifiedTransaction.raw_data` field removed from `tables.py`
- [ ] `IngestError.raw_data` (~L1094) untouched
- [ ] Alembic migration generated with working upgrade() and downgrade()
- [ ] `alembic upgrade head` ran successfully (gate ASK surfaced to user)
- [ ] `uv run pytest tests app/tests -x --ignore=tests/resolve` passes
- [ ] `gitnexus_detect_changes()` run before commit
