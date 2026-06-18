# Model: claude-sonnet-4-6

# Task 1a: Rewire Campaign Derivation off raw_data

## Phase

Wave 1 — strictly serial. This task must complete and be merged before 1b starts.

## Branch

`db-bloat/wave-1/task-1a-campaign-rewire`

## Objective

`campaigns.py` is the **only** consumer of `unified_transactions.raw_data`.
`_transaction_frame()` SELECTs `raw_data` from the DB and `_office_expr()` parses
office/district out of it with `json_path_match`. Rewire this before dropping the column.

## File Ownership

This task owns:
- `app/core/models/tables.py` — add three new columns only (do NOT drop raw_data yet)
- `app/core/ingest_vectorized/families/flat_txns.py` — derive and populate new cols
- `app/core/ingest_vectorized/campaigns.py` — rewire _transaction_frame() and _office_expr()
- Test fixture and golden test

## Mandatory Pre-work

Run `gitnexus_impact` on each of these symbols before touching them:
- `build_transaction`
- `_transaction_frame`
- `_office_expr`
- `finalize_campaigns`
- `flat_txns` family `run()`

**STOP and report if any returns HIGH or CRITICAL risk.**

Use Context7 (`resolve-library-id` → `get-library-docs`) for current Polars and SQLModel
docs before writing any Polars expressions or SQLModel field definitions.

## Implementation (Option 1a-i — preferred)

### Step 1: Add three narrow columns to UnifiedTransaction

In `app/core/models/tables.py`, add to `UnifiedTransaction` (~L385 area, after existing
campaign-related fields):

```python
campaign_office_src: Optional[str] = Field(default=None, max_length=200)
campaign_district_src: Optional[str] = Field(default=None, max_length=200)
campaign_name_src: Optional[str] = Field(default=None, max_length=200)
```

These must be:
- Nullable (`Optional[str]`)
- Un-indexed (no `index=True`)
- Short strings (`max_length=200` or similar), NOT `Text`
- NULL for rows with no campaign data (most rows)

Do NOT drop `raw_data` in this task.

### Step 2: Derive values in flat_txns ingest

In `app/core/ingest_vectorized/families/flat_txns.py` (~L210-224), during the
in-memory Polars frame processing, extract the three values from the source
parquet columns (they are already in memory during ingest):

Map:
- `campaign_office_src` ← source col with office description (check the TEC parquet schema)
- `campaign_district_src` ← source col with district
- `campaign_name_src` ← source col with campaign name / filer name

Use Polars `.alias()` expressions. These columns feed into the COPY writer for
`unified_transactions`.

### Step 3: Rewire campaigns.py

In `app/core/ingest_vectorized/campaigns.py`:
- `_transaction_frame()` (~L125-148): SELECT `campaign_office_src`,
  `campaign_district_src`, `campaign_name_src` instead of `raw_data`
- `_office_expr()` (~L151-166): read from the three narrow columns directly
  instead of `json_path_match` over `_raw`

### Step 4: Golden comparison test

Write a test (pytest) that:
1. Loads a sample fixture (small parquet or CSV with known TEC transactions)
2. Runs both the OLD path (`json_path_match` over `raw_data`) and the NEW path
   (reading `campaign_office_src` etc.)
3. Asserts the output values are identical

Place at `tests/test_campaign_rewire_golden.py`.

## Commit

```
feat: derive campaign source cols from ingest frame, rewire off raw_data (1a)
```

## Checklist

- [ ] `gitnexus_impact` run on all 5 symbols; no HIGH/CRITICAL
- [ ] Context7 consulted for Polars + SQLModel docs
- [ ] Three columns added to `UnifiedTransaction` (nullable, un-indexed, short String)
- [ ] `flat_txns.py` derives and populates the three columns during ingest
- [ ] `campaigns.py` `_transaction_frame()` selects new cols instead of `raw_data`
- [ ] `campaigns.py` `_office_expr()` reads new cols instead of `json_path_match`
- [ ] Golden comparison test passes
- [ ] `gitnexus_detect_changes()` run before commit
- [ ] Committed on branch `db-bloat/wave-1/task-1a-campaign-rewire`
