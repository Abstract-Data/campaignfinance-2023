# Model: gpt-5.3-codex-high-fast

# Task 2z: Wave 2 Report Ingest Smoke Test + Linkage Check

## Phase

Wave 2 — serial integration. Only start **after task 2b is merged**.

## Branch

`db-bloat/wave-2/task-2z-integration`

## Objective

Verify that dropping `unified_reports.raw_data` did not break report ingest or
committee/treasurer name tracking.

## Checks

### Report ingest smoke test

Run a small report ingest (or unit test) to verify the vectorized and ORM writers
still produce valid `UnifiedReport` rows with at-filing columns populated.

### Linkage checks

```sql
-- No reports with NULL committee_name_at_filing (should be 0 or low for new rows)
SELECT COUNT(*) FROM unified_reports WHERE committee_name_at_filing IS NULL;

-- No reports with NULL treasurer_name_at_filing
SELECT COUNT(*) FROM unified_reports WHERE treasurer_name_at_filing IS NULL;

-- Report → transaction linkage intact
SELECT COUNT(*) FROM unified_transactions WHERE report_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM unified_reports r WHERE r.id = unified_transactions.report_id
  );
```

### Column gone

```sql
-- Confirm raw_data column no longer exists
SELECT column_name FROM information_schema.columns
WHERE table_name = 'unified_reports' AND column_name = 'raw_data';
-- Must return 0 rows
```

## Deliverable

Write `tests/test_wave2_report_ingest.py` with the SQL checks as pytest assertions.

## Commit

```
test: wave-2 report ingest smoke + linkage verification (2z)
```

## Tag After Completion

```
db-bloat/wave-2-complete
```

## Checklist

- [ ] Task 2b confirmed merged
- [ ] Smoke test passes for report ingest
- [ ] Linkage SQL checks all pass
- [ ] `raw_data` column confirmed absent from `unified_reports`
- [ ] `tests/test_wave2_report_ingest.py` created and passes
- [ ] Tagged `db-bloat/wave-2-complete`
