# Model: claude-sonnet-4-6

# Task 1z: Wave 1 Linkage Integrity Verification

## Phase

Wave 1 — serial integration. Only start **after task 1b is merged**.

## Branch

`db-bloat/wave-1/task-1z-integration`

## Prerequisite

Tasks 1a and 1b must be merged. `unified_transactions.raw_data` must be dropped.

## Objective

Verify that dropping `raw_data` did not break any transaction linkage or introduce
duplication. Run and record linkage integrity checks.

## Checks to Run

### Linkage integrity

```sql
-- No orphaned transactions (must be 0)
SELECT COUNT(*) FROM unified_transactions WHERE committee_id IS NULL;

-- Transaction → report linkage
SELECT COUNT(*) FROM unified_transactions WHERE report_id IS NULL;

-- Transaction → person linkage (contributions should have contributor)
SELECT COUNT(*) 
FROM unified_transactions t
WHERE t.transaction_type = 'CONTRIBUTION'
  AND NOT EXISTS (
    SELECT 1 FROM unified_transaction_persons utp 
    WHERE utp.transaction_id = t.id
  );
```

### Row count verification (compare to pre-work baseline)

```sql
SELECT 
    (SELECT COUNT(*) FROM unified_persons) AS persons,
    (SELECT COUNT(*) FROM unified_addresses) AS addresses,
    (SELECT COUNT(*) FROM unified_entities) AS entities,
    (SELECT COUNT(*) FROM unified_transactions) AS transactions;
```

These should match the pre-work counts. Record them in `docs/db-bloat-baseline-2026-06-17.md`.

### Campaign derivation parity check

Verify that `campaign_office_src`, `campaign_district_src`, `campaign_name_src`
are populated for transactions that had campaign data:

```sql
SELECT COUNT(*) FROM unified_transactions 
WHERE campaign_name_src IS NOT NULL;

SELECT COUNT(*) FROM unified_transactions 
WHERE campaign_office_src IS NOT NULL;
```

## Deliverable

Write a pytest test file `tests/test_wave1_linkage.py` that:
1. Connects to the DB
2. Runs the linkage integrity SQL checks
3. Asserts orphan count = 0
4. Documents the row counts

## Commit

```
test: wave-1 linkage integrity verification (1z)
```

## Tag After Completion

```
db-bloat/wave-1-complete
```

## Checklist

- [ ] Tasks 1a and 1b confirmed merged
- [ ] Linkage SQL checks run — all pass
- [ ] Row counts match pre-work baseline
- [ ] `tests/test_wave1_linkage.py` created and passes
- [ ] `gitnexus_detect_changes()` run before commit
- [ ] Tagged `db-bloat/wave-1-complete`
