# Task 4b — Eliminate Commit-Per-Officer-Link + Fix `MONEY_TYPE` Bypasses

**Wave:** 4 — Performance  
**Branch:** `remediation-r3/wave-4/task-4b-commit-money-type`  
**Effort:** ~1 hour  
**Parallel with:** 4a, 4c, 4d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P2-PERF-003 | Commit-per-officer-link inside batch session — N×M round-trips | P2 High |
| P2-QUAL-001 | `MONEY_TYPE` constant bypassed by 12 inline `Numeric(15, 2)` literals | P2 High |

---

## Fix 1: Commit-Per-Officer-Link

### Context

`unified_state_loader.py:L490-543` — `_create_officer_link` and `_link_transaction_to_officers` call `session.commit()` inside loops. For N officers and M transactions, this is N×M individual commits to Postgres. The batch session already has a top-level commit at the end of `process_records_batch` — the inner commits are redundant and expensive.

### Changes

**In `_create_officer_link`:**
```python
# Remove:
session.commit()
session.refresh(link)

# Replace with (if refresh is needed):
session.flush()  # assigns the id without committing
# OR just session.add(link) and let the batch commit handle it
```

**In `_link_transaction_to_officers`:**
```python
# Remove any session.commit() calls inside the loop body
# Ensure the enclosing batch session will commit after the loop
```

**Verify the enclosing batch session commit is still in place:**
```bash
grep -n "session.commit()" app/core/unified_state_loader.py
# The only commit should be at the end of process_records_batch
# (or _persist_records_batch or whatever the batch-level method is named)
```

**If the officer link requires an ID before continuing (e.g., the link is referenced by another insert in the same batch), use `session.flush()` instead of `session.commit()`.** Flush assigns the ID without closing the transaction.

### DoD

```bash
# Count remaining session.commit() calls in the officer link methods
grep -A 30 "def _create_officer_link" app/core/unified_state_loader.py | grep "session.commit()" \
  && echo "FAIL" || echo "PASS"
grep -A 30 "def _link_transaction_to_officers" app/core/unified_state_loader.py | grep "session.commit()" \
  && echo "FAIL" || echo "PASS"
```

---

## Fix 2: Use `MONEY_TYPE` Constant Everywhere

### Context

`app/core/constants.py:7`:
```python
MONEY_TYPE = Numeric(15, 2)
```

`app/core/models/tables.py` imports `MONEY_TYPE` but 12 of 13 monetary `sa_column` definitions use the inline literal `Numeric(15, 2)` instead. A future precision change would require 12 manual edits and would be silently missed.

### Changes

**Find all offending lines:**
```bash
grep -n "Numeric(15, 2)" app/core/models/tables.py
```
Expected: ~12 matches.

**Replace each `Column(Numeric(15, 2), ...)` with `Column(MONEY_TYPE, ...)`:**
```bash
# Verify MONEY_TYPE is already imported in tables.py
grep -n "MONEY_TYPE" app/core/models/tables.py | head -5

# If not imported, add to the imports:
# from app.core.constants import MONEY_TYPE
```

**Automated replacement:**
```bash
# Careful: use sed only if the pattern is exactly Numeric(15, 2) with no variants
sed -i 's/Numeric(15, 2)/MONEY_TYPE/g' app/core/models/tables.py

# Verify result
grep -n "Numeric(15, 2)" app/core/models/tables.py && echo "FAIL" || echo "PASS"
grep -n "MONEY_TYPE" app/core/models/tables.py | wc -l  # should be 13+
```

**After the replacement, run the full test suite** — SQLAlchemy resolves `MONEY_TYPE` at column definition time, so this should be a no-op at runtime but tests will confirm.

---

## Verification Checklist

```bash
# 1. No session.commit() inside officer link loop methods
grep -A 30 "def _create_officer_link" app/core/unified_state_loader.py | \
  grep "session.commit()" && echo "FAIL" || echo "PASS"

# 2. No inline Numeric(15, 2) in tables.py
grep -n "Numeric(15, 2)" app/core/models/tables.py && echo "FAIL" || echo "PASS"

# 3. MONEY_TYPE used in tables.py
grep -c "MONEY_TYPE" app/core/models/tables.py  # should be ≥ 13

# 4. Tests pass
uv run pytest tests/ -q
```

---

## Commit Message

```
perf/refactor(loader,models): remove commit-per-link and use MONEY_TYPE everywhere

Commit-per-officer-link issued N×M individual commits per batch. Replaced
session.commit() inside loop bodies with session.flush() where needed, deferring
final commit to the batch-level session boundary.

Replaced 12 inline Numeric(15, 2) literals in tables.py with the MONEY_TYPE
constant from app/core/constants.py. A single constant change now governs all
monetary column precision.

Fixes P2-PERF-003, P2-QUAL-001
```
