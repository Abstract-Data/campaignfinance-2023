# Task 3b — Rewrite Analytics Methods with SQL Aggregates

**Wave:** 3 — God Class  
**Branch:** `remediation-r3/wave-3/task-3b-analytics-sql-aggregates`  
**Effort:** ~3-4 hours  
**Parallel with:** 3a, 3c

---

## Finding Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-CPLX-002 | `get_summary_statistics` and `get_cross_state_analysis` load ALL rows into Python memory | P1 Critical |

---

## Context

`app/core/unified_database.py:L391-522`:

```python
# L400-401 — loads EVERYTHING into Python
total_transactions = session.exec(
    select(UnifiedTransaction).options(*_transaction_analytics_options())
).all()
total_amount = sum(tx.amount for tx in total_transactions if tx.amount)
```

On a dataset of 100k+ rows this is an OOM risk. It also loads every transaction's related objects via `selectinload` — multiplying the memory footprint further.

**This task rewrites both methods to use SQL aggregates — `func.sum`, `func.count`, `func.avg`, `GROUP BY` — so the DB does the computation and only scalar results come back to Python.** The methods must produce the same output shape (same dict keys, same structure) so callers are unaffected.

---

## Changes Required

### Read the current methods first

```bash
sed -n '391,522p' app/core/unified_database.py
```

Understand exactly what keys each method returns before rewriting.

### Rewrite `get_summary_statistics`

**Pattern:**
```python
def get_summary_statistics(self) -> dict:
    with self.get_session() as session:
        # Total count and amount — one query
        total_count, total_amount = session.exec(
            select(
                func.count(UnifiedTransaction.id),
                func.coalesce(func.sum(UnifiedTransaction.amount), 0),
            )
        ).one()

        # Amount by transaction type — GROUP BY
        by_type = session.exec(
            select(
                UnifiedTransaction.transaction_type,
                func.count(UnifiedTransaction.id),
                func.coalesce(func.sum(UnifiedTransaction.amount), 0),
            ).group_by(UnifiedTransaction.transaction_type)
        ).all()

        # Average amount
        avg_amount = session.exec(
            select(func.avg(UnifiedTransaction.amount))
        ).one()

        # Committee count
        committee_count = session.exec(
            select(func.count(UnifiedCommittee.id))
        ).one()

        # Person count
        person_count = session.exec(
            select(func.count(UnifiedPerson.id))
        ).one()

        return {
            "total_transactions": total_count,
            "total_amount": float(total_amount or 0),
            "average_amount": float(avg_amount or 0),
            "committee_count": committee_count,
            "person_count": person_count,
            "by_transaction_type": {
                row[0]: {"count": row[1], "amount": float(row[2] or 0)}
                for row in by_type
            },
        }
```

Adjust the return dict keys to exactly match what the current method returns. Read the existing dict construction carefully.

### Rewrite `get_cross_state_analysis`

Same pattern — use `GROUP BY state_id` (or the appropriate FK to the state table) for per-state aggregates. Replace any Python-level `sum(tx.amount for tx in rows)` with `func.sum`, any `len(rows)` with `func.count`.

For top-N donors / top-N recipients: use `order_by(func.sum(...).desc()).limit(N)`.

```python
# Example top-5 donors by total amount
top_donors = session.exec(
    select(
        UnifiedPerson.full_name,
        func.sum(UnifiedTransaction.amount).label("total"),
    )
    .join(UnifiedTransaction, UnifiedTransaction.person_id == UnifiedPerson.id)
    .group_by(UnifiedPerson.id, UnifiedPerson.full_name)
    .order_by(func.sum(UnifiedTransaction.amount).desc())
    .limit(5)
).all()
```

### Create `app/core/analytics.py` (future home)

**This task does not move the methods** — Wave 3c handles the split. But create the file now as an empty module so Wave 3c can import from it:

```python
# app/core/analytics.py
"""
SQL-aggregate analytics — extracted from UnifiedDatabaseManager in Wave 3c.
All methods here must use SQL aggregates, never .all() full-table loads.
"""
```

---

## Verification Checklist

```bash
# 1. No .all() after select(UnifiedTransaction) in the analytics methods
grep -n "\.all()" app/core/unified_database.py | head -20
# The analytics methods (get_summary_statistics, get_cross_state_analysis)
# must not appear in this list

# 2. func.sum / func.count present
grep -n "func\.sum\|func\.count\|func\.avg" app/core/unified_database.py | \
  wc -l  # should be ≥ 6 (multiple aggregates per method)

# 3. Return shapes match — run a basic smoke test if DB is available
# (skip if no test DB configured)
python -c "
from app.core.unified_database import get_db_manager
m = get_db_manager()
stats = m.get_summary_statistics()
assert 'total_transactions' in stats
assert 'total_amount' in stats
print('Shape OK:', list(stats.keys()))
" 2>/dev/null || echo "(skipped — no DB)"

# 4. Tests pass
uv run pytest tests/ -q -k "analytics or statistics or summary"

# 5. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(analytics): replace full-table .all() scans with SQL aggregates

get_summary_statistics and get_cross_state_analysis previously loaded
all UnifiedTransaction rows into Python memory with selectinload. On
large datasets this causes OOM.

Rewrites both methods using func.sum, func.count, func.avg, GROUP BY.
DB computes aggregates; only scalar results returned to Python — O(1)
memory regardless of row count.

Creates app/core/analytics.py placeholder for Wave 3c extraction.

Fixes RF-CPLX-002
```
