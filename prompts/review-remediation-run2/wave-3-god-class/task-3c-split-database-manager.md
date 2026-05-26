# Task 3c — Split `UnifiedDatabaseManager` God Class into Four Focused Classes

**Wave:** 3 — God Class  
**Branch:** `remediation-r3/wave-3/task-3c-split-database-manager`  
**Effort:** ~1 day  
**Parallel with:** 3a, 3b  
**Note:** Depends on 3a and 3b outputs conceptually, but can begin on a separate branch

---

## Finding Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-CPLX-001 | `UnifiedDatabaseManager` god class — 1,444 lines, 4 distinct domains | P1 Critical |

---

## Context

`app/core/unified_database.py` currently spans four clearly separate domains inside one 1,444-line class:

1. **Engine setup + session management** — `__init__`, `get_session`, `bootstrap`
2. **CRUD & versioning** — `update_*`, `get_*_versions` (just collapsed in 3a)
3. **Officer relationship management** — `add_person_to_committee`, `get_committee_officers`, `auto_link_transactions_to_committee_roles`, etc.
4. **Analytics** — `get_summary_statistics`, `get_cross_state_analysis` (just rewritten in 3b)

**This task extracts domains 2, 3, and 4 into separate files, leaving the core class lean.**

---

## Target Structure After This Task

```
app/core/
  unified_database.py        ← Thin: engine, session, bootstrap, factory (~150 lines)
  repository.py              ← UnifiedVersionedRepository: update_* / get_*_versions
  officer_repository.py      ← UnifiedOfficerRepository: committee-person management
  analytics.py               ← UnifiedAnalyticsService: SQL aggregate queries
```

**Backward-compat shim:** `unified_database.py` must re-export the class and factory function under the same names so existing importers don't break immediately.

---

## Changes Required

### Step 1: Read the current file structure

```bash
grep -n "def " app/core/unified_database.py | head -60
wc -l app/core/unified_database.py
```

Group methods by domain. Identify which domain each method belongs to.

### Step 2: Create `app/core/repository.py`

```python
"""
UnifiedVersionedRepository — CRUD with version snapshotting.
Extracted from UnifiedDatabaseManager in Wave 3c.
"""
from __future__ import annotations

from sqlmodel import Session, select, func

from app.core.models.tables import (
    UnifiedTransaction, UnifiedTransactionVersion,
    UnifiedPerson, UnifiedPersonVersion,
    UnifiedCommittee, UnifiedCommitteeVersion,
    UnifiedAddress, UnifiedAddressVersion,
    UnifiedCommitteePerson, UnifiedCommitteePersonVersion,
)
from app.core.unified_database import _record_version, _utc_now  # shared helpers


class UnifiedVersionedRepository:
    """CRUD + version snapshotting for all unified entities."""

    def __init__(self, get_session_fn):
        self._get_session = get_session_fn

    # Move _update_entity, _get_versions, and all update_* / get_*_versions here
    # (already collapsed in task-3a — just move the 10 methods + 2 helpers)
```

### Step 3: Create `app/core/officer_repository.py`

```python
"""
UnifiedOfficerRepository — committee-person relationship management.
Extracted from UnifiedDatabaseManager in Wave 3c.
"""
from __future__ import annotations

class UnifiedOfficerRepository:
    """Committee-person relationship management."""

    def __init__(self, get_session_fn):
        self._get_session = get_session_fn

    # Move: add_person_to_committee, remove_person_from_committee,
    # get_committee_officers, get_active_treasurers,
    # link_transaction_to_committee_role, auto_link_transactions_to_committee_roles,
    # get_officer_contributions, get_officer_expenditures,
    # get_person_committee_financial_summary
```

### Step 4: Populate `app/core/analytics.py`

```python
"""
UnifiedAnalyticsService — SQL aggregate analytics.
Extracted from UnifiedDatabaseManager in Wave 3c.
Already rewritten to use SQL aggregates in task-3b.
"""
from __future__ import annotations

class UnifiedAnalyticsService:
    """Analytics queries using SQL aggregates — no full-table scans."""

    def __init__(self, get_session_fn):
        self._get_session = get_session_fn

    # Move: get_summary_statistics, get_cross_state_analysis, export_to_json
    # (already using SQL aggregates after task-3b)
```

### Step 5: Slim down `UnifiedDatabaseManager`

After extracting methods, `UnifiedDatabaseManager` should contain only:
- `__init__` (engine setup, connection pool)
- `get_session()` context manager
- `bootstrap(drop_first: bool)` — `create_all` / `drop_all`
- Instance attributes pointing to the extracted classes:

```python
class UnifiedDatabaseManager:
    def __init__(self, config: PostgresConfig):
        ...
        self.repo = UnifiedVersionedRepository(self.get_session)
        self.officers = UnifiedOfficerRepository(self.get_session)
        self.analytics = UnifiedAnalyticsService(self.get_session)
```

**Backward-compat delegation:** For every method moved to a sub-class, add a delegation wrapper in `UnifiedDatabaseManager` with a deprecation comment:

```python
def update_transaction(self, *args, **kwargs):
    """Delegated to self.repo.update_transaction. Use self.repo directly."""
    return self.repo.update_transaction(*args, **kwargs)
```

This ensures all existing callers continue to work without modification during this wave.

### Step 6: Verify `unified_database.py` line count

```bash
wc -l app/core/unified_database.py
# Target: < 300 lines (core class + backward-compat delegations)
```

---

## Verification Checklist

```bash
# 1. unified_database.py is under 300 lines
wc -l app/core/unified_database.py | awk '{if($1 < 300) print "PASS"; else print "FAIL: " $1 " lines"}'

# 2. New files exist
ls app/core/repository.py app/core/officer_repository.py app/core/analytics.py

# 3. No broken imports
python -c "
from app.core.unified_database import UnifiedDatabaseManager, get_db_manager
from app.core.repository import UnifiedVersionedRepository
from app.core.officer_repository import UnifiedOfficerRepository
from app.core.analytics import UnifiedAnalyticsService
print('All imports OK')
"

# 4. Backward compat: existing call sites still work via delegation
python -c "
from app.core.unified_database import UnifiedDatabaseManager
m = UnifiedDatabaseManager.__new__(UnifiedDatabaseManager)
assert hasattr(m.__class__, 'update_transaction'), 'delegation wrapper missing'
print('PASS')
"

# 5. Tests pass
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(db): split UnifiedDatabaseManager god class into 4 focused classes

Extracts three distinct domains from unified_database.py (1,444 lines):
  - app/core/repository.py     UnifiedVersionedRepository (CRUD + versioning)
  - app/core/officer_repository.py  UnifiedOfficerRepository (committee-person mgmt)
  - app/core/analytics.py      UnifiedAnalyticsService (SQL aggregate analytics)

UnifiedDatabaseManager retains: engine, get_session, bootstrap.
Backward-compat delegation wrappers added for all moved methods.

Target: unified_database.py < 300 lines.

Fixes RF-CPLX-001
```
