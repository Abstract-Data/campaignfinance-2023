# Task 3z — Wave 3 Integration

**Wave:** 3 — God Class (serial integration)  
**Branch:** `remediation-r3/wave-3/integration`  
**Depends on:** 3a, 3b, 3c completed  
**Effort:** ~30 minutes

---

## Steps

### 1. Merge all Wave 3 branches

```bash
git checkout main
git merge remediation-r3/wave-3/task-3a-generic-update-versions
git merge remediation-r3/wave-3/task-3b-analytics-sql-aggregates
git merge remediation-r3/wave-3/task-3c-split-database-manager
```

### 2. Sweep All Wave 3 DoD Checks

```bash
# 3a: generic helpers exist
grep -n "def _update_entity\|def _get_versions" app/core/unified_database.py \
  && echo "PASS" || echo "FAIL: helpers missing"

# 3a: net reduction from cloned methods
wc -l app/core/unified_database.py

# 3b: no full-table .all() in analytics
grep -n "\.all()" app/core/analytics.py 2>/dev/null && echo "FAIL" || echo "PASS"

# 3b: SQL aggregates present
grep -c "func\.sum\|func\.count\|func\.avg" app/core/analytics.py 2>/dev/null

# 3c: unified_database.py under 300 lines
wc -l app/core/unified_database.py | awk '{if($1 < 300) print "PASS"; else print "FAIL: " $1 " lines"}'

# 3c: all four files present
ls app/core/repository.py app/core/officer_repository.py app/core/analytics.py \
  && echo "PASS" || echo "FAIL"

# 3c: no broken imports
python -c "
from app.core.unified_database import UnifiedDatabaseManager, get_db_manager
from app.core.repository import UnifiedVersionedRepository
from app.core.officer_repository import UnifiedOfficerRepository
from app.core.analytics import UnifiedAnalyticsService
print('All imports OK')
"
```

### 3. Full Test Suite

```bash
uv run pytest tests/ -q --tb=short
```

All tests must pass before tagging.

### 4. Tag

```bash
git tag remediation-r3/wave-3-complete
git push origin remediation-r3/wave-3-complete
```

---

## Expected State After Integration

- `unified_database.py` is < 300 lines — engine, session, bootstrap, backward-compat delegations only
- `repository.py` — all `update_*` and `get_*_versions` methods (collapsed to 2 generics + 10 one-liner delegations)
- `officer_repository.py` — committee-person relationship management
- `analytics.py` — analytics using SQL aggregates, no full-table scans
- All existing call sites continue to work via delegation wrappers
- All tests passing
