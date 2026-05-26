# Task 4z — Wave 4 Integration

**Wave:** 4 — Performance (serial integration)  
**Branch:** `remediation-r3/wave-4/integration`  
**Depends on:** All of 4a, 4b, 4c, 4d completed and passing  
**Effort:** ~30 minutes

---

## Steps

### 1. Merge all Wave 4 branches

```bash
git checkout main
git merge remediation-r3/wave-4/task-4a-address-cache-double-lookup
git merge remediation-r3/wave-4/task-4b-commit-money-type
git merge remediation-r3/wave-4/task-4c-load-context-dataclass
git merge remediation-r3/wave-4/task-4d-loader-injection
```

**Watch for conflicts:** 4a and 4c both touch `UnifiedStateLoader` — merge 4c first, then 4a on top (4a adds `address_cache` to `LoadContext`, which 4c defines).

### 2. Sweep All Wave 4 DoD Checks

```bash
# 4a: address_cache exists, only one _resolve_state_record call
grep -n "address_cache" app/core/unified_state_loader.py | wc -l  # ≥ 2
grep -n "_resolve_state_record" app/core/unified_state_loader.py | wc -l  # should be 1

# 4b: no session.commit() inside officer link methods
grep -A 30 "def _create_officer_link" app/core/unified_state_loader.py | \
  grep "session.commit()" && echo "FAIL" || echo "PASS"

# 4b: no inline Numeric(15, 2)
grep -n "Numeric(15, 2)" app/core/models/tables.py && echo "FAIL" || echo "PASS"

# 4c: LoadContext exists
python -c "from app.core.load_context import LoadContext, LoadStats; print('PASS')"

# 4c: no self.person_cache in loader __init__
grep -n "self\.person_cache\s*=\|self\.committee_cache\s*=\|self\.stats\s*=" \
  app/core/unified_state_loader.py | grep -v "ctx\." && echo "FAIL" || echo "PASS"

# 4d: injection works
python -c "
from unittest.mock import MagicMock
from app.core.unified_state_loader import UnifiedStateLoader
mock = MagicMock()
loader = UnifiedStateLoader('texas', db_manager=mock)
print('PASS' if loader.db_manager is mock else 'FAIL')
"

# 4d: officer fields in field library
python -c "
from app.core.unified_field_library import UnifiedFieldLibrary
lib = UnifiedFieldLibrary()
assert lib.get_officer_fields('texas'), 'FAIL'
print('PASS')
"
```

### 3. Full Test Suite

```bash
uv run pytest tests/ -q --tb=short
```

### 4. Tag

```bash
git tag remediation-r3/wave-4-complete
git push origin remediation-r3/wave-4-complete
```

---

## Expected State After Integration

- Address N+1: in-memory `address_cache` prevents per-record DB round-trips
- State lookup: one `_resolve_state_record` call per batch, not two
- Officer links: `session.flush()` instead of `session.commit()` inside loops
- `MONEY_TYPE` used for all 13 monetary columns in `tables.py`
- `LoadContext` dataclass owns all mutable run state; loader is stateless
- `UnifiedStateLoader` accepts injected `db_manager`; tests can use MagicMock
- State officer field mappings live in `UnifiedFieldLibrary.get_officer_fields(state)`
- All tests passing
