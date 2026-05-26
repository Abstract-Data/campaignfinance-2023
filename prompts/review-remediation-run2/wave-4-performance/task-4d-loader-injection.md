# Task 4d — Inject `db_manager` + Move State Officer Mappings to Field Library

**Wave:** 4 — Performance  
**Branch:** `remediation-r3/wave-4/task-4d-loader-injection`  
**Effort:** ~3 hours  
**Parallel with:** 4a, 4b, 4c

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-ARCH-001 | `UnifiedStateLoader` directly imports module-global `db_manager` — prevents test injection | P2 High |
| P3-QUAL-004 / RF-SMELL-002 | Officer field mappings hardcoded dict literal in `_extract_officer_from_record` — adding Ohio requires editing this method | P3 |

---

## Fix 1: Inject `db_manager` into `UnifiedStateLoader`

### Context

`app/core/unified_state_loader.py:L555-581`:
```python
with db_manager.get_session() as session:
    ...
    db_manager.add_person_to_committee(...)
```

The loader accesses the module-level `db_manager` sentinel by importing it directly. This creates tight coupling — tests cannot inject a stub or in-memory manager.

### Changes

**In `UnifiedStateLoader.__init__`:**
```python
from app.core.unified_database import UnifiedDatabaseManager, get_db_manager

class UnifiedStateLoader:
    def __init__(
        self,
        state: str,
        db_manager: UnifiedDatabaseManager | None = None,
        *,
        field_library=None,
    ):
        self.db_manager = db_manager or get_db_manager()
        # ... rest of init
```

**Replace all references to the module-global `db_manager` inside the class:**
```bash
grep -n "\bdb_manager\b" app/core/unified_state_loader.py
```

For each `db_manager.xxx(...)` inside the class body, replace with `self.db_manager.xxx(...)`.

**Remove the module-level import** of the sentinel `db_manager` if it is only used inside `UnifiedStateLoader`. (The sentinel itself stays in `unified_database.py` for backward compat elsewhere.)

### Test update

```python
# Before: could not inject — test reached for real DB
loader = UnifiedStateLoader("texas")

# After: injectable
from unittest.mock import MagicMock
mock_manager = MagicMock()
loader = UnifiedStateLoader("texas", db_manager=mock_manager)
```

Add at least one test that constructs `UnifiedStateLoader` with a mock `db_manager` and verifies `_create_committee_relationships` calls `self.db_manager.add_person_to_committee(...)`.

---

## Fix 2: Move State Officer Field Mappings to `UnifiedFieldLibrary`

### Context

`app/core/unified_state_loader.py:L155-178` (approximately):
```python
state_mappings = {
    'texas': {
        'treasurer_name': ['treasurer_name', 'treasurer', ...],
        ...
    },
    'oklahoma': { ... },
}
```

Adding Ohio (or any other state) requires editing this method — a classic Open/Closed violation. The field library already has a `StateFieldMapping` abstraction that should own this data.

### Changes

**Step 1: Identify the existing `StateFieldMapping` structure:**
```bash
grep -n "StateFieldMapping\|officer_field\|FieldCategory" app/core/unified_field_library.py | head -20
```

**Step 2: Add an officer-fields registry to `UnifiedFieldLibrary`:**

```python
# In app/core/unified_field_library.py

# Add officer field declarations per state
_OFFICER_FIELD_REGISTRY: dict[str, dict[str, list[str]]] = {
    "texas": {
        "treasurer_name": ["treasurer_name", "treasurer", ...],
        "chair_name": ["chair_name", "chairperson", ...],
        # ... remaining TX officer fields
    },
    "oklahoma": {
        "treasurer_name": [...],
        # ... remaining OK officer fields
    },
}

class UnifiedFieldLibrary:
    ...
    def get_officer_fields(self, state: str) -> dict[str, list[str]]:
        """Return officer field name mappings for the given state."""
        return _OFFICER_FIELD_REGISTRY.get(state.lower(), {})
```

**Step 3: Update `_extract_officer_from_record` in the loader:**
```python
def _extract_officer_from_record(self, record: dict, state: str) -> dict:
    # Was: state_mappings = {...}
    # Now:
    officer_fields = self.field_library.get_officer_fields(state)
    ...
```

**Verify the method now delegates to field_library:**
```bash
grep -n "state_mappings\s*=" app/core/unified_state_loader.py && echo "FAIL" || echo "PASS"
```

**DoD:** Adding a new state's officer fields requires only editing `_OFFICER_FIELD_REGISTRY` in `unified_field_library.py` — no changes to `unified_state_loader.py`.

---

## Verification Checklist

```bash
# 1. No module-global db_manager import in loader (only self.db_manager usage)
grep -n "^from app.core.unified_database import.*\bdb_manager\b" \
  app/core/unified_state_loader.py && echo "WARN: check if still needed" || echo "PASS"

# 2. db_manager injection works
python -c "
from unittest.mock import MagicMock
from app.core.unified_state_loader import UnifiedStateLoader
mock = MagicMock()
loader = UnifiedStateLoader('texas', db_manager=mock)
print('Injection OK:', loader.db_manager is mock)
"

# 3. No inline state_mappings dict in loader
grep -n "state_mappings\s*=" app/core/unified_state_loader.py && echo "FAIL" || echo "PASS"

# 4. get_officer_fields exists in field library
python -c "
from app.core.unified_field_library import UnifiedFieldLibrary
lib = UnifiedFieldLibrary()
fields = lib.get_officer_fields('texas')
assert isinstance(fields, dict), 'FAIL'
print('PASS — TX officer fields:', list(fields.keys())[:3])
"

# 5. Tests pass
uv run pytest tests/ -q -k "loader or officer or field_library"

# 6. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(loader): inject db_manager and move officer fields to field library

UnifiedStateLoader previously imported the module-global db_manager sentinel
directly, preventing test injection. Now accepts db_manager via __init__
(defaults to get_db_manager()) and uses self.db_manager throughout.

Officer field name mappings hardcoded in _extract_officer_from_record as a
dict literal are moved to UnifiedFieldLibrary.get_officer_fields(state).
Adding a new state now requires only editing the field library registry.

Fixes RF-ARCH-001, P3-QUAL-004, RF-SMELL-002
```
