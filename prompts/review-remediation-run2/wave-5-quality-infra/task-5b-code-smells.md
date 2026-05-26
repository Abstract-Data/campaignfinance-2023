# Task 5b — Code Smell Fixes: Fuzzy Match Logging, `format_zipcode`, `StopIteration`, Double Lookup

**Wave:** 5 — Quality & Infrastructure  
**Branch:** `remediation-r3/wave-5/task-5b-code-smells`  
**Effort:** ~2 hours  
**Parallel with:** 5a, 5c, 5d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-SMELL-003 | `_get_field_value` fuzzy match fires silently — no logging, no strict-mode flag | P2 High |
| RF-CPLX-003 | `format_zipcode` 7-branch if/elif chain with a logically dead branch | P3 |
| RF-SMELL-005 | Double `_resolve_state_record` call in same method | P3 |
| RF-SMELL-006 | `add_all` catches `StopIteration` from a for-loop — never raised in Python 3 | P3 |

---

## Fix 1: Log Fuzzy Match in `_get_field_value`

### Context

`app/core/builders.py:267-293` — three-level lookup chain:
1. Direct key lookup
2. State field mapping lookup
3. Fuzzy word-overlap match (`overlap >= 2 words`)

The fuzzy match can silently return wrong field values for short field names. When it fires, nothing is logged — making field-resolution failures invisible during new-state onboarding.

### Changes

```python
def _get_field_value(self, record: dict, field_name: str, state: str | None = None) -> str | None:
    # Level 1: direct
    if field_name in record:
        return record[field_name]

    # Level 2: state field mapping
    if state:
        mapped = self._state_field_mapping(record, field_name, state)
        if mapped is not None:
            return mapped

    # Level 3: fuzzy word-overlap — log when this fires
    fuzzy_result = self._fuzzy_field_match(record, field_name)
    if fuzzy_result is not None:
        logger.debug(
            "Field '%s' resolved via fuzzy match for state '%s' — "
            "consider adding an explicit mapping to UnifiedFieldLibrary",
            field_name, state,
        )
        return fuzzy_result

    return None
```

**Also add a `strict_field_resolution` flag** to `UnifiedSQLModelBuilder.__init__`:
```python
def __init__(self, ..., strict_field_resolution: bool = False):
    self.strict_field_resolution = strict_field_resolution
```

When `strict_field_resolution=True`, raise `ValueError` instead of using the fuzzy path:
```python
    if self.strict_field_resolution:
        raise ValueError(
            f"No explicit mapping found for field '{field_name}' in state '{state}'. "
            "Add a mapping to UnifiedFieldLibrary."
        )
    fuzzy_result = self._fuzzy_field_match(record, field_name)
```

This flag is useful in tests and new-state onboarding — set it to confirm all field mappings are explicit.

---

## Fix 2: Simplify `format_zipcode` 7-Branch Chain

### Context

`app/funcs/validator_functions.py:131-166`:
```python
if len(v) == 5:
    return v
elif len(v) == 4:
    return "0" + v
elif len(v) == 9:
    return v[:5] + "-" + v[5:]
elif len(v) == 10:  # already formatted 12345-6789
    return v
elif len(v) > 5:   # ← logically dead: if len > 5, the == 9 and == 10 branches above matched
    return v[:5]
...
```

The `len > 5` branch is logically dead because all lengths above 5 are matched by `== 9` or `== 10` first.

### Changes

```python
def format_zipcode(v: str) -> str:
    """Normalise a zipcode to 5 or 9+4 format."""
    if not v:
        return v
    digits = re.sub(r"\D", "", v)   # strip non-digits first
    if len(digits) == 5:
        return digits
    if len(digits) == 4:
        return "0" + digits          # leading zero
    if len(digits) == 9:
        return digits[:5] + "-" + digits[5:]
    if len(digits) == 10 and "-" in v:
        return v                     # already 12345-6789
    # Truncate to 5 for any other length
    return digits[:5]
```

Read the existing implementation carefully before rewriting — preserve any edge-case handling that is actually tested. Run the existing tests after to confirm behaviour is unchanged.

**DoD:**
```bash
grep -n "len(v)" app/funcs/validator_functions.py | wc -l
# Should be fewer branches than before
uv run pytest tests/ -q -k "zipcode or format_zip"
```

---

## Fix 3: Double `_resolve_state_record` (if not already fixed in 4a)

### Context

`app/core/unified_state_loader.py:L368 and L394` — `_resolve_state_record` called twice in the same method body. If task-4a fixed the batch-level double lookup via `_load_batch_indexes`, check whether this specific method-level double call is a different occurrence. If it remains, apply the same fix: extract `state_record` once and reuse.

```bash
grep -n "_resolve_state_record" app/core/unified_state_loader.py
# Should be 1 occurrence (inside _load_batch_indexes only)
```

If more than 1 occurrence, deduplicate.

---

## Fix 4: `StopIteration` in `db_loader.py`

### Context

`app/funcs/db_loader.py:76-82`:
```python
try:
    for item in data:
        session.add(item)
except StopIteration:
    pass
```

`StopIteration` is **never raised inside a for-loop** in Python 3 — it becomes `RuntimeError` when propagated out of a generator's `__next__`. This `except` block silently swallows actual errors that happen to be `StopIteration` subclasses, giving false confidence.

### Changes

```python
# Remove the try/except entirely — for-loops don't raise StopIteration
for item in data:
    session.add(item)
```

If the intent was to catch exhausted generators, that is not necessary — a for-loop handles it automatically.

**If the original code had a different intent** (e.g., the `data` argument could be `None` or not iterable), convert to an explicit check:
```python
if data is None:
    return
for item in data:
    session.add(item)
```

---

## Verification Checklist

```bash
# 1. Fuzzy match logs at DEBUG level
grep -n "logger.debug\|DEBUG" app/core/builders.py | grep -i "fuzzy\|explicit" \
  && echo "PASS" || echo "FAIL"

# 2. strict_field_resolution flag exists
grep -n "strict_field_resolution" app/core/builders.py && echo "PASS" || echo "FAIL"

# 3. format_zipcode dead branch gone
python -c "
import ast
src = open('app/funcs/validator_functions.py').read()
# Simple heuristic: count elif branches in format_zipcode
count = src.count('elif len(') 
print('elif len() branches:', count, '— target: ≤ 3')
"

# 4. No StopIteration catch in for-loops
grep -n "StopIteration" app/funcs/db_loader.py && echo "FAIL" || echo "PASS"

# 5. Tests pass
uv run pytest tests/ -q -k "zipcode or loader or builder or field_value"

# 6. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(builders,funcs): code smell cleanup — fuzzy logging, zipcode, StopIteration

- builders.py: log DEBUG when fuzzy field-match fires; add strict_field_resolution
  flag that raises ValueError instead of fuzzy matching (useful for tests)
- validator_functions.py: simplify format_zipcode — remove logically dead
  len > 5 branch; normalize via re.sub first
- db_loader.py: remove StopIteration catch from for-loop (never raised in Python 3)
- unified_state_loader.py: confirm _resolve_state_record called once per method

Fixes RF-SMELL-003, RF-CPLX-003, RF-SMELL-005, RF-SMELL-006
```
