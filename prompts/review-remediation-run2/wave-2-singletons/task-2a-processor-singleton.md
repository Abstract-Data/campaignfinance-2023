# Task 2a — Fix Global `unified_sql_processor` Mutable Singleton

**Wave:** 2 — Singletons & Coupling  
**Branch:** `remediation-r3/wave-2/task-2a-processor-singleton`  
**Effort:** ~45 minutes  
**Parallel with:** 2b, 2c

---

## Finding Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P1-ARCH-001 | Global `unified_sql_processor` singleton accumulates mutable session state | P1 Critical |

---

## Context

`app/core/processor.py` exports a module-level singleton:
```python
unified_sql_processor = UnifiedSQLDataProcessor()
```

Inside `UnifiedSQLDataProcessor`, `self.builders` is a dict keyed by state string. The `get_builder` method **mutates** the cached builder in place on every call:
```python
def get_builder(self, state, state_id=None, state_code=None, *, session=None):
    if state not in self.builders:
        self.builders[state] = UnifiedSQLModelBuilder(state)
    builder = self.builders[state]
    builder.state_id = state_id      # ← mutates cached instance
    builder.state_code = state_code  # ← mutates cached instance
    builder.session = session        # ← mutates cached instance
    return builder
```

Two consequences:
1. **Test isolation hazard:** Sequential tests in the same process that call `unified_sql_processor.get_builder("texas", ...)` share one `UnifiedSQLModelBuilder` instance. State from test N leaks into test N+1.
2. **Latent race condition:** Under concurrent access (e.g., if async or threading is added later), two callers can interleave their mutations on the same builder object.

The fix is straightforward: construct a new `UnifiedSQLModelBuilder` per call. Builders are cheap to create — they only read from `field_library`, which is already instantiated.

---

## Changes Required

### `app/core/processor.py`

**Current `get_builder` method (~L328-348):**
```python
def get_builder(
    self,
    state: str,
    state_id: int | None = None,
    state_code: str | None = None,
    *,
    session: Session | None = None,
) -> UnifiedSQLModelBuilder:
    if state not in self.builders:
        self.builders[state] = UnifiedSQLModelBuilder(state)
    builder = self.builders[state]
    builder.state_id = state_id
    builder.state_code = state_code
    builder.session = session
    return builder
```

**Required:**
```python
def get_builder(
    self,
    state: str,
    state_id: int | None = None,
    state_code: str | None = None,
    *,
    session: Session | None = None,
) -> UnifiedSQLModelBuilder:
    """Return a fresh builder per call.

    Builders are lightweight (read-only reference to field_library).
    Caching and mutating a shared builder is a test-isolation hazard and
    a latent race condition — eliminated by construction per call.
    """
    return UnifiedSQLModelBuilder(
        state,
        state_id=state_id,
        state_code=state_code,
        session=session,
    )
```

**Also remove:**
- The `self.builders: dict[str, UnifiedSQLModelBuilder] = {}` attribute from `__init__`
- Any `builders` property or accessor that exposed the dict

**Verify `UnifiedSQLModelBuilder.__init__` accepts these kwargs:**
```bash
grep -n "def __init__" app/core/builders.py | head -5
```
If the constructor signature differs, adjust the call accordingly — do not add parameters that don't exist.

### Test assertion

Add or update a test in `tests/test_processor.py`:

```python
def test_get_builder_returns_new_instance_per_call(processor):
    """get_builder must not cache mutable builder state between calls."""
    b1 = processor.get_builder("texas", state_id=1, state_code="TX")
    b2 = processor.get_builder("texas", state_id=2, state_code="TX")
    assert b1 is not b2, "get_builder should return a new instance each call"
    assert b1.state_id != b2.state_id or b2.state_id == 2
```

---

## Verification Checklist

```bash
# 1. No self.builders dict in processor.py
grep -n "self\.builders" app/core/processor.py && echo "FAIL" || echo "PASS"

# 2. get_builder returns UnifiedSQLModelBuilder directly
grep -A 10 "def get_builder" app/core/processor.py | grep "return UnifiedSQLModelBuilder" \
  && echo "PASS" || echo "FAIL"

# 3. Tests pass
uv run pytest tests/test_processor.py -v

# 4. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
fix(processor): eliminate mutable singleton — get_builder returns fresh instance

The cached self.builders dict mutated shared UnifiedSQLModelBuilder
instances across calls, creating a test-isolation hazard and a latent
race condition. Builders are cheap to construct (read-only field_library
reference), so construct one per call instead.

Fixes P1-ARCH-001
```
