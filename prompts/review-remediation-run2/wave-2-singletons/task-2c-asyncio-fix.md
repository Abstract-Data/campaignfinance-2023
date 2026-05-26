# Task 2c — Fix `asyncio.run()` in `OnePasswordItem.__init__`

**Wave:** 2 — Singletons & Coupling  
**Branch:** `remediation-r3/wave-2/task-2c-asyncio-fix`  
**Effort:** ~45 minutes  
**Parallel with:** 2a, 2b

---

## Finding Addressed

| ID | Finding | Severity |
|----|---------|----------|
| R4 | `asyncio.run()` called inside `OnePasswordItem.__init__` — raises `RuntimeError` if already in an async context | P2 High |

---

## Context

`app/op.py:L82` (approximately):
```python
class OnePasswordItem:
    def __init__(self, ...):
        ...
        self._secrets = asyncio.run(self._fetch_secrets())
```

`asyncio.run()` creates and runs a **new event loop**. If this is called from inside an already-running event loop (e.g., an async CLI command, a FastAPI route, or a pytest-asyncio test), it raises:
```
RuntimeError: This event loop is already running.
```

This is a known Python async anti-pattern. The developer assessment flagged it as an AI-generation artifact — the code looks correct in isolation but fails in any async context.

---

## Recommended Approach: Async Factory Pattern

Convert `OnePasswordItem` to use an async class method for construction. The `__init__` should not call async code.

### Pattern A — Async Factory (preferred)

```python
class OnePasswordItem:
    def __init__(self, vault: str, item_name: str):
        self.vault = vault
        self.item_name = item_name
        self._secrets: dict[str, str] = {}
        # No async work here

    @classmethod
    async def create(cls, vault: str, item_name: str) -> "OnePasswordItem":
        """Async factory — use this instead of __init__ in async contexts."""
        instance = cls(vault, item_name)
        instance._secrets = await instance._fetch_secrets()
        return instance

    @classmethod
    def create_sync(cls, vault: str, item_name: str) -> "OnePasswordItem":
        """Sync factory — use this from synchronous contexts only."""
        return asyncio.run(cls.create(vault, item_name))
```

**Then update all call sites:**
```bash
grep -rn "OnePasswordItem(" app/ scripts/
```

- Sync call sites (CLI entrypoints, scheduler): use `OnePasswordItem.create_sync(...)`
- Async call sites (if any): use `await OnePasswordItem.create(...)`
- If `asyncio.run(...)` appears at a call site that was wrapping the `__init__`, simplify to `OnePasswordItem.create_sync(...)`

### Pattern B — Lazy initialization (acceptable alternative)

If the factory pattern is too disruptive to call sites, use lazy initialization:

```python
class OnePasswordItem:
    def __init__(self, vault: str, item_name: str):
        self.vault = vault
        self.item_name = item_name
        self._secrets: dict[str, str] | None = None

    def _ensure_secrets(self) -> None:
        if self._secrets is None:
            self._secrets = asyncio.run(self._fetch_secrets())

    def get_secret(self, key: str) -> str:
        self._ensure_secrets()
        return self._secrets[key]
```

This defers `asyncio.run()` to first access rather than `__init__`, which is marginally better but still crashes in an async context. **Use Pattern A if at all possible.**

---

## Files to Read First

```bash
cat app/op.py
grep -rn "OnePasswordItem\|OnePasswordSettings" app/ scripts/ | head -30
```

Understand the full call graph before changing the interface.

---

## Verification Checklist

```bash
# 1. No asyncio.run() inside __init__ of any class in op.py
grep -n "asyncio\.run" app/op.py
# Should not appear inside a class __init__ body

# 2. Sync factory works
python -c "
# This should not raise RuntimeError even if mocked
from unittest.mock import patch, AsyncMock
import asyncio
from app.op import OnePasswordItem
# Just verifying import works and __init__ is safe
print('Import OK')
"

# 3. pytest-asyncio test can import op without hanging
uv run pytest tests/ -q -k "op or password or secrets"

# 4. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
fix(op): replace asyncio.run() in __init__ with async factory pattern

asyncio.run() inside __init__ raises RuntimeError when called from an
already-running event loop (async CLI, pytest-asyncio, FastAPI).

Introduces:
  OnePasswordItem.create(vault, item) → async factory
  OnePasswordItem.create_sync(vault, item) → sync wrapper for CLI/scheduler

Updates all call sites to use the appropriate factory method.

Fixes R4
```
