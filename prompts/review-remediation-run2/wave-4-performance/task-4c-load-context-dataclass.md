# Task 4c — `LoadContext` Dataclass: Make `UnifiedStateLoader` Stateless

**Wave:** 4 — Performance  
**Branch:** `remediation-r3/wave-4/task-4c-load-context-dataclass`  
**Effort:** ~1 day  
**Parallel with:** 4a, 4b, 4d

---

## Finding Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-SMELL-001 | `UnifiedStateLoader` holds 8 mutable dicts/lists as instance state across pipeline steps — non-reentrant, untestable in isolation | P2 High |

---

## Context

`app/core/unified_state_loader.py:L83-101`:
```python
self.stats = {"files_processed": 0, "transactions_created": 0, ...}
self.person_cache = {}
self.committee_cache = {}
self.address_cache = {}   # (will be added in task-4a)
self.committee_officers = {}
# ...more mutable dicts
```

These attributes are mutated across five private pipeline methods. The loader is non-reentrant: if `load_state(...)` is called twice on the same instance, the second call sees leftover state from the first. It also means you cannot test `_process_data_file` in isolation — you must construct a full loader instance with all the attendant state.

The fix: extract all pipeline state into a `LoadContext` dataclass that is **created fresh on each `load_state(...)` call** and passed explicitly through the pipeline. The loader itself becomes stateless (only configuration, not mutable run state).

---

## Changes Required

### Step 1: Define `LoadContext` in `app/core/load_context.py`

```python
"""Pipeline state for a single UnifiedStateLoader.load_state() run."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LoadStats:
    files_processed: int = 0
    transactions_created: int = 0
    transactions_failed: int = 0
    committees_created: int = 0
    persons_created: int = 0
    addresses_created: int = 0

    @property
    def total_attempted(self) -> int:
        return self.transactions_created + self.transactions_failed


@dataclass
class LoadContext:
    """All mutable state for one load_state() invocation.

    Create a fresh LoadContext at the start of each run.
    Pass it explicitly to each pipeline step.
    """
    state: str
    state_id: int | None = None
    state_code: str | None = None

    # Per-batch caches — populated by _load_batch_indexes
    person_cache: dict[int, object] = field(default_factory=dict)
    committee_cache: dict[int, object] = field(default_factory=dict)
    address_cache: dict[tuple[str, str, str, str], int] = field(default_factory=dict)
    committee_officers: dict[int, list] = field(default_factory=dict)

    # Discovered files
    data_files: list[Path] = field(default_factory=list)

    # Run statistics
    stats: LoadStats = field(default_factory=LoadStats)
```

Adapt field names to match the actual attribute names currently on the loader.

### Step 2: Update `UnifiedStateLoader.load_state()`

```python
def load_state(self, state: str, state_id: int | None = None) -> LoadContext:
    """Run the full ingestion pipeline for one state. Returns the load context."""
    ctx = LoadContext(state=state, state_id=state_id)
    ctx = self._discover_data_files(ctx)
    ctx = self._extract_committee_officers(ctx)
    ctx = self._process_data_file(ctx)
    ctx = self._create_committee_relationships(ctx)
    ctx = self._auto_link_all_transactions(ctx)
    return ctx
```

### Step 3: Update each private pipeline method signature

Each method that currently mutates `self.*` attributes must now accept and return `ctx: LoadContext`:

```python
def _discover_data_files(self, ctx: LoadContext) -> LoadContext:
    # was: self.data_files = [...]
    # now: ctx.data_files = [...]
    return ctx

def _extract_committee_officers(self, ctx: LoadContext) -> LoadContext:
    # was: self.committee_officers = {...}
    # now: ctx.committee_officers = {...}
    return ctx

# ... and so on for _process_data_file, _create_committee_relationships,
# _auto_link_all_transactions
```

### Step 4: Remove instance attributes that are now in LoadContext

After updating all methods, remove these from `__init__`:
```python
# DELETE from __init__:
self.stats = ...
self.person_cache = ...
self.committee_cache = ...
self.address_cache = ...
self.committee_officers = ...
```

**Keep in `__init__`:** configuration attributes (state, db_manager, field_library, etc.) that don't change per run.

### Step 5: Update tests

Any test that accesses `loader.person_cache` or `loader.stats` must be updated to inspect the returned `ctx`:
```python
# Before:
loader.load_state("texas")
assert loader.stats["transactions_created"] > 0

# After:
ctx = loader.load_state("texas")
assert ctx.stats.transactions_created > 0
```

---

## Verification Checklist

```bash
# 1. LoadContext dataclass exists
python -c "from app.core.load_context import LoadContext, LoadStats; print('OK')"

# 2. No self.person_cache / self.committee_cache in loader.__init__
grep -n "self\.person_cache\s*=\|self\.committee_cache\s*=\|self\.stats\s*=" \
  app/core/unified_state_loader.py | grep "__init__" && echo "FAIL" || echo "PASS"

# 3. Pipeline methods accept ctx
grep -n "def _discover_data_files\|def _extract_committee_officers\|def _process_data_file" \
  app/core/unified_state_loader.py

# 4. Tests pass
uv run pytest tests/ -q -k "loader or load_state"

# 5. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(loader): extract mutable run state into LoadContext dataclass

UnifiedStateLoader accumulated 8 mutable dicts/lists as instance attributes
shared across pipeline steps, making the loader non-reentrant and untestable
in isolation.

Introduces LoadContext dataclass (app/core/load_context.py) created fresh on
each load_state() call. All pipeline methods now accept and return ctx.
Loader __init__ retains only configuration (db_manager, field_library, etc.).

Fixes RF-SMELL-001
```
