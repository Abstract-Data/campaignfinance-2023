# Model: claude-sonnet-4-6

# Task 4a: State-Scope full_address_lookup()

## Phase

Wave 4 — parallel with 4b (disjoint file ownership). Only after wave-3-complete.

## Branch

`db-bloat/wave-4/task-4a-address-lookup`

## Objective

`full_address_lookup()` in `common.py` currently does a `SELECT *` over the entire
`unified_addresses` table into memory. Add a `state_id` predicate and cache the
result once per run on `FamilyContext`.

## File Ownership

This task owns:
- `app/core/ingest_vectorized/common.py` — `full_address_lookup()` (~L228-277)

Do NOT touch `families/filer.py` or `finalize.py` (owned by 4b).

## Mandatory Pre-work

Run `gitnexus_impact` on `full_address_lookup` before editing.
**STOP and report if HIGH or CRITICAL.**

Use Context7 for current Polars and SQLModel docs.

## Implementation

### Step 1: Add state_id parameter

```python
def full_address_lookup(session: Session, state_id: int) -> pl.DataFrame:
    """Load address lookup for a single state.
    
    Scoped by state_id to avoid full-table scans across all states.
    """
```

### Step 2: Add state_id predicate to query

Filter by `state_id` in the SQL/ORM query so only addresses for the current
state are loaded into memory.

Use parameterized queries — never string interpolation.

### Step 3: Cache on FamilyContext

If `FamilyContext` exists, add a field or method to cache the result so dim
and detail families reuse it instead of re-calling the function:

```python
# FamilyContext pseudo-example
class FamilyContext:
    _address_lookup: Optional[pl.DataFrame] = None
    
    def get_address_lookup(self, session: Session, state_id: int) -> pl.DataFrame:
        if self._address_lookup is None:
            self._address_lookup = full_address_lookup(session, state_id)
        return self._address_lookup
```

### Step 4: Update all call sites

Find all callers of `full_address_lookup()` and update them to pass `state_id`.

## Output Parity Test

Write or update a test that confirms the same FK assignments are produced
before and after the state_id scoping on a sample state.

## Commit

```
perf: state-scope full_address_lookup + cache on FamilyContext (4a)
```

## Checklist

- [ ] Wave 3 `db-bloat/wave-3-complete` tag confirmed
- [ ] `gitnexus_impact` run on `full_address_lookup`; no HIGH/CRITICAL
- [ ] Context7 consulted for Polars + SQLModel docs
- [ ] `state_id` parameter added to `full_address_lookup()`
- [ ] SQL query uses parameterized `WHERE state_id = ?`
- [ ] Result cached on `FamilyContext` (or equivalent)
- [ ] All call sites updated
- [ ] FK parity test passes (same assignments before/after)
- [ ] `gitnexus_detect_changes()` run before commit
