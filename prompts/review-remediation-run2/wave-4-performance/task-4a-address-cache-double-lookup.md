# Task 4a — Address Cache + Eliminate Double State Lookup

**Wave:** 4 — Performance  
**Branch:** `remediation-r3/wave-4/task-4a-address-cache-double-lookup`  
**Effort:** ~2-3 hours  
**Parallel with:** 4b, 4c, 4d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P2-PERF-001 | Per-record address dedup query — N+1 pattern at scale | P2 High |
| P2-PERF-002 | Double state record lookup per batch — 2 queries for same row | P2 High |

---

## Context

### P2-PERF-001: Address N+1

`_persist_transaction_from_record` (`unified_state_loader.py:L451-461`) issues a 4-column equality SELECT against `unified_addresses` for every transaction that has a person address. At 1 million Texas records, this is ~1 million sequential DB round-trips.

The person cache (`self.person_cache`) and committee cache (`self.committee_cache`) already prove the correct pattern — an in-memory dict loaded once per batch. The address cache is missing.

The cache key is a tuple of the four fields used in `_find_address_by_fields`: `(street_1, city, state, zip_code)`.

### P2-PERF-002: Double State Lookup

`process_records_batch` in `unified_state_loader.py:L303-312`:
1. `_load_batch_indexes(...)` calls `db_manager._resolve_state_record(state_id)` — returns `CommitteeIndex, PersonIndex`
2. Immediately after, `process_records_batch` calls `db_manager._resolve_state_record(state_id)` again to extract `state_code`

This is two DB round-trips for the same row per batch.

---

## Changes Required

### Fix 1: Add `address_cache` to `UnifiedStateLoader`

**In `__init__` or wherever `person_cache` and `committee_cache` are initialized:**
```python
self.address_cache: dict[tuple[str, str, str, str], int] = {}
```

**In `_load_batch_indexes` (or equivalent batch initialization method), pre-load addresses from the DB for the current batch's geography:**
```python
# Pre-populate address_cache for addresses already in DB
# (Optional optimization — the cache will also build up as new addresses are inserted)
existing_addresses = session.exec(select(UnifiedAddress)).all()
for addr in existing_addresses:
    key = (addr.street_1 or "", addr.city or "", addr.state or "", addr.zip_code or "")
    self.address_cache[key] = addr.id
```

**In `build_address` / `_find_address_by_fields` (wherever the 4-column SELECT occurs):**
```python
cache_key = (
    parts.street_1 or "",
    parts.city or "",
    parts.state or "",
    parts.zip_code or "",
)
if cache_key in self.address_cache:
    return self.address_cache[cache_key]

# Cache miss — query DB
existing = session.exec(
    select(UnifiedAddress).where(
        UnifiedAddress.street_1 == parts.street_1,
        UnifiedAddress.city == parts.city,
        UnifiedAddress.state == parts.state,
        UnifiedAddress.zip_code == parts.zip_code,
    )
).first()
if existing:
    self.address_cache[cache_key] = existing.id
    return existing.id

# New address — insert
new_addr = UnifiedAddress(**parts.model_dump())
session.add(new_addr)
session.flush()  # get the id without committing
self.address_cache[cache_key] = new_addr.id
return new_addr.id
```

Read the actual call sites for `_find_address_by_fields` carefully — the above is a pattern, not a literal drop-in. Adapt field names to match the actual code.

**DoD:**
```bash
grep -n "_find_address_by_fields\|find_address_by_fields" app/core/unified_state_loader.py app/core/builders.py
# Every call site should now check address_cache first
```

### Fix 2: Return `state_code` from `_load_batch_indexes`

**Current signature:**
```python
def _load_batch_indexes(self, ...) -> tuple[CommitteeIndex, PersonIndex]:
    ...
    state_record = db_manager._resolve_state_record(state_id)
    ...
```

**Required signature:**
```python
def _load_batch_indexes(self, ...) -> tuple[CommitteeIndex, PersonIndex, str | None]:
    ...
    state_record = db_manager._resolve_state_record(state_id)
    state_code = state_record.state_code if state_record else None
    return committee_index, person_index, state_code
```

**Update the call site in `process_records_batch`:**
```python
# Before:
committee_index, person_index = self._load_batch_indexes(...)
state_record = db_manager._resolve_state_record(state_id)  # second call — DELETE THIS
state_code = state_record.state_code if state_record else None

# After:
committee_index, person_index, state_code = self._load_batch_indexes(...)
```

**DoD:**
```bash
grep -n "_resolve_state_record" app/core/unified_state_loader.py | wc -l
# Should be 1 (only inside _load_batch_indexes), not 2
```

---

## Verification Checklist

```bash
# 1. address_cache attribute exists on UnifiedStateLoader
grep -n "address_cache" app/core/unified_state_loader.py | wc -l  # ≥ 2

# 2. Double state lookup eliminated
grep -n "_resolve_state_record" app/core/unified_state_loader.py | wc -l
# Expected: 1

# 3. _load_batch_indexes returns 3-tuple
grep -A 3 "def _load_batch_indexes" app/core/unified_state_loader.py | grep "return"

# 4. Tests pass
uv run pytest tests/ -q -k "loader or batch or address"

# 5. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
perf(loader): add address_cache and eliminate double state lookup per batch

Address dedup was issuing one SELECT per transaction against unified_addresses
(N+1 at scale). Now uses an in-memory dict keyed on (street_1, city, state, zip)
consistent with the existing person_cache and committee_cache pattern.

_load_batch_indexes now returns state_code as the third tuple element,
eliminating a redundant _resolve_state_record call in process_records_batch.

Fixes P2-PERF-001, P2-PERF-002
```
