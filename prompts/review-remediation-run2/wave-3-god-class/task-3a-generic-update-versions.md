# Task 3a — Collapse Five-Way `update_*` and `get_*_versions` Clones

**Wave:** 3 — God Class  
**Branch:** `remediation-r3/wave-3/task-3a-generic-update-versions`  
**Effort:** ~3-4 hours  
**Parallel with:** 3b, 3c

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-DRY-001 | Five `update_*` methods — 95% identical, ~120 lines cloned | P1 Critical |
| RF-DRY-002 | Five `get_*_versions` methods — 98% identical, ~50 lines cloned | P1 Critical |

---

## Context

`app/core/unified_database.py` contains five structurally identical blocks for each entity type:

**update_* methods (lines 612-834, ~30 lines each):**
- `update_transaction` (L612-664)
- `update_person` (L682-718)
- `update_committee` (L732-770)
- `update_address` (L784-822)
- `update_committee_person` (L945-992)

**get_*_versions methods (~10 lines each):**
- `get_transaction_versions` (L666-680)
- `get_person_versions` (L720-730)
- `get_committee_versions` (L772-782)
- `get_address_versions` (L824-834)
- `get_committee_person_versions` (L994-1006)

Each `update_*` block: fetch entity by id → count existing version rows → call `_record_version(...)` → apply `setattr` loop → set audit fields → commit.
Each `get_*_versions` block: open session → select version_model where fk == id → order by version_number → return.

**This task only touches `unified_database.py`.** Wave 3c will handle splitting the file into separate classes — this task just collapses the duplication within the existing file.

---

## Changes Required

### Step 1: Add `_update_entity` private method

Add this method to `UnifiedDatabaseManager` (place it just before the first `update_transaction` method):

```python
def _update_entity(
    self,
    entity_model: type,
    entity_id: int | str,
    updates: dict,
    *,
    version_model: type,
    fk_field: str,
    user: str | None = None,
    reason: str | None = None,
    amendment_details: str | None = None,
) -> object | None:
    """Generic update-with-versioning for any entity.

    Fetches the entity, records a version snapshot, applies updates,
    sets audit fields, and commits — all in one session.
    """
    with self.get_session() as session:
        entity = session.get(entity_model, entity_id)
        if entity is None:
            return None
        version_count = session.exec(
            select(func.count()).where(
                getattr(version_model, fk_field) == entity_id
            )
        ).one()
        _record_version(
            session,
            entity=entity,
            version_model=version_model,
            fk_field=fk_field,
            fk_value=entity_id,
            version_number=version_count + 1,
            user=user,
            reason=reason,
            amendment_details=amendment_details,
        )
        for k, v in updates.items():
            if not hasattr(entity, k):
                raise AttributeError(
                    f"{entity_model.__name__} has no field '{k}'"
                )
            setattr(entity, k, v)
        entity.last_modified_at = _utc_now()
        entity.last_modified_by = user
        entity.change_reason = reason
        entity.amendment_details = amendment_details
        session.add(entity)
        session.commit()
        session.refresh(entity)
        return entity
```

Verify that `func` is imported from `sqlmodel` or `sqlalchemy`:
```bash
grep -n "^from sqlmodel import\|^from sqlalchemy import" app/core/unified_database.py | head -5
```
Add `func` to the import if missing.

### Step 2: Replace each `update_*` method with a one-liner delegation

```python
def update_transaction(self, transaction_id, updates, user=None, reason=None, amendment_details=None):
    return self._update_entity(
        UnifiedTransaction, transaction_id, updates,
        version_model=UnifiedTransactionVersion,
        fk_field="transaction_id",
        user=user, reason=reason, amendment_details=amendment_details,
    )

def update_person(self, person_id, updates, user=None, reason=None, amendment_details=None):
    return self._update_entity(
        UnifiedPerson, person_id, updates,
        version_model=UnifiedPersonVersion,
        fk_field="person_id",
        user=user, reason=reason, amendment_details=amendment_details,
    )

def update_committee(self, committee_id, updates, user=None, reason=None, amendment_details=None):
    return self._update_entity(
        UnifiedCommittee, committee_id, updates,
        version_model=UnifiedCommitteeVersion,
        fk_field="committee_id",
        user=user, reason=reason, amendment_details=amendment_details,
    )

def update_address(self, address_id, updates, user=None, reason=None, amendment_details=None):
    return self._update_entity(
        UnifiedAddress, address_id, updates,
        version_model=UnifiedAddressVersion,
        fk_field="address_id",
        user=user, reason=reason, amendment_details=amendment_details,
    )

def update_committee_person(self, cp_id, updates, user=None, reason=None, amendment_details=None):
    return self._update_entity(
        UnifiedCommitteePerson, cp_id, updates,
        version_model=UnifiedCommitteePersonVersion,
        fk_field="committee_person_id",
        user=user, reason=reason, amendment_details=amendment_details,
    )
```

**Verify the FK field names** by reading the version model definitions. Adjust `fk_field` if the actual column name differs.

### Step 3: Add `_get_versions` private method

```python
def _get_versions(
    self,
    version_model: type,
    fk_field: str,
    entity_id: int | str,
) -> list:
    """Return all version records for an entity, ordered by version_number."""
    with self.get_session() as session:
        return session.exec(
            select(version_model)
            .where(getattr(version_model, fk_field) == entity_id)
            .order_by(version_model.version_number)
        ).all()
```

### Step 4: Replace each `get_*_versions` method with delegation

```python
def get_transaction_versions(self, transaction_id):
    return self._get_versions(UnifiedTransactionVersion, "transaction_id", transaction_id)

def get_person_versions(self, person_id):
    return self._get_versions(UnifiedPersonVersion, "person_id", person_id)

def get_committee_versions(self, committee_id):
    return self._get_versions(UnifiedCommitteeVersion, "committee_id", committee_id)

def get_address_versions(self, address_id):
    return self._get_versions(UnifiedAddressVersion, "address_id", address_id)

def get_committee_person_versions(self, cp_id):
    return self._get_versions(UnifiedCommitteePersonVersion, "committee_person_id", cp_id)
```

---

## Verification Checklist

```bash
# 1. Net line reduction — unified_database.py should be ~170 lines shorter
wc -l app/core/unified_database.py

# 2. _update_entity and _get_versions exist
grep -n "def _update_entity\|def _get_versions" app/core/unified_database.py \
  && echo "PASS" || echo "FAIL"

# 3. update_transaction is now a one-liner
grep -A 6 "def update_transaction" app/core/unified_database.py | head -6

# 4. All update_* tests pass
uv run pytest tests/ -q -k "update or version"

# 5. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(db): collapse 10 cloned entity methods into 2 generic helpers

Replace five update_* methods (95% identical, ~120 lines) with
_update_entity() generic helper. Replace five get_*_versions methods
(98% identical, ~50 lines) with _get_versions() generic helper.

Net reduction: ~170 lines. Single place for future audit field changes.
Uses func.count() instead of .all() for version counting.

Fixes RF-DRY-001, RF-DRY-002
```
