# Task 5a — DRY: INDIVIDUAL/ENTITY Mixin, `clear_blank_strings`, `FileReader.read()`

**Wave:** 5 — Quality & Infrastructure  
**Branch:** `remediation-r3/wave-5/task-5a-validator-dry`  
**Effort:** ~2-3 hours  
**Parallel with:** 5b, 5c, 5d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-DRY-003 | INDIVIDUAL/ENTITY discriminator validation duplicated in TX contributions + TX expenses (~85% identical) | P2 High |
| RF-DRY-004 | `clear_blank_strings` defined as an inline method in `TECContributionBase` — duplicates the function in `funcs` | P2 High |
| RF-DRY-005 | CSV/Parquet file dispatcher (`if suffix == '.parquet': ... else: read_csv(...)`) duplicated in 3 places | P2 High |

---

## Fix 1: Extract INDIVIDUAL/ENTITY Validator to `_mixins.py`

### Context

`app/states/texas/validators/texas_contributions.py:245-277` (`_check_individual_field`) and `app/states/texas/validators/texas_expenses.py:194-235` (`_check_payee_field`) implement the same INDIVIDUAL/ENTITY discriminator pattern with ~85% structural identity: check the type field, then assert either the name-last field (INDIVIDUAL) or the organization field (ENTITY) is present.

### Changes

**Create or open `app/states/texas/validators/_mixins.py`** (it may already exist — check first):

```python
def validate_individual_entity_discriminator(
    values: dict,
    *,
    type_field: str,
    individual_name_field: str,
    entity_org_field: str,
) -> dict:
    """Validate INDIVIDUAL/ENTITY discriminator pattern.

    Raises PydanticCustomError if:
    - type is INDIVIDUAL but individual_name_field is empty
    - type is ENTITY but entity_org_field is empty
    """
    from pydantic_core import PydanticCustomError

    person_type = values.get(type_field, "")

    if person_type == "INDIVIDUAL":
        if not values.get(individual_name_field):
            raise PydanticCustomError(
                "missing_required_value",
                f"{individual_name_field} is required for INDIVIDUAL {type_field}",
                {"column": individual_name_field, "value": values.get(individual_name_field)},
            )
    elif person_type == "ENTITY":
        if not values.get(entity_org_field):
            raise PydanticCustomError(
                "missing_required_value",
                f"{entity_org_field} is required for ENTITY {type_field}",
                {"column": entity_org_field, "value": values.get(entity_org_field)},
            )
    return values
```

**Update `texas_contributions.py`:**
```python
from app.states.texas.validators._mixins import validate_individual_entity_discriminator

@model_validator(mode="before")
@classmethod
def _check_individual_field(cls, values):
    return validate_individual_entity_discriminator(
        values,
        type_field="contributorPersentTypeCd",
        individual_name_field="contributorNameLast",
        entity_org_field="contributorNameOrganization",
    )
```

**Update `texas_expenses.py`:** Same pattern, with the appropriate field names for payees.

---

## Fix 2: Remove Duplicate `clear_blank_strings` from `TECContributionBase`

### Context

`texas_contributions.py:L178-189` contains an inline `@model_validator(mode='before')` method that is a copy of `funcs.clear_blank_strings`. Every other validator uses `_clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)` (or the equivalent mixin pattern). Only `TECContributionBase` re-implements it inline.

### Changes

**Read `texas_contributions.py:L178-189`** to confirm it is the duplicate.

**Delete the inline method** from `TECContributionBase`.

**Verify the mixin chain already provides `clear_blank_strings`:**
```bash
# Check if TECContributionBase inherits from AddressValidatedModel or similar mixin
grep -n "class TECContributionBase" app/states/texas/validators/texas_contributions.py
grep -n "class AddressValidatedModel\|clear_blank_strings" app/states/texas/validators/_mixins.py
```

If the mixin is not providing it, add the standard wire-up to the base class instead of the inline method:
```python
class TECContributionBase(...):
    _clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)
```

**DoD:**
```bash
grep -n "def clear_blank_strings" app/states/texas/validators/texas_contributions.py \
  && echo "FAIL" || echo "PASS"
```

---

## Fix 3: Add `FileReader.read(path)` Dispatch Method

### Context

Three places in the codebase contain identical 2-branch CSV/Parquet dispatch:
```python
if file_path.suffix.lower() == '.parquet':
    data_generator = file_reader.read_parquet(file_path)
else:
    data_generator = file_reader.read_csv(file_path)
```

Locations:
- `app/core/unified_database.py:191-194`
- `app/core/unified_state_loader.py:221-226`
- `app/core/unified_state_loader.py:334-337`

### Changes

**Find the `FileReader` class:**
```bash
grep -rn "class FileReader\|def read_parquet\|def read_csv" app/
```

**Add a `read(path)` dispatch method to the class:**
```python
def read(self, file_path: Path):
    """Dispatch to read_parquet or read_csv based on file extension."""
    if file_path.suffix.lower() == ".parquet":
        return self.read_parquet(file_path)
    return self.read_csv(file_path)
```

**Replace all three call sites:**
```python
# Before:
if file_path.suffix.lower() == '.parquet':
    data_generator = file_reader.read_parquet(file_path)
else:
    data_generator = file_reader.read_csv(file_path)

# After:
data_generator = file_reader.read(file_path)
```

**DoD:**
```bash
# No more inline dispatch blocks
grep -n "\.suffix.*parquet" app/core/unified_database.py app/core/unified_state_loader.py \
  && echo "FAIL" || echo "PASS"
```

---

## Verification Checklist

```bash
# 1. validate_individual_entity_discriminator exists in mixins
grep -n "def validate_individual_entity_discriminator" \
  app/states/texas/validators/_mixins.py && echo "PASS" || echo "FAIL"

# 2. No inline duplicate clear_blank_strings in TECContributionBase
grep -n "def clear_blank_strings" app/states/texas/validators/texas_contributions.py \
  && echo "FAIL" || echo "PASS"

# 3. FileReader.read() exists
grep -n "def read\b" $(grep -rln "class FileReader" app/) && echo "PASS" || echo "FAIL"

# 4. No inline suffix dispatch blocks remain
grep -n "\.suffix.*parquet" app/core/unified_database.py app/core/unified_state_loader.py \
  && echo "FAIL" || echo "PASS"

# 5. Tests pass
uv run pytest tests/ -q -k "contribution or expense or validator or file_reader"

# 6. Full suite
uv run pytest tests/ -q
```

---

## Commit Message

```
refactor(validators,io): three DRY fixes — mixin, blank-string dedup, FileReader.read()

- Extract INDIVIDUAL/ENTITY discriminator validation into
  validate_individual_entity_discriminator() in _mixins.py;
  used by texas_contributions and texas_expenses

- Remove duplicate clear_blank_strings implementation from TECContributionBase;
  rely on the standard funcs.clear_blank_strings mixin wire-up

- Add FileReader.read(path) dispatch method eliminating three copies of
  the parquet/csv 2-branch switch across unified_database and unified_state_loader

Fixes RF-DRY-003, RF-DRY-004, RF-DRY-005
```
