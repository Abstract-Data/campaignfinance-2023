# Task 2b — `OklahomaContribution` Four-Level Model Split

**Wave:** 2 — Singletons & Coupling  
**Branch:** `remediation-r3/wave-2/task-2b-oklahoma-contribution-split`  
**Effort:** ~1 hour  
**Parallel with:** 2a, 2c

---

## Finding Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P2-SEC-001 | `OklahomaContribution` uses `table=True` directly as ingestion shape; unknown CSV fields silently dropped | P2 High |

---

## Context

`app/states/oklahoma/validators/ok_contribution.py:L20`:
```python
class OklahomaContribution(OklahomaSettings, table=True):
    ...
```

This single-class pattern uses the SQLModel table model directly as the ingestion/validation shape. The effective Pydantic config is `extra='ignore'` (SQLModel default), meaning unknown fields from a raw CSV row are silently dropped rather than rejected. This is a security/data-quality gap: a malformed row with an unexpected field will not raise an error.

The correct pattern — already applied to `OklahomaExpenditure` in the prior remediation — is a **four-level hierarchy**:
1. `OklahomaContributionBase(CreateValidatorModel, OklahomaSettings)` — `extra='forbid'`, strict validation
2. `OklahomaContributionCreate(OklahomaContributionBase)` — ingestion shape
3. `OklahomaContributionRead(OklahomaContributionBase)` — API/read shape (includes `id`, `created_at`)
4. `OklahomaContribution(OklahomaContributionBase, table=True)` — SQLModel table

---

## Reference Implementation

Read `app/states/oklahoma/validators/ok_expenditure.py` first — mirror its exact structure for contributions. The key elements:

```python
from app.abcs.base_models import CreateValidatorModel, ReadValidatorModel, TableValidatorModel

class OklahomaContributionBase(CreateValidatorModel, OklahomaSettings):
    """Ingestion validator — extra='forbid' from CreateValidatorModel."""
    # All existing OklahomaContribution fields go here
    # Validators (@field_validator, @model_validator) go here
    ...

class OklahomaContributionCreate(OklahomaContributionBase):
    """Shape for new record creation — excludes server-set fields (id, created_at)."""
    pass

class OklahomaContributionRead(ReadValidatorModel, OklahomaContributionBase):
    """Shape for reading — includes id and created_at."""
    pass

class OklahomaContribution(TableValidatorModel, OklahomaContributionBase, table=True):
    """SQLModel table model — the persisted shape."""
    __tablename__ = "oklahoma_contributions"  # verify against existing table name
```

**Critical:** `CreateValidatorModel` must enforce `extra='forbid'`. Verify this in `app/abcs/base_models.py`:
```python
class CreateValidatorModel(SQLModel):
    model_config = ConfigDict(extra="forbid", ...)
```

---

## Migration Impact

### Check all importers of `OklahomaContribution`

```bash
grep -rn "OklahomaContribution" app/ scripts/ tests/
```

For any importer that uses `OklahomaContribution` as the **ingestion shape** (i.e., `OklahomaContribution(**row_dict)`), update it to use `OklahomaContributionCreate(**row_dict)` instead.

For any importer that uses it as the **table model** (SQLAlchemy `session.add(...)`, `session.get(OklahomaContribution, ...)`), keep using `OklahomaContribution`.

### Update `__init__.py` re-exports

If `app/states/oklahoma/validators/__init__.py` exports `OklahomaContribution`, add the new classes to the export list.

---

## Verification Checklist

```bash
# 1. extra='forbid' enforced on ingestion shape
python -c "
from app.states.oklahoma.validators.ok_contribution import OklahomaContributionCreate
try:
    OklahomaContributionCreate(**{'unknown_field': 'x', 'contribution_date': '01/01/2024'})
    print('FAIL — extra field accepted')
except Exception as e:
    print('PASS —', type(e).__name__)
"

# 2. Table model still queryable
python -c "
from app.states.oklahoma.validators.ok_contribution import OklahomaContribution
from sqlmodel import SQLModel
assert hasattr(OklahomaContribution, '__tablename__'), 'FAIL'
print('PASS — table model OK')
"

# 3. No import errors
python -c "import app.states.oklahoma.validators.ok_contribution; print('OK')"

# 4. Tests pass
uv run pytest tests/ -q -k "oklahoma or contribution"
```

---

## Commit Message

```
refactor(oklahoma): apply four-level model split to OklahomaContribution

Mirror the OklahomaExpenditure pattern:
  OklahomaContributionBase (CreateValidatorModel, extra='forbid')
  OklahomaContributionCreate (ingestion shape)
  OklahomaContributionRead (read shape with id/created_at)
  OklahomaContribution (table=True, SQLModel persistence)

Unknown CSV fields are now rejected at ingestion instead of silently dropped.

Fixes P2-SEC-001
```
