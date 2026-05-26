# Task 1b — Fix / Delete `ok_validation_funcs.py` (Import-Time Crash Risk)

**Wave:** 1 — Immediate  
**Branch:** `remediation-r3/wave-1/task-1b-ok-validation-dead-code`  
**Effort:** ~15 minutes  
**Parallel with:** 1a, 1c, 1d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-SMELL-004 | Broken `partial` call — crashes on import | P1 Critical |
| RF-DEAD-001 | Entire `ok_validation_funcs.py` file is dead/unreachable | P3 |

---

## Context

`app/states/oklahoma/funcs/ok_validation_funcs.py:5`:
```python
ok_date_validation = partial(funcs.validate_date(fmt='%m/%d/%Y'))
```

This is **syntactically wrong and will crash on import**. `funcs.validate_date(fmt='%m/%d/%Y')` calls the function immediately (passing no `v` argument → raises `TypeError`), then attempts to pass the result to `partial()`. Any environment or future test that causes this module to be imported will crash at module load time.

Additionally, `ok_date_validation` is **never imported anywhere** in the codebase. The entire file is dead code.

---

## Decision

**Delete the file.** There is no valid reason to keep a dead file that crashes on import. If Oklahoma date validation is needed in the future, it belongs in `app/states/oklahoma/validators/ok_settings.py` using the standard `funcs.validate_date` mixin pattern already used by Texas.

**Verify nothing imports it first:**
```bash
grep -rn "ok_validation_funcs\|ok_date_validation" app/ tests/ scripts/
```
Expected: zero matches. If any match appears, update that importer to use `funcs.validate_date` directly before deleting.

---

## Changes Required

```bash
git rm app/states/oklahoma/funcs/ok_validation_funcs.py
```

If `app/states/oklahoma/funcs/__init__.py` re-exports `ok_date_validation`, remove that export line too.

---

## Verification Checklist

```bash
# 1. File is gone
ls app/states/oklahoma/funcs/ok_validation_funcs.py 2>/dev/null && echo "FAIL" || echo "PASS"

# 2. No remaining references
grep -rn "ok_validation_funcs\|ok_date_validation" app/ tests/ scripts/ \
  && echo "FAIL" || echo "PASS"

# 3. Oklahoma module still imports cleanly
python -c "import app.states.oklahoma; print('OK')"

# 4. Full test suite still passes
uv run pytest tests/ -q
```

---

## Commit Message

```
fix(oklahoma): delete ok_validation_funcs.py — import-time crash risk

The file contained a broken partial() call that would raise TypeError on
import. The symbol (ok_date_validation) was never imported anywhere, making
the entire file dead code.

Fixes RF-SMELL-004, RF-DEAD-001
```
