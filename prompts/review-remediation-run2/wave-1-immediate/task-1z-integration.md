# Task 1z — Wave 1 Integration

**Wave:** 1 — Immediate (serial integration)  
**Branch:** `remediation-r3/wave-1/integration`  
**Depends on:** All of 1a, 1b, 1c, 1d completed and passing  
**Effort:** ~30 minutes

---

## Purpose

Merge all Wave 1 parallel branches, run a sweep of all wave-level DoD checks, and tag the milestone.

---

## Steps

### 1. Merge all Wave 1 branches

```bash
git checkout main
git merge remediation-r3/wave-1/task-1a-ci-scan
git merge remediation-r3/wave-1/task-1b-ok-validation-dead-code
git merge remediation-r3/wave-1/task-1c-dead-code-cleanup
git merge remediation-r3/wave-1/task-1d-quick-quality
```

Resolve any conflicts (unlikely — all four tasks touch different files).

### 2. Full Sweep — All Wave 1 DoD Checks

```bash
# P1-QUAL-001: pip-audit step is clean
grep -n "cov-fail-under\|junitxml\|PYTHONPATH\|codecov" \
  .github/workflows/dependency-scan.yml && echo "FAIL" || echo "PASS"

# P3-QUAL-003: coverage threshold aligned
grep "fail_under" pyproject.toml | grep "70" && echo "PASS" || echo "FAIL"

# RF-SMELL-004 / RF-DEAD-001: dead file gone
ls app/states/oklahoma/funcs/ok_validation_funcs.py 2>/dev/null && echo "FAIL" || echo "PASS"

# No remaining ok_validation_funcs references
grep -rn "ok_validation_funcs\|ok_date_validation" app/ tests/ scripts/ \
  && echo "FAIL" || echo "PASS"

# RF-DEAD-002: commented-out TX contribution validators gone
grep -n "copy_sos_fullname_first_and_last\|_check_state_code" \
  app/states/texas/validators/texas_contributions.py && echo "FAIL" || echo "PASS"

# RF-DRY-006: no legacy typing imports in unified_database.py
grep -n "from typing import" app/core/unified_database.py | \
  grep -E "Dict|List|Optional|Tuple" && echo "FAIL" || echo "PASS"

# P3-QUAL-001: no utcnow anywhere
grep -rn "utcnow" app/ && echo "FAIL" || echo "PASS"

# P3-QUAL-002: no bare except:
grep -rn "^\s*except:\s*$" app/ && echo "FAIL" || echo "PASS"
```

### 3. Run Full Test Suite

```bash
uv run pytest tests/ -q --tb=short
```

All tests must pass. If any fail, do not tag — fix and re-run.

### 4. Oklahoma module import check

```bash
python -c "import app.states.oklahoma; print('Oklahoma imports OK')"
python -c "import app.states.texas; print('Texas imports OK')"
python -c "import app.core.unified_database; print('unified_database imports OK')"
```

### 5. Tag

```bash
git tag remediation-r3/wave-1-complete
git push origin remediation-r3/wave-1-complete
```

---

## Expected State After Integration

- `dependency-scan.yml` runs `uvx pip-audit` only — no pytest flags
- Coverage gate: both `pyproject.toml` and `ci-tests.yml` say 70
- `ok_validation_funcs.py` deleted
- ~95 lines of commented-out code removed across two validator files
- `unified_database.py` uses `dict`, `list`, `X | None` instead of `Dict`, `List`, `Optional`
- No `utcnow` anywhere in `app/`
- No bare `except:` anywhere in `app/`
- All tests passing
