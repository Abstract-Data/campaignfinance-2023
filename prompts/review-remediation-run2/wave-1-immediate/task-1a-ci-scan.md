# Task 1a — Fix CI Dependency Scan + Coverage Threshold

**Wave:** 1 — Immediate  
**Branch:** `remediation-r3/wave-1/task-1a-ci-scan`  
**Effort:** ~30 minutes  
**Parallel with:** 1b, 1c, 1d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P1-QUAL-001 | `dependency-scan.yml` step broken by copy-paste error | P1 Critical |
| P3-QUAL-003 | Coverage threshold mismatch: `pyproject.toml` says 60, CI says 70 | P3 |

---

## Context

The `dependency-scan.yml` workflow was added in the Wave 6 remediation but contains a copy-paste error: lines from `ci-tests.yml` (`--cov-fail-under=70`, `--junitxml`, `PYTHONPATH`, Codecov upload) appear inside the `pip-audit` step body. pip-audit does not accept pytest flags. The step either silently errors or produces no CVE output — meaning every PR since the file was added has had a broken vulnerability scan.

Separately, `pyproject.toml:83` sets `fail_under = 60` but `.github/workflows/ci-tests.yml:30` passes `--cov-fail-under=70`. The CI value takes precedence (higher wins), but a developer running `uv run pytest --cov` locally sees a misleading 60% gate and may believe they're passing when CI would reject them.

---

## Changes Required

### Fix 1: `.github/workflows/dependency-scan.yml`

**Current state (broken):**
```yaml
- name: Run dependency vulnerability scan
  run: |
    uvx pip-audit
    pytest tests/ --cov=app --cov-fail-under=70 --junitxml=...
    # [more pytest flags pasted here]
```

**Required state:**
```yaml
name: Dependency Vulnerability Scan

on:
  pull_request:
  push:
    branches: [main]
  schedule:
    - cron: '0 6 * * 1'   # Weekly Monday 6am UTC

jobs:
  audit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install uv
        run: pip install uv

      - name: Install dependencies
        run: uv sync --frozen

      - name: Run dependency vulnerability scan
        run: uvx pip-audit
```

**DoD:** `grep -n "cov-fail-under\|junitxml\|PYTHONPATH\|codecov" .github/workflows/dependency-scan.yml` → empty output.

### Fix 2: `pyproject.toml` — Coverage Threshold

**Location:** `pyproject.toml:83` (inside `[tool.pytest.ini_options]` or `[tool.coverage.report]`)

**Current:**
```toml
fail_under = 60
```

**Required:**
```toml
fail_under = 70
```

**Note:** Do NOT raise to 80 here — that is Wave 5c's responsibility once more Hypothesis tests are added. The goal now is simply to align local and CI gates.

**DoD:** `grep "fail_under" pyproject.toml` → `fail_under = 70`

---

## Verification Checklist

```bash
# 1. No pytest flags inside pip-audit step
grep -n "cov-fail-under\|junitxml\|PYTHONPATH\|codecov" \
  .github/workflows/dependency-scan.yml && echo "FAIL" || echo "PASS"

# 2. Coverage threshold aligned
grep "fail_under" pyproject.toml | grep "70" && echo "PASS" || echo "FAIL"

# 3. pip-audit invocation exists
grep -n "pip-audit" .github/workflows/dependency-scan.yml | grep -v "#" \
  && echo "PASS" || echo "FAIL"

# 4. Full test suite still passes
uv run pytest tests/ -q
```

---

## Commit Message

```
fix(ci): restore dependency scan and align coverage threshold

- Remove copy-pasted pytest flags from pip-audit step in dependency-scan.yml
- Clean up workflow to: checkout → setup-python → install uv → pip-audit only
- Add weekly Monday cron to dependency-scan.yml
- Align pyproject.toml fail_under=70 with CI --cov-fail-under=70

Fixes P1-QUAL-001, P3-QUAL-003
```
