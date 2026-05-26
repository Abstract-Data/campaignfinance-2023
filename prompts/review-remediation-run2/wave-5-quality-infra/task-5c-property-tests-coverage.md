# Task 5c — Hypothesis Property Tests + Raise Coverage Gate to 80%

**Wave:** 5 — Quality & Infrastructure  
**Branch:** `remediation-r3/wave-5/task-5c-property-tests-coverage`  
**Effort:** ~3 hours  
**Parallel with:** 5a, 5b, 5d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P3-QUAL-005 | `hypothesis` is a dev dependency but no `@given` tests exist | P3 |
| R8 | Coverage gate at 70% — raise to 80% incrementally | P3 |

---

## Context

The `hypothesis` library is installed (`pyproject.toml` dev deps) and the developer assessment flagged its absence as a missed opportunity. The targets are:
- `validate_date` (handles many format strings, empty/None, Unicode)
- `format_zipcode` (just simplified in 5b — perfect time to add invariant tests)
- `validate_phone_number` (format normalization)

These are pure-function validators with clearly testable invariants — ideal Hypothesis targets.

---

## Property Tests to Add

### `tests/test_validator_property.py` (new file)

```python
"""Property-based tests for core validator functions using Hypothesis."""
from __future__ import annotations

from hypothesis import given, settings, assume
from hypothesis import strategies as st
import pytest

from app.funcs.validator_functions import (
    validate_date,
    format_zipcode,
    validate_phone_number,
    clear_blank_strings,
)


class TestValidateDateProperties:
    @given(st.text())
    @settings(max_examples=500)
    def test_validate_date_never_raises_on_any_string(self, value: str):
        """validate_date must handle any string input without raising."""
        try:
            result = validate_date(value)
            assert result is None or isinstance(result, str)
        except Exception as e:
            pytest.fail(f"validate_date raised on input {value!r}: {e}")

    @given(st.none())
    def test_validate_date_handles_none(self, value):
        """validate_date must handle None input."""
        result = validate_date(value)
        assert result is None or isinstance(result, str)

    @given(st.just(""))
    def test_validate_date_handles_empty_string(self, value: str):
        result = validate_date(value)
        assert result is None or result == ""


class TestFormatZipcodeProperties:
    @given(st.text())
    @settings(max_examples=500)
    def test_format_zipcode_never_raises(self, value: str):
        """format_zipcode must handle any string without raising."""
        try:
            result = format_zipcode(value)
            assert result is None or isinstance(result, str)
        except Exception as e:
            pytest.fail(f"format_zipcode raised on {value!r}: {e}")

    @given(st.from_regex(r"[0-9]{5}", fullmatch=True))
    def test_five_digit_zipcode_passes_through(self, value: str):
        """A valid 5-digit zipcode should be returned unchanged."""
        result = format_zipcode(value)
        assert result == value, f"Expected {value!r}, got {result!r}"

    @given(st.from_regex(r"[0-9]{9}", fullmatch=True))
    def test_nine_digit_zipcode_gets_hyphen(self, value: str):
        """A 9-digit zipcode should be formatted as XXXXX-XXXX."""
        result = format_zipcode(value)
        assert result == value[:5] + "-" + value[5:]

    @given(st.text(min_size=5, max_size=5, alphabet=st.characters(whitelist_categories=("Nd",))))
    def test_output_is_at_most_10_chars(self, value: str):
        """format_zipcode output should never exceed 10 characters."""
        result = format_zipcode(value) or ""
        assert len(result) <= 10


class TestValidatePhoneProperties:
    @given(st.text())
    @settings(max_examples=300)
    def test_validate_phone_never_raises(self, value: str):
        """validate_phone_number must handle any string without raising."""
        try:
            result = validate_phone_number(value)
            assert result is None or isinstance(result, str)
        except Exception as e:
            pytest.fail(f"validate_phone_number raised on {value!r}: {e}")


class TestClearBlankStringsProperties:
    @given(st.dictionaries(st.text(), st.one_of(st.text(), st.none())))
    def test_clear_blank_strings_returns_dict(self, values: dict):
        """clear_blank_strings must always return a dict."""
        result = clear_blank_strings(values)
        assert isinstance(result, dict)

    @given(st.dictionaries(st.text(), st.text(min_size=1)))
    def test_non_blank_values_preserved(self, values: dict):
        """Non-blank string values must be preserved."""
        result = clear_blank_strings(values)
        for k, v in values.items():
            if v.strip():  # non-blank
                assert result.get(k) == v or result.get(k) is None
```

Adapt function signatures and import paths to match the actual functions. Read `app/funcs/validator_functions.py` before writing tests.

### `tests/states/test_tx_validation_property.py` (new file)

```python
"""Property tests for Texas-specific validation functions."""
from hypothesis import given, settings
from hypothesis import strategies as st
import pytest

from app.states.texas.funcs.tx_validation_funcs import (
    validate_tx_date,       # or whatever the function names are
    format_tx_amount,
)


class TestTXDateValidatorProperties:
    @given(st.text())
    @settings(max_examples=300)
    def test_validate_tx_date_never_raises(self, value: str):
        try:
            result = validate_tx_date(value)
            assert result is None or isinstance(result, str)
        except Exception as e:
            pytest.fail(f"validate_tx_date raised on {value!r}: {e}")
```

Read `app/states/texas/funcs/tx_validation_funcs.py` to get the actual function names before writing.

---

## Coverage Gate: Raise to 80%

### Step 1: Check current coverage

```bash
uv run pytest tests/ --cov=app --cov-report=term-missing -q 2>&1 | tail -20
```

Note the current total. If it is already ≥ 80%, proceed directly to raising the gate. If it is < 80%, identify the lowest-coverage modules and add targeted tests before raising.

**Priority modules for coverage uplift** (from developer assessment context):
- `app/core/builders.py` — field resolution logic
- `app/core/unified_state_loader.py` — pipeline steps

### Step 2: Raise the gate

**`pyproject.toml`** (should be 70 after Wave 1 — raise to 80):
```toml
fail_under = 80
```

**`.github/workflows/ci-tests.yml`** (should be 70 after Wave 1 — raise to 80):
```yaml
--cov-fail-under=80
```

### Step 3: Confirm gate works

```bash
uv run pytest tests/ --cov=app --cov-fail-under=80 -q
```

Must exit 0. If it exits 1 (coverage below gate), add more tests before raising the gate.

---

## Verification Checklist

```bash
# 1. Hypothesis tests exist
grep -rn "@given" tests/ | wc -l  # should be ≥ 8

# 2. Property tests are collected by pytest
uv run pytest tests/test_validator_property.py -v --co | grep "test session starts"

# 3. Coverage gate updated
grep "fail_under" pyproject.toml | grep "80" && echo "PASS" || echo "FAIL"
grep "cov-fail-under=80" .github/workflows/ci-tests.yml && echo "PASS" || echo "FAIL"

# 4. Full suite passes at 80% gate
uv run pytest tests/ --cov=app --cov-fail-under=80 -q

# 5. Property tests run fast enough for CI (< 60s total)
time uv run pytest tests/test_validator_property.py -q
```

---

## Commit Message

```
test(validators): add Hypothesis property tests + raise coverage gate to 80%

Add property-based tests for validate_date, format_zipcode, validate_phone_number,
and clear_blank_strings — covering any-string safety, invariants (5-digit passthrough,
9-digit hyphenation), and None/empty handling.

Add property tests for Texas validation functions.

Raise fail_under from 70 to 80 in pyproject.toml and ci-tests.yml.

Fixes P3-QUAL-005, R8
```
