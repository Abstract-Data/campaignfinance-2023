# Task 1c — Dead Code Sweep + Typing Import Modernization

**Wave:** 1 — Immediate  
**Branch:** `remediation-r3/wave-1/task-1c-dead-code-cleanup`  
**Effort:** ~30 minutes  
**Parallel with:** 1a, 1b, 1d

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| RF-DEAD-002 | ~50 lines commented-out in `texas_contributions.py:195-243` | P3 |
| RF-DEAD-003 | ~45 lines commented-out in `ok_settings.py:21-68` | P3 |
| RF-DRY-006 | `Dict`, `List`, `Optional` imported from `typing` (use Python 3.10+ builtins) | P3 |

---

## Changes Required

### Fix 1: Strip commented-out validators from `texas_contributions.py`

**Location:** `app/states/texas/validators/texas_contributions.py:195-243`

These are ~50 lines of commented-out validator methods (`_check_state_code`, `copy_sos_fullname_first_and_last`) that were replaced by the mixin chain. They should have been deleted in the prior remediation; they survived because the file was only partially cleaned.

**Action:** Read the block from line 195 to 243. Confirm they are all inside `# ...` comment markers. Delete them in their entirety. Do not delete any surrounding active code.

**DoD:**
```bash
wc -l app/states/texas/validators/texas_contributions.py
# Should be ~50 lines shorter than before this task
grep -n "copy_sos_fullname_first_and_last\|_check_state_code" \
  app/states/texas/validators/texas_contributions.py && echo "FAIL" || echo "PASS"
```

### Fix 2: Strip commented-out validators from `ok_settings.py`

**Location:** `app/states/oklahoma/validators/ok_settings.py:21-68`

~45 lines of commented-out date, name, and zipcode validators that were replaced by the mixin helpers. Same pattern as above.

**Action:** Read lines 21–68. Confirm they are all inside `# ...` markers. Delete them.

**DoD:**
```bash
grep -n "^#.*validate_date\|^#.*format_zipcode\|^#.*parse_candidate" \
  app/states/oklahoma/validators/ok_settings.py && echo "FAIL" || echo "PASS"
```

### Fix 3: Modernize `typing` imports in `unified_database.py`

**Location:** `app/core/unified_database.py:8-9` (and anywhere else `typing.Dict`, `typing.List`, `typing.Optional`, `typing.Tuple` appear in `app/`)

The project targets Python 3.12. `Dict`, `List`, `Optional`, `Tuple` from `typing` are deprecated since 3.9; builtins should be used instead.

**Run the ruff autofix:**
```bash
uv run ruff check --select UP006,UP007,UP035 --fix app/core/unified_database.py
```

Then verify the imports are gone:
```bash
grep -n "from typing import.*Dict\|from typing import.*List\|from typing import.*Optional\|from typing import.*Tuple" \
  app/core/unified_database.py && echo "FAIL" || echo "PASS"
```

**Also sweep the rest of app/ for the same pattern:**
```bash
uv run ruff check --select UP006,UP007,UP035 --fix app/
```

Review the diff — only legacy type annotation imports should change, not runtime logic.

---

## Verification Checklist

```bash
# 1. Commented-out validators gone from texas_contributions.py
grep -n "copy_sos_fullname_first_and_last\|_check_state_code" \
  app/states/texas/validators/texas_contributions.py && echo "FAIL" || echo "PASS"

# 2. Commented-out validators gone from ok_settings.py
grep -c "^#" app/states/oklahoma/validators/ok_settings.py
# Should be significantly lower than before

# 3. No legacy typing imports in unified_database.py
grep -n "from typing import" app/core/unified_database.py | \
  grep -E "Dict|List|Optional|Tuple" && echo "FAIL" || echo "PASS"

# 4. Tests still pass
uv run pytest tests/ -q
```

---

## Commit Message

```
chore(cleanup): remove commented-out validators and modernize typing imports

- Delete ~50 lines commented-out in texas_contributions.py (replaced methods)
- Delete ~45 lines commented-out in ok_settings.py (replaced validators)
- Replace Dict/List/Optional/Tuple from typing with Python 3.10+ builtins
  in unified_database.py and across app/

Fixes RF-DEAD-002, RF-DEAD-003, RF-DRY-006
```
