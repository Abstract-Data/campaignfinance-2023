# Task 1d — Quick Quality Fixes: `utcnow`, Bare Excepts

**Wave:** 1 — Immediate  
**Branch:** `remediation-r3/wave-1/task-1d-quick-quality`  
**Effort:** ~20 minutes  
**Parallel with:** 1a, 1b, 1c

---

## Findings Addressed

| ID | Finding | Severity |
|----|---------|----------|
| P3-QUAL-001 | `datetime.utcnow()` surviving in `spac.py` | P3 |
| P3-QUAL-002 | Bare `except:` in two legacy ABC session scopes | P3 |

---

## Context

Both of these were supposed to be fixed in the prior remediation round. `spac.py` was missed because it is a source model rather than a unified model. The bare `except:` clauses were not fixed because the files (`abc_db_loader.py`, `db_loader.py`) are in the legacy ABC layer — they weren't on the active hot-path. They remain importable surface area that catches `SystemExit` and `KeyboardInterrupt`, which can mask SIGINT during long loads.

---

## Changes Required

### Fix 1: `datetime.utcnow()` in `spac.py`

**Location:** `app/core/source_models/spac.py:L31-32`

**Current:**
```python
created_at: datetime = Field(default_factory=datetime.utcnow)
updated_at: datetime = Field(default_factory=datetime.utcnow)
```

**Required:**
```python
from datetime import datetime, timezone

created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
```

Confirm the import at the top of the file uses `from datetime import datetime, timezone` (not `import datetime`).

**Sweep for any remaining utcnow:**
```bash
grep -rn "utcnow" app/ && echo "FAIL — residue found" || echo "PASS"
```

### Fix 2: Bare `except:` in `abc_db_loader.py`

**Location:** `app/abcs/abc_db_loader.py:L52`

**Current:**
```python
except:
    session.rollback()
    raise
```

**Required:**
```python
except Exception:
    session.rollback()
    raise
```

**Why:** Bare `except:` catches `BaseException` including `SystemExit`, `KeyboardInterrupt`, and `GeneratorExit`. This triggers `session.rollback()` before re-raising, which is usually harmless but can produce confusing log output during graceful shutdown. `except Exception:` is the correct scope — it does not catch `SystemExit` or `KeyboardInterrupt`.

### Fix 3: Bare `except:` in `db_loader.py`

**Location:** `app/funcs/db_loader.py:L20`

Same fix as above:

**Current:**
```python
except:
    session.rollback()
    raise
```

**Required:**
```python
except Exception:
    session.rollback()
    raise
```

---

## Verification Checklist

```bash
# 1. No datetime.utcnow() anywhere in app/
grep -rn "utcnow" app/ && echo "FAIL" || echo "PASS"

# 2. No bare except: anywhere in app/
grep -rn "^\s*except:\s*$" app/ && echo "FAIL" || echo "PASS"

# 3. spac.py timezone.utc correct
grep -n "timezone.utc" app/core/source_models/spac.py | wc -l
# Should be 2 (created_at and updated_at)

# 4. Tests pass
uv run pytest tests/ -q
```

---

## Commit Message

```
fix(quality): fix utcnow survivor in spac.py and bare excepts in legacy ABCs

- Replace datetime.utcnow() with datetime.now(timezone.utc) in spac.py
- Change bare except: → except Exception: in abc_db_loader.py:52
- Change bare except: → except Exception: in db_loader.py:20

Fixes P3-QUAL-001, P3-QUAL-002
```
