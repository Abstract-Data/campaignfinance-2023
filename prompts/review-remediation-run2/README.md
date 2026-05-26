# Review Remediation Run 2

**Source reviews:** May 25, 2026 re-run (third code review cycle)
**Baseline scores:** Code Quality 7 | Architecture 8 | Performance 5 | Security 8 | Testing 7 | Maintainability 7 | Scalability 5 | Overall 6.8
**Target scores:** Code Quality ≥8 | Architecture ≥9 | Performance ≥7 | Security ≥9 | Testing ≥8 | Maintainability ≥8 | Scalability ≥7 | Overall ≥8.0

## Source Reports

| Report | URL |
|--------|-----|
| Code Review | https://www.notion.so/36c7d7f5629881c1b944c5680a7844fa |
| Refactoring Analysis | https://www.notion.so/36c7d7f5629881dc940cdc4322a9bb3f |
| Developer Assessment | https://www.notion.so/36c7d7f562988154aa87d34700ccccc7 |

---

## Full Backlog

| ID | Finding | Location | Severity | Wave |
|----|---------|----------|----------|------|
| P1-QUAL-001 | `dependency-scan.yml` broken by copy-paste | `.github/workflows/dependency-scan.yml` | P1 | 1a |
| P3-QUAL-003 | Coverage threshold mismatch (60 vs 70) | `pyproject.toml:L83`, `ci-tests.yml:L30` | P3 | 1a |
| RF-SMELL-004 | `ok_validation_funcs.py` broken `partial` — import-time crash | `app/states/oklahoma/funcs/ok_validation_funcs.py:5` | P1 | 1b |
| RF-DEAD-001 | `ok_validation_funcs.py` entire file unreachable | same file | P3 | 1b |
| RF-DEAD-002 | 50 lines commented-out in TX contributions | `texas_contributions.py:195-243` | P3 | 1c |
| RF-DEAD-003 | ~45 lines commented-out in OK settings | `ok_settings.py:21-68` | P3 | 1c |
| RF-DRY-006 | `Dict`/`List`/`Optional` from `typing` (use builtins) | `unified_database.py:8-9` | P3 | 1c |
| P3-QUAL-001 | `datetime.utcnow()` in `spac.py` | `app/core/source_models/spac.py:L31-32` | P3 | 1d |
| P3-QUAL-002 | Bare `except:` in legacy ABCs | `abc_db_loader.py:L52`, `db_loader.py:L20` | P3 | 1d |
| P1-ARCH-001 | Global `unified_sql_processor` singleton mutable state | `app/core/processor.py:L328-L348` | P1 | 2a |
| P2-SEC-001 | `OklahomaContribution` missing four-level split | `ok_contribution.py:L20` | P2 | 2b |
| R4 | `asyncio.run()` inside `OnePasswordItem.__init__` | `app/op.py:L82` | P2 | 2c |
| RF-DRY-001 | Five-way clone of `update_*` pattern (~120 lines) | `unified_database.py:612-834` | P1 | 3a |
| RF-DRY-002 | Five-way clone of `get_*_versions` pattern (~50 lines) | `unified_database.py:666-1006` | P1 | 3a |
| RF-CPLX-002 | `get_summary_statistics` + `get_cross_state_analysis` full-table scans | `unified_database.py:391-522` | P1 | 3b |
| RF-CPLX-001 | `UnifiedDatabaseManager` god class (1,444 lines) | `app/core/unified_database.py` | P1 | 3c |
| P2-PERF-001 | Per-record address dedup N+1 query | `unified_state_loader.py:L451-461`, `builders.py:L190` | P2 | 4a |
| P2-PERF-002 | Double state record lookup per batch | `unified_state_loader.py:L303-312` | P2 | 4a |
| P2-PERF-003 | Commit-per-officer-link inside batch | `unified_state_loader.py:L490-543` | P2 | 4b |
| P2-QUAL-001 | `MONEY_TYPE` bypassed by 12 inline literals | `app/core/models/tables.py` | P2 | 4b |
| RF-SMELL-001 | `UnifiedStateLoader` accumulates mutable instance state | `app/core/unified_state_loader.py:83-101` | P2 | 4c |
| RF-ARCH-001 | `UnifiedStateLoader` tightly coupled to module-global `db_manager` | `unified_state_loader.py:555-581` | P2 | 4d |
| P3-QUAL-004 / RF-SMELL-002 | State officer field mappings hardcoded in loader | `unified_state_loader.py:L155-178` | P3 | 4d |
| RF-DRY-003 | INDIVIDUAL/ENTITY validator pattern duplicated TX contributions/expenses | `texas_contributions.py:245-277`, `texas_expenses.py:194-235` | P2 | 5a |
| RF-DRY-004 | `clear_blank_strings` defined twice | `texas_contributions.py:178-189` | P2 | 5a |
| RF-DRY-005 | CSV/Parquet dispatcher duplicated 3× | `unified_database.py:191-194`, `unified_state_loader.py:221-226, 334-337` | P2 | 5a |
| RF-SMELL-003 | `_get_field_value` fuzzy match silent fallthrough | `builders.py:267-293` | P2 | 5b |
| RF-CPLX-003 | `format_zipcode` 7-branch chain with dead branch | `validator_functions.py:131-166` | P3 | 5b |
| RF-SMELL-005 | Double `_resolve_state_record` call per method | `unified_state_loader.py:393-395` | P3 | 5b |
| RF-SMELL-006 | `add_all` catches `StopIteration` (never raised in for-loop) | `db_loader.py:76-82` | P3 | 5b |
| P3-QUAL-005 | Hypothesis property tests missing | `validator_functions.py`, `tx_validation_funcs.py` | P3 | 5c |
| R8 | Coverage gate at 70% — raise to 80% | `pyproject.toml`, `ci-tests.yml` | P3 | 5c |
| R1 | Selenium scraper fragility — expand drift detector | `app/scrapers/` | P2 | 5d |
| R5 | No containerization | — | P2 | 5d |
| R6 | Splink version not pinned in CI matrix | `pyproject.toml`, `ci.yml` | P2 | 5d |
| R7 | PII exposure in logged `raw_data` field | `app/core/` | P2 | 5d |

---

## Wave Structure

```
wave-1-immediate/      ← Quick wins, crash risks, trivial fixes   (~1 day)
  task-1a-ci-scan.md
  task-1b-ok-validation-dead-code.md
  task-1c-dead-code-cleanup.md
  task-1d-quick-quality.md
  task-1z-integration.md

wave-2-singletons/     ← Singleton hazards, model split, asyncio  (~1 day)
  task-2a-processor-singleton.md
  task-2b-oklahoma-contribution-split.md
  task-2c-asyncio-fix.md
  task-2z-integration.md

wave-3-god-class/      ← UnifiedDatabaseManager split             (~3-4 days)
  task-3a-generic-update-versions.md
  task-3b-analytics-sql-aggregates.md
  task-3c-split-database-manager.md
  task-3z-integration.md

wave-4-performance/    ← N+1 fixes, state loader refactor         (~2-3 days)
  task-4a-address-cache-double-lookup.md
  task-4b-commit-money-type.md
  task-4c-load-context-dataclass.md
  task-4d-loader-injection.md
  task-4z-integration.md

wave-5-quality-infra/  ← DRY validators, Hypothesis, Docker       (~3-4 days)
  task-5a-validator-dry.md
  task-5b-code-smells.md
  task-5c-property-tests-coverage.md
  task-5d-infra.md
  task-5z-integration.md
```

---

## Orchestration Instructions

Each wave (1–5) runs **parallel tasks a–d concurrently**, then a single **serial integration task (z)**.

```
Agent A → task-Na  ─┐
Agent B → task-Nb  ─┤─→ merge ─→ task-Nz (serial integration)
Agent C → task-Nc  ─┤
Agent D → task-Nd  ─┘
```

### Collision Protocol
- Each task owns specific files — no file is listed in two tasks of the same wave.
- After all parallel agents complete, task-Nz merges branches and runs the full suite.
- If a parallel agent is blocked by another agent's file, mark the dependency and continue with other subtasks.

### Branch Naming
```
remediation-r3/wave-N/task-Na
```

### Commit Style
```
fix(scope): description   — P1/P2 bug fixes
refactor(scope): description  — structural changes
test(scope): description  — test additions
chore(scope): description — CI, deps, cleanup
```

---

## DoD Verification — grep-verifiable per task

After completing all 5 waves, run these sweep commands as the final gate:

```bash
# No broken partial call
grep -rn "partial(funcs\." app/states/oklahoma/funcs/ && echo "FAIL" || echo "PASS"

# No datetime.utcnow()
grep -rn "utcnow" app/ && echo "FAIL" || echo "PASS"

# No bare except:
grep -rn "^\s*except:\s*$" app/ && echo "FAIL" || echo "PASS"

# MONEY_TYPE used everywhere
grep -n "Numeric(15, 2)" app/core/models/tables.py && echo "FAIL" || echo "PASS"

# No full-table .all() in analytics
grep -n "\.all()" app/core/analytics.py 2>/dev/null && echo "FAIL" || echo "PASS"

# unified_database.py under 700 lines
wc -l app/core/unified_database.py | awk '{if ($1 > 700) print "FAIL"; else print "PASS"}'

# Coverage gate matches
grep "fail_under" pyproject.toml ci-tests.yml  # both should show 80

# Hypothesis tests exist
grep -rn "@given" tests/ | wc -l  # should be ≥ 5
```

**Final tag:** `remediation-r3/complete`
**Target Overall:** ≥8.0
