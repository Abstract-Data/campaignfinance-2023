# Task 5z — Wave 5 Integration + Final Gate

**Wave:** 5 — Quality & Infrastructure (serial integration)  
**Branch:** `remediation-r3/wave-5/integration`  
**Depends on:** All of 5a, 5b, 5c, 5d completed and passing  
**Effort:** ~45 minutes

---

## Steps

### 1. Merge all Wave 5 branches

```bash
git checkout main
git merge remediation-r3/wave-5/task-5a-validator-dry
git merge remediation-r3/wave-5/task-5b-code-smells
git merge remediation-r3/wave-5/task-5c-property-tests-coverage
git merge remediation-r3/wave-5/task-5d-infra
```

### 2. Sweep All Wave 5 DoD Checks

```bash
# 5a: INDIVIDUAL/ENTITY mixin exists
grep -n "def validate_individual_entity_discriminator" \
  app/states/texas/validators/_mixins.py && echo "PASS" || echo "FAIL"

# 5a: No duplicate clear_blank_strings in TECContributionBase
grep -n "def clear_blank_strings" app/states/texas/validators/texas_contributions.py \
  && echo "FAIL" || echo "PASS"

# 5a: FileReader.read() dispatch method
grep -rn "def read\b" app/ | grep -i "file_reader\|filereader" && echo "PASS" || echo "FAIL"

# 5a: No inline suffix dispatch blocks
grep -n "\.suffix.*parquet" app/core/unified_database.py app/core/unified_state_loader.py \
  && echo "FAIL" || echo "PASS"

# 5b: Fuzzy match logs at DEBUG
grep -n "logger.debug" app/core/builders.py | grep -i "fuzzy\|explicit" \
  && echo "PASS" || echo "FAIL"

# 5b: No StopIteration catch in for-loops
grep -n "StopIteration" app/funcs/db_loader.py && echo "FAIL" || echo "PASS"

# 5c: Hypothesis tests exist
grep -rn "@given" tests/ | wc -l  # ≥ 8

# 5c: Coverage gate at 80%
grep "fail_under" pyproject.toml | grep "80" && echo "PASS" || echo "FAIL"

# 5d: Dockerfile exists
ls Dockerfile docker-compose.yml && echo "PASS" || echo "FAIL"

# 5d: Splink pinned
grep "splink" pyproject.toml | grep "<" && echo "PASS" || echo "FAIL"
```

### 3. Final Full Suite at 80% Coverage Gate

```bash
uv run pytest tests/ --cov=app --cov-fail-under=80 -q --tb=short
```

All tests must pass and coverage must be ≥ 80%.

### 4. All-Waves Final Sweep

Run the master DoD verification block from `README.md`:

```bash
# No broken partial call
grep -rn "partial(funcs\." app/states/oklahoma/funcs/ 2>/dev/null && echo "FAIL" || echo "PASS"

# No datetime.utcnow()
grep -rn "utcnow" app/ && echo "FAIL" || echo "PASS"

# No bare except:
grep -rn "^\s*except:\s*$" app/ && echo "FAIL" || echo "PASS"

# MONEY_TYPE used everywhere in tables.py
grep -n "Numeric(15, 2)" app/core/models/tables.py && echo "FAIL" || echo "PASS"

# No full-table .all() in analytics
grep -n "\.all()" app/core/analytics.py 2>/dev/null && echo "FAIL" || echo "PASS"

# unified_database.py under 700 lines (should be < 300 after wave 3)
wc -l app/core/unified_database.py | awk '{if ($1 > 700) print "FAIL: " $1; else print "PASS"}'

# Coverage gate at 80%
grep "fail_under" pyproject.toml | grep "80" && echo "PASS" || echo "FAIL"
grep "cov-fail-under=80" .github/workflows/ci-tests.yml && echo "PASS" || echo "FAIL"

# Hypothesis tests ≥ 8
count=$(grep -rn "@given" tests/ | wc -l)
[ "$count" -ge 8 ] && echo "PASS: $count @given tests" || echo "FAIL: only $count @given tests"
```

### 5. Tag Final Release

```bash
git tag remediation-r3/wave-5-complete
git tag remediation-r3/complete
git push origin remediation-r3/wave-5-complete remediation-r3/complete
```

---

## Target Scores After Full Remediation

| Dimension | Baseline (May 25) | Target |
|-----------|------------------|--------|
| Code Quality | 7 | ≥8 |
| Architecture | 8 | ≥9 |
| Performance | 5 | ≥7 |
| Security | 8 | ≥9 |
| Testing | 7 | ≥8 |
| Maintainability | 7 | ≥8 |
| Scalability | 5 | ≥7 |
| **Overall** | **6.8** | **≥8.0** |

**Key drivers:**
- Architecture ≥9: god class split (3c) + LoadContext (4c) + injection (4d) eliminate the 3 remaining architectural smells
- Performance ≥7: address cache (4a) + commit-per-link fix (4b) are the largest scalability wins
- Security ≥9: OklahomaContribution four-level split (2b) closes the last `extra='ignore'` ingestion gap
- Testing ≥8: Hypothesis tests (5c) + 80% gate
- Overall ≥8: Security×1.5 + Architecture×1.5 drive the weighted average up
