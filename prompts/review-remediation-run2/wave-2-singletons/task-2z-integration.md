# Task 2z — Wave 2 Integration

**Wave:** 2 — Singletons & Coupling (serial integration)  
**Branch:** `remediation-r3/wave-2/integration`  
**Depends on:** All of 2a, 2b, 2c completed and passing  
**Effort:** ~20 minutes

---

## Steps

### 1. Merge all Wave 2 branches

```bash
git checkout main
git merge remediation-r3/wave-2/task-2a-processor-singleton
git merge remediation-r3/wave-2/task-2b-oklahoma-contribution-split
git merge remediation-r3/wave-2/task-2c-asyncio-fix
```

### 2. Sweep All Wave 2 DoD Checks

```bash
# 2a: No self.builders caching in processor
grep -n "self\.builders" app/core/processor.py && echo "FAIL" || echo "PASS"

# 2b: OklahomaContributionCreate exists with extra='forbid'
python -c "
from app.states.oklahoma.validators.ok_contribution import OklahomaContributionCreate
try:
    OklahomaContributionCreate(unknown_field='x')
    print('FAIL')
except:
    print('PASS — extra=forbid enforced')
"

# 2c: No asyncio.run() inside __init__ in op.py
python -c "
import ast, sys
src = open('app/op.py').read()
tree = ast.parse(src)
for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == '__init__':
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                func = child.func
                if isinstance(func, ast.Attribute) and func.attr == 'run':
                    print('FAIL — asyncio.run in __init__ at line', child.lineno)
                    sys.exit(1)
print('PASS')
"
```

### 3. Full Test Suite

```bash
uv run pytest tests/ -q --tb=short
```

### 4. Tag

```bash
git tag remediation-r3/wave-2-complete
git push origin remediation-r3/wave-2-complete
```

---

## Expected State After Integration

- `get_builder` constructs a fresh `UnifiedSQLModelBuilder` per call — no shared mutable state
- `OklahomaContribution` has four-level hierarchy; `OklahomaContributionCreate` enforces `extra='forbid'`
- `OnePasswordItem` uses async factory pattern — no `asyncio.run()` inside `__init__`
- All tests passing
