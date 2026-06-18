# Model: gpt-5.3-codex-high-fast

# Task 4z: Wave 4 Integration — FK Parity Verification

## Phase

Wave 4 — serial integration. Only start **after both 4a and 4b are merged**.

## Branch

`db-bloat/wave-4/task-4z-integration`

## Prerequisite

Tasks 4a and 4b must both be merged.

## Objective

Verify FK parity on a sample state: same FK assignments before/after the
state-scoping changes. Run full pytest.

## Steps

### 1. FK parity check on sample state

Run `tests/test_wave4_fk_parity.py` (created by 4a/4b).

Verify that:
- Same committee IDs assigned to transactions
- Same address IDs assigned
- Same entity IDs assigned
- No new NULLs introduced in FK columns

### 2. Full pytest

```bash
uv run pytest tests app/tests -x --ignore=tests/resolve
```

All tests must pass.

### 3. Commit

```
test: wave-4 FK parity verification (4z)
```

## Tag After Completion

```
db-bloat/wave-4-complete
```

## Checklist

- [ ] Tasks 4a and 4b confirmed merged
- [ ] `tests/test_wave4_fk_parity.py` passes
- [ ] Full pytest passes
- [ ] `gitnexus_detect_changes()` run before commit
- [ ] Tagged `db-bloat/wave-4-complete`
