#!/bin/bash
# verify-completion.sh — Stop hook
# Three-gate completion verification. Blocks Claude from declaring "done"
# until all gates pass. campaignfinance project.
INPUT=$(cat)

# CRITICAL: never loop. When stop_hook_active=true, let it stop.
if [ "$(echo "$INPUT" | jq -r '.stop_hook_active')" = "true" ]; then
    exit 0
fi

# GOAL_MODE guard — non-blocking in autonomous /goal runs.
if [[ "${GOAL_MODE:-0}" == "1" ]]; then
    echo "INFO: verify-completion running in GOAL_MODE — findings logged, not blocking" >&2
    BLOCKING=0
else
    BLOCKING=1
fi

fail() {
    echo "$1" >&2
    [ "$BLOCKING" -eq 1 ] && exit 2
}

# Gate 1: TASK.md unchecked items.
if [ -f "TASK.md" ]; then
    UNCHECKED=$(grep -c '^- \[ \]' TASK.md 2>/dev/null || echo 0)
    if [ "$UNCHECKED" -gt 0 ]; then
        fail "FAIL: TASK.md still has $UNCHECKED unchecked item(s). Complete them before declaring done."
    fi
fi

# Gate 2: unit tests must pass.
if [ -f "pyproject.toml" ]; then
    TEST_OUTPUT=$(uv run pytest app/tests -x -q --tb=short 2>&1 | tail -15)
    TEST_EXIT=${PIPESTATUS[0]}
    if [ "$TEST_EXIT" -ne 0 ]; then
        echo "$TEST_OUTPUT" >&2
        fail "FAIL: unit tests failing (app/tests). Fix before declaring done."
    fi
fi

# Gate 3: no ruff errors on changed Python files.
CHANGED_PY=$(git diff --name-only HEAD 2>/dev/null | grep '\.py$' | head -20)
if [ -n "$CHANGED_PY" ]; then
    RUFF_OUTPUT=$(uv run ruff check $CHANGED_PY --quiet 2>&1)
    if [ $? -ne 0 ]; then
        echo "$RUFF_OUTPUT" | head -10 >&2
        fail "FAIL: ruff errors on changed files. Run 'uv run ruff check . --fix' first."
    fi
fi

echo "OK: completion verified — TASK.md clean, tests passing, lint clean."
exit 0
