#!/bin/bash
# pre-commit.sh — PreToolUse hook (matcher: Bash, fires on git commit)
# Two gates: (1) block commits containing secrets, (2) warn on code changes
# with no accompanying test changes. campaignfinance project.
INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="pre-commit"
case "$PROFILE" in
  minimal)  exit 0 ;;
  standard) ;;
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

echo "$CMD" | grep -qE "git commit" || exit 0

# Gate 1: secrets scanner (BLOCK).
SECRETS=$(git diff --staged 2>/dev/null \
  | grep -iE '(api_key|secret|token|password|credential|op_service_account_token)[[:space:]]*[:=][[:space:]]*["'"'"']\w{8,}' \
  || true)
if [ -n "$SECRETS" ]; then
  echo "BLOCKED: potential secrets detected in staged changes:" >&2
  echo "$SECRETS" >&2
  echo "Move credentials to .env (gitignored) + pydantic-settings / 1Password SDK." >&2
  exit 2
fi

# Gate 2: tests-required (WARN — exit 2 forces acknowledgement).
CODE_FILES=$(git diff --staged --name-only -- 'app/*.py' 'scripts/*.py' 2>/dev/null | grep -v -E 'test' | wc -l | tr -d ' ')
TEST_FILES=$(git diff --staged --name-only -- '*test_*.py' 'tests/*' 'app/tests/*' 2>/dev/null | wc -l | tr -d ' ')
if [ "${CODE_FILES:-0}" -gt 3 ] && [ "${TEST_FILES:-0}" -eq 0 ]; then
  echo "WARNING: $CODE_FILES source file(s) changed with no test changes." >&2
  echo "Add tests or confirm this is intentional (AGENTS.md PR Requirements)." >&2
  exit 2
fi
exit 0
