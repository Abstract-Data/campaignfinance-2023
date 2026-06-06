#!/bin/bash
# router-boundary-check.sh — PostToolUse hook (matcher: Edit|Write) — WARN
# Entry-point / CLI modules should orchestrate, not run raw DB sessions inline.
# (No HTTP API in this project — fires only on entry-point modules.)

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="router-boundary-check"
case "$PROFILE" in
  minimal)  [[ "$HOOK_NAME" != "block-dangerous" && "$HOOK_NAME" != "block-env-writes" ]] && exit 0 ;;
  standard) [[ "$HOOK_NAME" == "post-edit-test" ]] && exit 0 ;;
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
[[ "$FILE" != *.py ]] && exit 0
[[ "$FILE" == *test* ]] && exit 0

if echo "$FILE" | grep -qE '(main|cli|download_and_analyze)\.py$'; then
  if grep -qE '\.get_session\(\)|Session\(' "$FILE" 2>/dev/null; then
    echo "ROUTER BOUNDARY: entry-point module $FILE opens DB sessions directly." >&2
    echo "Delegate persistence to app/funcs/db_loader or app/core loaders; keep entry points thin." >&2
  fi
fi
exit 0
