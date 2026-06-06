#!/bin/bash
# domain-purity-check.sh — PostToolUse hook (matcher: Edit|Write) — WARN
# Domain/core layers (app/core, app/abcs) model pure data + interfaces.
# Warn when I/O or scraping frameworks leak into them.

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="domain-purity-check"
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

if echo "$FILE" | grep -qE 'app/(core|abcs)/'; then
  HITS=$(grep -nE '^[[:space:]]*(import|from)[[:space:]]+(selenium|requests|psycopg2|bs4|aiohttp)' "$FILE" 2>/dev/null)
  if [ -n "$HITS" ]; then
    echo "DOMAIN PURITY: I/O / scraping framework imported into a pure domain layer: $FILE" >&2
    echo "$HITS" >&2
    echo "Keep scraping/HTTP/DB-driver code in app/states/<state> or app/funcs — see AGENTS.md Architecture." >&2
  fi
fi
exit 0
