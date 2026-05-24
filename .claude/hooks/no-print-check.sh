#!/bin/bash
# no-print-check.sh — PostToolUse hook (matcher: Edit|Write) — WARN
# Production code under app/ must log via the Logger class, not print().
INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
[[ "$FILE" != *.py ]] && exit 0
[[ "$FILE" == *test* || "$FILE" == scripts/* || "$FILE" == */live_display.py ]] && exit 0

if echo "$FILE" | grep -qE '^app/|/app/'; then
  HITS=$(grep -nE '^[[:space:]]*print\(' "$FILE" 2>/dev/null)
  if [ -n "$HITS" ]; then
    echo "NO PRINT: print() found in production code: $FILE" >&2
    echo "$HITS" >&2
    echo "Use the Logger class (app/logger.py) — see AGENTS.md NEVER DO." >&2
  fi
fi
exit 0
