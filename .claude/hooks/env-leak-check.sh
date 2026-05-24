#!/bin/bash
# env-leak-check.sh — PostToolUse hook (matcher: Edit|Write) — WARN
# Environment access belongs in pydantic-settings classes (app/op.py),
# not scattered through the codebase.
INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
[[ "$FILE" != *.py ]] && exit 0
[[ "$FILE" == *test* ]] && exit 0
[[ "$(basename "$FILE")" == "op.py" || "$(basename "$FILE")" == "config.py" || "$(basename "$FILE")" == "settings.py" ]] && exit 0

HITS=$(grep -nE 'os\.environ|os\.getenv' "$FILE" 2>/dev/null)
if [ -n "$HITS" ]; then
  echo "ENV LEAK: direct environment access outside settings module: $FILE" >&2
  echo "$HITS" >&2
  echo "Resolve env/secrets via OnePasswordSettings / pydantic-settings (app/op.py)." >&2
fi
exit 0
