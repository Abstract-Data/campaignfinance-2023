#!/bin/bash
# sql-injection-check.sh — PreToolUse hook (matcher: Edit|Write) — BLOCKER
# Blocks string-interpolated SQL. Use SQLModel/SQLAlchemy parameterized queries.

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="sql-injection-check"
case "$PROFILE" in
  minimal)  [[ "$HOOK_NAME" != "block-dangerous" && "$HOOK_NAME" != "block-env-writes" ]] && exit 0 ;;
  standard) [[ "$HOOK_NAME" == "post-edit-test" ]] && exit 0 ;;
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty')
CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // .tool_input.new_string // empty')
[[ "$FILE" != *.py ]] && exit 0

# f-string / concat / .format() / % feeding a SQL verb or text()/execute().
if echo "$CONTENT" | grep -qiE '(execute|text)\([[:space:]]*f["'"'"']' \
   || echo "$CONTENT" | grep -qiE 'f["'"'"'].*(SELECT|INSERT|UPDATE|DELETE|DROP|WHERE).*\{' \
   || echo "$CONTENT" | grep -qiE '["'"'"'].*(SELECT|INSERT|UPDATE|DELETE).*["'"'"'][[:space:]]*(\+|%)[[:space:]]'; then
  echo "BLOCKED: possible SQL injection — string-interpolated SQL in $FILE." >&2
  echo "Use SQLModel/SQLAlchemy parameterized queries: select(Model).where(Model.col == value)." >&2
  echo "See AGENTS.md ## Security & Best Practices -> Database Queries." >&2
  exit 2
fi
exit 0
