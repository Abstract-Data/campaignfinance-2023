#!/bin/bash
# block-op-read.sh — PreToolUse hook (matcher: Bash)
# This project uses the 1Password Environments SDK (onepassword-sdk,
# app/op.py -> Client.authenticate / secrets.resolve). Block the `op read`
# CLI so secret resolution stays in the SDK path.
INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if echo "$CMD" | grep -qE 'op read op://|op item get'; then
  echo "BLOCK: use the 1Password SDK (app/op.py -> OnePasswordSettings) instead of the 'op read' CLI." >&2
  echo "See AGENTS.md ## Security & Best Practices -> Secrets Management." >&2
  exit 1
fi
exit 0
