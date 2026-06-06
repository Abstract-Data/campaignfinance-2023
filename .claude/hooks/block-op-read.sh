#!/bin/bash
# block-op-read.sh — PreToolUse hook (matcher: Bash)
# This project uses the 1Password Environments SDK (onepassword-sdk,
# app/op.py -> Client.authenticate / secrets.resolve). Block the `op read`
# CLI so secret resolution stays in the SDK path.

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="block-op-read"
case "$PROFILE" in
  minimal)  [[ "$HOOK_NAME" != "block-dangerous" && "$HOOK_NAME" != "block-env-writes" ]] && exit 0 ;;
  standard) [[ "$HOOK_NAME" == "post-edit-test" ]] && exit 0 ;;
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

if echo "$CMD" | grep -qE 'op read op://|op item get'; then
  echo "BLOCK: use the 1Password SDK (app/op.py -> OnePasswordSettings) instead of the 'op read' CLI." >&2
  echo "See AGENTS.md ## Security & Best Practices -> Secrets Management." >&2
  exit 1
fi
exit 0
