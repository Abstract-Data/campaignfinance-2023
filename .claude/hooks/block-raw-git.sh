#!/bin/bash
# block-raw-git.sh — PreToolUse hook (matcher: Bash)
# This project uses GitButler for virtual-branch management. Raw git branch
# operations (checkout / switch / branch / merge) desync GitButler's virtual
# branches — route them through the `but` CLI instead.
# campaignfinance project — installed by project-alignment skill.
INPUT=$(cat)
CMD=$(echo "$INPUT" | jq -r '.tool_input.command // empty')

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="block-raw-git"
case "$PROFILE" in
  minimal)  [[ "$HOOK_NAME" != "block-dangerous" && "$HOOK_NAME" != "block-env-writes" ]] && exit 0 ;;
  standard) [[ "$HOOK_NAME" == "post-edit-test" ]] && exit 0 ;;
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

BLOCK=0

# checkout <branch> / checkout -b / switch / merge — always a branch-context op.
# Allows `git checkout -- <file>` and `git checkout -f` (file restore, not branch).
if echo "$CMD" | grep -qE 'git[[:space:]]+(checkout[[:space:]]+-b|checkout[[:space:]]+[A-Za-z0-9_./]|switch[[:space:]]|merge[[:space:]])'; then
  BLOCK=1
fi

# `git branch` create/delete/move/copy/upstream — but allow read-only inspection
# (`git branch`, `-a`, `-r`, `-v`, `--list`, `--show-current`, `--merged`, ...).
if echo "$CMD" | grep -qE 'git[[:space:]]+branch[[:space:]]+(-[dDmMcCu]|--(delete|move|copy|set-upstream|unset-upstream|edit-description)|[A-Za-z0-9_./])'; then
  BLOCK=1
fi

if [ "$BLOCK" = "1" ]; then
  echo "BLOCK: this project uses GitButler — use the 'but' CLI for branch operations, not raw git." >&2
  echo "  Create:  but branch create <name>" >&2
  echo "  Switch:  but branch switch <name>" >&2
  echo "  Push:    but branch push <name>" >&2
  echo "  See docs/GITBUTLER.md and AGENTS.md ## GitButler." >&2
  exit 2
fi
exit 0
