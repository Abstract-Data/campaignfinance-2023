#!/bin/bash
# goal-gate.sh — PreToolUse hook (matcher: Bash, Edit|Write)
# Gates task start on an active GOAL_MODE session marker.
# Only active when ECC_HOOK_PROFILE=strict — exits immediately in minimal/standard.
# campaignfinance project — installed by project-alignment skill v2.1.0.

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="goal-gate"
case "$PROFILE" in
  minimal)  exit 0 ;;   # goal-gate only runs in strict
  standard) exit 0 ;;   # goal-gate only runs in strict
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

INPUT=$(cat)
TOOL=$(echo "$INPUT" | jq -r '.tool_name // empty' 2>/dev/null)
SESSION_MARKER=".claude/goal-session-$(date +%Y%m%d).marker"

# Only gate on write/edit/bash tools, not reads
if [[ "$TOOL" =~ ^(Write|Edit|Bash)$ ]]; then
  if [[ "${GOAL_MODE:-0}" != "1" ]]; then
    if [[ ! -f "$SESSION_MARKER" ]]; then
      echo "BLOCK: GOAL_MODE not active. Run '/goal: <task description>' to activate." >&2
      echo "  This project uses ECC_HOOK_PROFILE=strict — explicit goal activation required before multi-step writes." >&2
      echo "  Or set GOAL_MODE=1 in your environment before starting Claude Code." >&2
      exit 2
    fi
  fi
fi
exit 0
