#!/usr/bin/env bash
# pre-pr-review-gate.sh — PreToolUse hook
# matcher: gh pr create|gt submit|gt stack submit|but pr new
# Blocks PR creation until code-reviewer has run and written a receipt.
# Receipt TTL: 8 hours. Source: project-alignment skill v2.4.0 §4.7.16

set -euo pipefail

RECEIPT=".claude/code-reviewer-receipt.json"

if [ ! -f "$RECEIPT" ]; then
  TOOL=$(echo "${INPUT:-}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('tool_name',''))" 2>/dev/null || echo "unknown")
  cat >&2 <<EOF
❌ Pre-PR gate: code-reviewer receipt not found.

Invoke code-reviewer before creating a PR:
  /use agent:code-reviewer

The subagent writes .claude/code-reviewer-receipt.json on completion.
Blocked command: ${TOOL}
EOF
  exit 2
fi

RESULT=$(python3 - <<'PYEOF'
import json, sys, time
try:
    d = json.load(open(".claude/code-reviewer-receipt.json"))
    age = time.time() - d.get("completed_at_unix", 0)
    verdict = d.get("verdict", "UNKNOWN")
    branch = d.get("branch", "unknown")
    if age > 28800:
        print(f"STALE|{branch}|{verdict}")
    else:
        print(f"FRESH|{branch}|{verdict}")
except Exception as e:
    print(f"INVALID|unknown|ERROR: {e}")
PYEOF
)

STATUS=$(echo "$RESULT" | cut -d'|' -f1)
BRANCH=$(echo "$RESULT" | cut -d'|' -f2)
VERDICT=$(echo "$RESULT" | cut -d'|' -f3)

if [ "$STATUS" = "STALE" ]; then
  cat >&2 <<EOF
❌ Pre-PR gate: code-reviewer receipt is stale (>8 hours old).
  Branch reviewed: ${BRANCH}
  Prior verdict:   ${VERDICT}

Re-run code-reviewer for this session:
  /use agent:code-reviewer
EOF
  exit 2
fi

if [ "$STATUS" = "INVALID" ]; then
  cat >&2 <<EOF
❌ Pre-PR gate: code-reviewer receipt is malformed (${VERDICT}).
Re-run code-reviewer:
  /use agent:code-reviewer
EOF
  exit 2
fi

cat >&2 <<EOF
✅ Pre-PR gate: code-reviewer receipt valid.
  Branch: ${BRANCH}
  Verdict: ${VERDICT}
Proceeding with PR creation.
EOF
exit 0
