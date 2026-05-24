#!/bin/bash
# block-env-writes.sh — PreToolUse hook (matcher: Edit|Write)
# Blocks writes to secret/credential files; warns on guarded config files.
# campaignfinance project — installed by project-alignment skill.
INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // .tool_input.path // empty')

# --- ECC_HOOK_PROFILE guard ---------------------------------------------
PROFILE="${ECC_HOOK_PROFILE:-standard}"
HOOK_NAME="block-env-writes"
case "$PROFILE" in
  minimal)  [[ "$HOOK_NAME" != "block-dangerous" && "$HOOK_NAME" != "block-env-writes" ]] && exit 0 ;;
  standard) [[ "$HOOK_NAME" == "post-edit-test" ]] && exit 0 ;;
  strict)   ;;
esac
echo "${ECC_DISABLED_HOOKS:-}" | grep -q "$HOOK_NAME" && exit 0
# ------------------------------------------------------------------------

[ -z "$FILE" ] && exit 0
BASENAME=$(basename "$FILE")

# Hard-block: never agent-edit these. .env.example is intentionally allowed.
if [[ "$BASENAME" == ".env" || "$FILE" == *.env || "$BASENAME" == *.pem || "$BASENAME" == *.key \
   || "$FILE" == *secrets/* || "$FILE" == */.git/* || "$BASENAME" == "uv.lock" ]]; then
  if [[ "$BASENAME" != ".env.example" ]]; then
    echo "BLOCKED: '$FILE' is protected (secrets/credentials/lockfile). Get human approval before editing." >&2
    exit 2
  fi
fi

# Warn-only: guarded config files.
for pattern in "pyproject.toml" "Dockerfile" ".pre-commit-config.yaml" ".ruff.toml"; do
  if [[ "$BASENAME" == "$pattern" ]]; then
    echo "CAUTION: modifying guarded config file: $FILE. Verify this change is intentional." >&2
  fi
done
exit 0
