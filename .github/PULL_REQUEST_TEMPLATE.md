## Summary

<!-- One paragraph: what changed and why. -->

## Related issue / ticket

<!-- URL, issue number, or N/A. -->

## Changes

<!-- Concrete bullet list of what was added, modified, or removed. -->

-

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor (no behavior change)
- [ ] Breaking change
- [ ] Docs
- [ ] Other

## Testing

<!-- What was tested, how, and against which environment. -->

- [ ] Unit tests (`uv run pytest app/tests`)
- [ ] Integration tests (`uv run pytest tests`)
- [ ] Manual verification

## Checklist

- [ ] `uv run pytest` passes
- [ ] `uv run ruff check .` clean
- [ ] `uv run ruff format --check .` clean
- [ ] New features include tests; field mappings registered in the unified field library
- [ ] No secrets, credentials, or `.env` files committed; no donor/filer PII in code or logs
- [ ] No string-interpolated SQL; parameterized SQLModel queries only
- [ ] Docs updated (AGENTS.md / RUNBOOK.md / docs/adr/) if behavior, ops, or a decision changed
- [ ] No debug code or stray `print()` statements
