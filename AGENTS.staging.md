# AGENTS.staging.md
# Version: 1.1.0
# Last Updated: 2026-05-23
# Environment: staging
# Model: claude-sonnet-4-6
# Fallback Model: claude-opus-4-6
# Project: campaignfinance
# Maintainer: John Eakin / Abstract Data

Staging overlay. Inherits everything from `AGENTS.md`; only the differences
below apply when working against the staging environment. The base file remains
the source of truth for standards, commands, and the Notion References block.

## Agent Scope (staging override)

```
Reads:      app/, scripts/, tests/, docs/, .env.example
Writes:     tests/, docs/ only — application code is frozen at staging
Executes:   uv, ruff, pytest, loaders against the STAGING PostgreSQL only
Off-limits: app/ source edits, production PostgreSQL, .env, schema changes
```

Staging is for pre-production validation, not feature development. Code changes
belong on a dev branch reviewed through the base `AGENTS.md` workflow.

## Guardrail tier (staging)

- Run loaders at full data volume against staging before any production load.
- Any schema change, ABC interface change, or unified field-mapping change is
  **out of scope** at staging — escalate to a human.
- Treat a staging row-count anomaly as a release blocker (see
  `docs/GUARDRAILS.md` — Error Classification).

## Promotion

A staging run that completes with clean row counts and passing validation is the
gate for a production load. Production loads are a human-only action.
