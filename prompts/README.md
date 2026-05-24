# Prompts

Version-controlled home for agent system prompts used by this project.

This project's specialized agent **definitions** live in `.claude/agents/`
(Claude Code subagents). This directory holds standalone, versioned **system
prompts** — for example, a prompt used by a scheduled loader-monitoring agent or
any prompt invoked outside Claude Code. Add one subdirectory per such agent.

## Layout

```
prompts/
  README.md              # this file — the version registry
  {agent-name}/
    current.md           # active prompt — a copy of the latest versioned snapshot
    v{MAJOR.MINOR.PATCH}.md   # immutable versioned snapshot (REQUIRED)
    .gitkeep
```

## Versioning workflow

Prompts are versioned artifacts. They are never edited in place.

1. Copy `current.md` to a new `v{X.Y.Z}.md` snapshot before changing it.
2. Edit the new snapshot.
3. Copy the finished snapshot back over `current.md`.
4. Record the bump in the registry below.

Each prompt file starts with this header:

```
# {agent-name} — System Prompt
# Version: {MAJOR.MINOR.PATCH}
# Model: {exact model string}
# Last Updated: {YYYY-MM-DD}
# Maintainer: {name}
```

Semver: **MAJOR** = behavior change / model swap, **MINOR** = new tool or
guardrail, **PATCH** = wording or token optimization. Prompts follow the same
dev → staging → prod promotion path as `AGENTS.md`.

`prompts/` is **never** gitignored.

## Version registry

_No standalone agent prompts versioned yet. Add a `##` section per agent here
(current version, model, last updated) when the first one is created._
