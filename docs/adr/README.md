# Architecture / AI Decision Records

This directory records **why** significant decisions were made — tool choices,
model selections, and architectural patterns. ADRs capture context that cannot
be recovered from the code itself.

## Rules

- **Append-only.** Once an ADR is `accepted`, it is never edited. A decision
  that changes gets a *new* ADR whose status supersedes the old one; the old
  ADR's status is updated to `superseded by ADR-NNNN` and nothing else.
- **Naming:** `{NNNN}-{slug}.md` — zero-padded sequence number, kebab-case slug
  (e.g. `0001-initial-tool-selection.md`).
- **Status:** `proposed` → `accepted` → `superseded`.

## Template

```markdown
# ADR NNNN: {Title}

**Date:** {YYYY-MM-DD}
**Status:** proposed | accepted | superseded by ADR-NNNN

## Context

What situation prompted this decision? What constraints applied?

## Decision

What was decided, and why this option over the alternatives?

## Consequences

What becomes easier? What becomes harder? What follow-up is required?
```

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-initial-tool-selection.md) | Initial tool & stack selection | accepted |
| [0002](0002-data-classification-and-retention.md) | Data classification and retention | accepted |
| [0003](0003-ai-governance-entity-resolution.md) | AI governance for entity resolution | accepted |
