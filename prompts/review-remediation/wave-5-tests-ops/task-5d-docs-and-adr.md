# Task 5d — Architecture diagram, legacy docs, data-classification ADR

> **Wave 5, parallel. Branch `remediation/wave-5/task-5d-docs-and-adr`.**
> Requires Wave 4 merged. Read the pack README and the Developer Assessment
> Report (risks **R3**, **R12**).

## Context

Bus factor is 1 and there is no visual architecture/ERD; the legacy core lacks
per-module docs; and there is no documented data-classification/retention
decision for the PII-bearing public-records data.

## Files

- **Create:** `docs/ARCHITECTURE-DIAGRAM.md` (Mermaid) + per-module README/
  docstring additions for the (now-split) `app/core/` modules
- **Create:** `docs/adr/0002-data-classification-and-retention.md`

## What to implement

- **R3** — Add an architecture / ERD diagram. Use Mermaid (renders in the repo):
  a component diagram of the pipeline (download → convert → verify → validate →
  unify → load) and an ERD of the unified schema (post Wave-3 split). Add a
  short module-purpose docstring or per-package `README.md` to each `app/core/`
  module created by the Wave-3 split (`enums`, `constants`, `models`,
  `builders`, `processor`) so the core is navigable without tribal knowledge.
- **R12** — Write ADR `0002-data-classification-and-retention.md` following the
  existing `docs/adr/0001` format: state that the pipeline processes public-
  record campaign-finance data that nonetheless contains PII (donor/filer names,
  addresses, employers), record the decision on minimization / retention /
  deletion (likely "retain as public record, no deletion" — but make it an
  explicit, recorded decision), and cross-reference the `GUARDRAILS.md`
  PII-logging rule.

## Steps

- [ ] **1** — Write `docs/ARCHITECTURE-DIAGRAM.md` with the Mermaid component
  diagram + unified-schema ERD.
- [ ] **2** — Add module-purpose docs to the split `app/core/` modules.
- [ ] **3** — Write `docs/adr/0002-data-classification-and-retention.md`.
- [ ] **4** — Confirm the Mermaid renders; commit.

## Acceptance criteria

- [ ] An architecture diagram + unified-schema ERD exist and render.
- [ ] Each split `app/core/` module has a clear purpose doc.
- [ ] ADR 0002 records the data-classification + retention decision.

## Collision protocol

You own the new doc files + module docstrings in `app/core/`. The Wave-4 tasks
that restructured those modules are merged; adding docstrings to them now is
collision-free. Task 5c owns `docs/DEPLOYMENTS.md` — do not edit it.
