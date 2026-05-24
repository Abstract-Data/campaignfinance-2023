# Data Resolution Pipeline — Implementation Prompt Pack

> **For agentic workers:** Each `task-*.md` file in a phase directory is a
> complete, self-contained work order for **one agent**. Hand the whole file to
> a fresh agent. Steps use `- [ ]` checkbox syntax for tracking. Recommended
> driver: `superpowers:subagent-driven-development` or `superpowers:executing-plans`.

**Goal:** Build a non-destructive pipeline that cleans the campaign-finance
data, resolves duplicate records, and links every record into one relational
graph — one canonical row per real-world person, organization, committee,
campaign, and address, with every source record crosswalked to it.

**Architecture:** The existing `unified_*` tables become an immutable source
layer. A new resolution layer (canonical tables + crosswalk + audit) is built on
top by a re-runnable, staged pipeline. Deterministic matching handles the
certain cases; Splink handles the fuzzy ones. Nothing in `unified_*` is mutated.

**Tech stack:** Python 3.12, `uv`, SQLModel, Pydantic v2, PostgreSQL; Polars for
feature prep; Splink (Phase 2+); `usaddress` / `usaddress-scourgify` /
`probablepeople` for standardization; pytest + Hypothesis.

**Source spec:** [`docs/superpowers/specs/2026-05-23-data-resolution-pipeline-design.md`](../../docs/superpowers/specs/2026-05-23-data-resolution-pipeline-design.md)
— every agent must read the spec before starting a task.

---

## How to use this pack

1. Pick a phase. Read that phase's `README.md` first — it lists which tasks run
   in parallel, in what rounds, and what the integration task does.
2. Assign each `task-*.md` to its own agent. The file is the agent's entire
   brief: context, files to touch, interface contracts, TDD steps, acceptance
   criteria, and a collision protocol.
3. Tasks in the same **round** have no dependencies on each other — dispatch
   them concurrently.
4. The `task-*z-integration.md` file in each phase is **serial**: it runs only
   after every other task in the phase is merged.

## Phase dependency graph

```
Phase 0  Source-layer completion
  round 1 (parallel):  0a  0b  0c  0d  0e  0f
  then:                0z  integration

Phase 1  Foundation + deterministic wins
  round 1 (parallel):  1a  1b  1c
  round 2 (parallel):  1d  1e  1f  1g
  then:                1z  integration

Phase 2  Probabilistic matching
  round 1 (parallel):  2a  2b  2c  2e
  round 2:             2d
  then:                2z  integration

Phase 3  Review queue + audit
  round 1 (parallel):  3a  3b  3c
  then:                3z  integration

Phase 4  Publish + cross-state hook
  round 1 (parallel):  4a  4b  4c  4d
  then:                4z  integration

Phase 0 ──▶ Phase 1 ──▶ Phase 2 ──▶ Phase 3 ──▶ Phase 4
(each phase depends on the prior phase being merged)
```

Phases run in sequence; **within** a phase, the rounds above run in parallel.

## Parallel-safety rules (collision protocol)

Multiple agents work a phase at once, so tasks must not fight over files.

1. **A parallel task creates only NEW files** — except where its work order
   explicitly names a single shared-file edit that **no other task in the same
   round touches**. Those exceptions are called out per task.
2. **Each task works on its own git branch:** `resolve/phase-<N>/task-<Nx>-<slug>`.
3. **Every new model goes in its own module.** Two agents never edit one model
   file in the same round.
4. **Registry / `__init__.py` / dispatch wiring is done only by the phase
   integration task** (`task-*z`). Parallel tasks export their pieces; the
   integration task wires them together.
5. If a task discovers it must edit a file another parallel task owns: **stop
   and flag it** in the task's notes — do not edit across task boundaries.

## Status of this pack

| Phase | State |
|-------|-------|
| 0 — Source-layer completion | Full work orders |
| 1 — Foundation + deterministic wins | Full work orders |
| 2 — Probabilistic matching | Full work orders |
| 3 — Review queue + audit | Full work orders |
| 4 — Publish + cross-state hook | Full work orders |

All five phases are fully decomposed into per-task work orders. Phases are
implemented in sequence; within each phase, the round-1 tasks run concurrently.

## Conventions

- **TDD throughout:** write the failing test, see it fail, implement, see it
  pass, commit. One green step = one commit. Conventional Commit messages.
- **Tests** live under `tests/resolve/`. Run with `uv run pytest`. `tests/resolve/`
  is a plain directory (no `__init__.py` needed) — the first task to land simply
  creates it; each task adds its own distinctly-named test file, so there is no
  owner to coordinate.
- New pipeline code lives under `app/resolve/`; new source-layer models under
  `app/core/source_models/`.
- Do not edit `CLAUDE.md` or anything under `.claude/` (environment-protected).
- Each task ends merged to the phase branch; the integration task verifies the
  phase end-to-end before the phase is considered done.
