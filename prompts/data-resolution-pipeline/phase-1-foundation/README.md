# Phase 1 — Foundation + deterministic wins

**Goal:** Stand up the resolution layer and a re-runnable pipeline that performs
the *deterministic* part of resolution — no machine learning yet. When this
phase merges, exact-ish duplicates (same `filer_id`, same standardized
name+address) collapse into canonical records, with a crosswalk, dated name
history, and a full run audit.

**Outcome when done:** `uv run python -m app.resolve run --state texas`
standardizes the source layer, blocks candidates, resolves the deterministic
matches, builds canonical records, and writes the crosswalk — all inside one
audited `match_run`, re-runnably and idempotently.

**Prerequisite:** Phase 0 is merged (the source layer is complete).

See the spec sections "The resolution pipeline" (stages 1–3, 7), "Schema design"
(canonical + resolution layers), and "Error handling and resilience".

## Rounds

Phase 1 runs in **two parallel rounds**, then integration.

**Round 1 — foundations (fully independent, run concurrently):**

| Task | Delivers | Owns |
|------|----------|------|
| `task-1a` | Canonical schema | `app/resolve/models/canonical.py` |
| `task-1b` | Resolution schema (crosswalk + audit + review) | `app/resolve/models/resolution.py` |
| `task-1c` | Standardizers + stage 1 (feature-prep) | `app/resolve/standardize/` package |

**Round 2 — pipeline stages (run concurrently, after round 1 merges):**

| Task | Delivers | Owns |
|------|----------|------|
| `task-1d` | `resolve` CLI + run orchestration + `match_run` lifecycle | `app/resolve/cli.py`, `app/resolve/run.py` |
| `task-1e` | Stage 2 — blocking | `app/resolve/blocking.py` |
| `task-1f` | Stage 3 — deterministic fast-path | `app/resolve/stages/fastpath.py` |
| `task-1g` | Stage 7 — trivial clustering + survivorship + publish | `app/resolve/stages/survivorship.py` |

**Then — `task-1z` integration:** wires the CLI to call stages 1→2→3→7 in
order, runs the deterministic pipeline end-to-end, adds the integration test.

## Why the rounds

Round-2 tasks consume round-1 interfaces (the schema models, the standardizer
function signatures). Those interfaces are fully specified in the round-1 task
files, so a round-2 agent *can* start against the spec'd interface early — but
to keep merges clean, treat round 1 as a gate: round 2 begins once 1a/1b/1c are
merged to the Phase 1 branch.

## Collision-freedom

- Round 1: three disjoint paths — `models/canonical.py`, `models/resolution.py`,
  `standardize/`. No overlap.
- Round 2: four disjoint files — `cli.py`+`run.py`, `blocking.py`,
  `stages/fastpath.py`, `stages/survivorship.py`. No overlap.
- `app/resolve/__init__.py`, `app/resolve/models/__init__.py`, and
  `app/resolve/stages/__init__.py` are created by **`task-1z` only**.
- Staging tables: `task-1c` owns the `resolution_input` table (it produces it);
  `task-1b` owns the crosswalk/audit/review tables. No table is defined twice.

## Verifying the phase

`task-1z` is done when `uv run pytest tests/resolve/` is green and a full
deterministic run on Texas produces canonical rows, crosswalk rows, a completed
`match_run`, and an *idempotent* result (running twice yields an identical
crosswalk).
