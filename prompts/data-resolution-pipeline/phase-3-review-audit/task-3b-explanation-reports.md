# Task 3b — Match-explanation reports

> **Phase 3, round 1. Parallel-safe with 3a, 3c.** Blocks `task-3z`.
> Read the pack README, the Phase 3 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Every
`match_decision` (and queued `merge_review`) carries an `explanation_json` — the
Splink per-comparison contribution breakdown written by Phase 2 `task-2a`. Raw
JSON is not reviewable. This task renders it into a human-readable explanation
so a reviewer (or an auditor) can see exactly why a pair scored as it did.

Reference: the spec's "Matching engine" (per-comparison breakdown = the audit
trail) and `match_decision` schema.

## Dependencies

- **Depends on:** Phase 2 merged (so `explanation_json` exists and has a known
  shape — inspect a real `match_decision.explanation_json` before coding).
- **Blocks:** `task-3z`.
- **Parallel-safe with:** 3a, 3c.

## Files

- **Create:** `app/resolve/review/explain.py` — the renderer + report.
- **Create:** `tests/resolve/test_explain.py`.

New files only. Do **not** create `app/resolve/review/__init__.py` — `task-3z`
owns it.

## Interface contract

`explain.py` exports:

- `render_explanation(explanation_json: str | dict) -> str` — a readable,
  plain-text "waterfall": one line per comparison field showing the field, the
  observed similarity level, that comparison's weight/contribution, the running
  total, and the final match probability. Tolerates a missing or malformed
  `explanation_json` (returns a clear "no explanation available" line, never
  raises).
- `explanation_table(explanation_json) -> list[dict]` — the same data as rows,
  for programmatic use (the CLI and tests consume this).
- `run_report(session, run_id, *, band=None) -> str` — a multi-decision report:
  for a `match_run`, render the explanations for its decisions (optionally
  filtered to one band), with a summary header (counts per band).

The renderer is **pure** with respect to the JSON (no DB access) so it is
trivially unit-testable; only `run_report` touches the session.

## Steps (TDD)

- [ ] **Step 1** — Inspect a real `match_decision.explanation_json` from a Phase
  2 run to confirm its structure. Write `tests/resolve/test_explain.py`: failing
  tests that `render_explanation` on a sample produces one line per comparison
  field plus a final probability line, and that malformed input yields the
  graceful fallback rather than an exception.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `render_explanation` and `explanation_table`. Run;
  pass. Commit.
- [ ] **Step 4** — Write a failing test for `run_report` over a seeded run with
  a few decisions; implement it. Run; pass; commit.

## Acceptance criteria

- [ ] `render_explanation` turns `explanation_json` into a readable per-field
  waterfall ending in the match probability.
- [ ] Malformed/missing explanations degrade gracefully, never raise.
- [ ] `run_report` produces a per-run, optionally band-filtered explanation
  report with a summary header.
- [ ] The renderer is pure (DB-free); covered by passing tests.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-3/task-3b-explanation-reports`. New files only, under
`app/resolve/review/`. Do not create `review/__init__.py`. `task-3a`'s CLI
imports `render_explanation` from you via a guarded import — keep the function
name and signature exactly as in the contract.
