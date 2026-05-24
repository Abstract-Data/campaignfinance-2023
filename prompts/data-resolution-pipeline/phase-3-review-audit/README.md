# Phase 3 — Review queue + audit

**Goal:** Make every merge defensible and reversible. Build the workflow to work
the `merge_review` queue, render human-readable match explanations, and unmerge
a run.

**Outcome when done:** A reviewer can list, inspect, approve, and reject queued
pairs from the CLI; every decision shows the Splink explanation behind it;
decisions are durable across runs; and any run can be cleanly reverted.

**Prerequisite:** Phase 2 merged (probabilistic matching populates the
`merge_review` queue and writes `match_decision.explanation_json`).

Reference: the spec's "Schema design → Resolution layer" (`merge_review`,
`match_decision`), "Error handling and resilience", and the reversibility test
in the Testing strategy.

## Rounds

**Round 1 — three disjoint areas (run concurrently):**

| Task | Delivers | Owns |
|------|----------|------|
| `task-3a` | `merge_review` CLI workflow + durable queue lifecycle | `app/resolve/review/cli.py`, `app/resolve/review/queue.py` |
| `task-3b` | Match-explanation reports (renders the Splink breakdown) | `app/resolve/review/explain.py` |
| `task-3c` | Reversibility tooling — unmerge a run + the reversibility test | `app/resolve/reverse.py`, `tests/resolve/test_reversibility.py` |

**Then — `task-3z` integration:** wires the review CLI as a subcommand and runs
an end-to-end review-then-rerun cycle test.

## The feedback loop

The "approved reviews re-enter the next run" loop is **already realized** by two
existing pieces: Phase 2 `task-2b` (classify) reads decided `merge_review` rows
— treating approved pairs as `auto` edges and never re-queuing rejected ones —
and `task-3a` here makes those decisions durable. No separate feedback task is
needed; `task-3z` verifies the loop with a review→rerun test.

## Collision-freedom

- Round 1: three disjoint paths — `review/cli.py`+`review/queue.py`,
  `review/explain.py`, `reverse.py`. `task-3a` and `task-3b` both add files
  under the new `app/resolve/review/` package but never the same file;
  `review/__init__.py` is created by `task-3z`.
- No round-1 task edits a Phase 1/2 stage file — reversal and review are
  additive. `task-3z` does the subcommand wiring.

## Verifying the phase

`task-3z` is done when `uv run pytest tests/resolve/` is green (including the
reversibility test), the review CLI can approve/reject a queued pair, and a
review→rerun cycle shows an approved pair merged on the following run.
