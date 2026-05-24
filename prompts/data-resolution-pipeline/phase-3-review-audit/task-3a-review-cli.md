# Task 3a — `merge_review` CLI workflow + durable queue lifecycle

> **Phase 3, round 1. Parallel-safe with 3b, 3c.** Blocks `task-3z`.
> Read the pack README, the Phase 3 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Phase 2's
classify stage routes medium-confidence pairs into the `merge_review` table.
This task builds the workflow a human uses to clear that queue: list, inspect,
approve, reject — with decisions that are durable across runs.

Reference: the spec's `merge_review` schema and the `merge_review` band policy.

## Dependencies

- **Depends on:** Phase 2 merged.
- **Blocks:** `task-3z`.
- **Parallel-safe with:** 3b, 3c.

## Files

- **Create:** `app/resolve/review/queue.py` — queue lifecycle functions.
- **Create:** `app/resolve/review/cli.py` — the reviewer CLI.
- **Create:** `tests/resolve/test_review_queue.py`.

New files only. Do **not** create `app/resolve/review/__init__.py` — `task-3z`
owns it.

## Interface contract

`queue.py` exports:

- `list_pending(session, *, run_id=None, entity_type=None, limit=None) -> list[MergeReview]`
  — pending rows, optionally filtered, ordered by score descending.
- `get_review(session, review_id) -> MergeReview` — one row with its pair.
- `approve(session, review_id, *, reviewer, notes="") -> MergeReview` — sets
  `status="approved"`, `reviewer`, `decided_at`, `notes`.
- `reject(session, review_id, *, reviewer, notes="") -> MergeReview` — sets
  `status="rejected"` and the same audit fields.
- A decided row (`approved`/`rejected`) is **immutable** — `approve`/`reject`
  on an already-decided row raises rather than silently flipping it.

`cli.py` provides `python -m app.resolve.review`:

- `list [--run N] [--type person] [--limit N]` — tabular pending queue.
- `show <review_id>` — the pair side by side (call `task-3b`'s
  `render_explanation` if available; degrade to raw `explanation_json` if 3b is
  not yet merged).
- `approve <review_id> --reviewer NAME [--notes ...]`
- `reject  <review_id> --reviewer NAME [--notes ...]`

Durability: decided rows are never re-queued. Phase 2 `task-2b` (classify)
already reads decided rows on the next run — this task only has to make the
decisions stick and stay queryable.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_review_queue.py`: failing tests
  that `list_pending` returns only `pending` rows ordered by score; `approve`
  flips status and stamps `reviewer`/`decided_at`; `reject` likewise;
  approving an already-decided row raises.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `queue.py`. Run; pass. Commit.
- [ ] **Step 4** — Write a CLI smoke test (invoke `list` and `approve` against a
  seeded DB). Implement `cli.py`. Run; pass. Commit.
- [ ] **Step 5** — Add a test that a decided row is excluded from a later
  `list_pending`. Run; pass; commit.

## Acceptance criteria

- [ ] `list_pending` / `get_review` / `approve` / `reject` behave per contract;
  decided rows are immutable and never re-listed.
- [ ] The CLI supports `list` / `show` / `approve` / `reject`.
- [ ] `show` uses `task-3b`'s renderer when present, degrades gracefully if not.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-3/task-3a-review-cli`. New files only, under
`app/resolve/review/`. Do not create `review/__init__.py`. Reference
`task-3b`'s `render_explanation` via a guarded import so this task is
committable independently.
