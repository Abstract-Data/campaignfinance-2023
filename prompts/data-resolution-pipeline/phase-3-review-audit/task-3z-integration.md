# Task 3z — Phase 3 integration: wire the review CLI, verify the loop

> **Phase 3, serial — runs LAST.** Depends on 3a, 3b, 3c all merged.
> Read the pack README, the Phase 3 README, and the spec.

## Context

Round-1 tasks delivered the review queue, the explanation renderer, and the
reversal tooling in isolation. This task wires them together, exposes the review
CLI as a first-class subcommand, and proves the review feedback loop end-to-end.

## Dependencies

- **Depends on:** 3a, 3b, 3c (all merged to the Phase 3 branch).
- **Blocks:** Phase 4.

## Files

- **Create:** `app/resolve/review/__init__.py` — make `review` a package;
  export `queue`, `cli`, `explain` entry points.
- **Modify:** `app/resolve/cli.py` — add a `review` subcommand that delegates to
  `app.resolve.review.cli`, and an `unmerge` subcommand delegating to
  `app.resolve.reverse`.
- **Create:** `tests/resolve/test_phase3_integration.py`.

## What to build

1. **Package wiring** — `review/__init__.py` so `app.resolve.review` imports
   cleanly.
2. **CLI subcommands** — extend `app/resolve/cli.py`:
   - `python -m app.resolve review list|show|approve|reject ...`
   - `python -m app.resolve unmerge --run N`
3. **Confirm the renderer is wired** — `task-3a`'s `show` command renders
   explanations via `task-3b`'s `render_explanation` (remove any guarded-import
   fallback now that 3b is merged).

## Steps (TDD)

- [ ] **Step 1** — Create `review/__init__.py`; wire the `review` and `unmerge`
  subcommands into `cli.py`. Confirm `python -m app.resolve review list` and
  `python -m app.resolve unmerge --help` run. Commit.
- [ ] **Step 2** — Write `tests/resolve/test_phase3_integration.py`: a failing
  **review→rerun** test — run the pipeline, take a pair sitting in
  `merge_review`, `approve` it via the queue API, run the pipeline again, and
  assert that pair is now an `auto` edge and the two records share one canonical
  entity.
- [ ] **Step 3** — Fix any wiring gaps so the test passes. Commit.
- [ ] **Step 4** — Add an integration test that `show` renders a real
  explanation (not raw JSON) for a queued pair. Commit.
- [ ] **Step 5** — Run the full `tests/resolve/` suite including the
  reversibility test; confirm green. Commit.

## Acceptance criteria

- [ ] `uv run pytest tests/resolve/` is fully green, including the reversibility
  and review→rerun tests.
- [ ] `python -m app.resolve review ...` and `python -m app.resolve unmerge ...`
  work.
- [ ] An approved review causes the pair to merge on the next run; a rejected
  review keeps it apart.
- [ ] `show` displays a rendered explanation.

## Collision protocol

Branch `resolve/phase-3/task-3z-integration`, cut after 3a–3c are merged. This
task is expected to edit shared files (`review/__init__.py`, `cli.py`). If a
round-1 interface gap surfaces, fix it here and note the deviation.
