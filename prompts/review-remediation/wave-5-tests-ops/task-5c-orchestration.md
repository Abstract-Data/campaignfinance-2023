# Task 5c — Production orchestration & runtime entrypoint

> **Wave 5, parallel. Branch `remediation/wave-5/task-5c-orchestration`.**
> Requires Wave 4 merged. Read the pack README and the Developer Assessment
> Report (risks **R9**, **R11**).

## Context

The pipeline runs only via the `cf` CLI or a loader script invoked by hand —
there is no scheduler and no production runtime entrypoint, so it cannot "run on
a schedule." This task adds that layer.

## Files

- **Create:** `app/orchestration/` package — a pipeline runner + schedule
  definition
- **Create:** `tests/test_orchestration.py`
- **Modify:** `docs/DEPLOYMENTS.md` (document how to run the scheduled pipeline)

## What to implement

- **R9** — Add a production runtime entrypoint that runs the full pipeline
  end-to-end (download → convert → verify → load) as one invocation, with
  structured logging, non-zero exit on failure, and idempotency. Provide a
  scheduler integration — choose the lightest fit and document the choice: a
  documented cron invocation, or a Prefect/Airflow flow definition. Keep the
  scheduler optional/pluggable; the entrypoint itself must be runnable
  standalone.
- **R11** — Ensure the orchestrated run uses the **streaming** processing path
  from Wave 4 (`process_record_stream`, batched Polars reads) so a full
  multi-state run is memory-bounded — no legacy `process_records` whole-file
  materialization on the production path.
- Document the new entrypoint and schedule in `docs/DEPLOYMENTS.md`.

## Steps

- [ ] **1** — `tests/test_orchestration.py`: failing tests that the runner
  executes the stages in order, returns a non-zero status on a stage failure,
  and is idempotent (a second run does not double-load).
- [ ] **2** — Run; expect fail. **3** — Implement `app/orchestration/`, wire the
  streaming path, add the schedule definition. **4** — Run; pass. Update
  `docs/DEPLOYMENTS.md`. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] A single production entrypoint runs download→convert→verify→load with
  structured logging and correct exit codes.
- [ ] A scheduler integration exists and is documented.
- [ ] The orchestrated run uses the streaming/batched path (memory-bounded).

## Collision protocol

New `app/orchestration/` package + one test file. You also edit
`docs/DEPLOYMENTS.md`; task 5d edits other docs (`docs/adr/`, architecture
diagram) — different files, no collision.
