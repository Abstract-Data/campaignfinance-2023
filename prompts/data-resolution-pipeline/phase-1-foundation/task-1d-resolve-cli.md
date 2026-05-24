# Task 1d — `resolve` CLI + run orchestration + `match_run` lifecycle

> **Phase 1, round 2. Parallel-safe with 1e, 1f, 1g.** Blocks `task-1z`.
> Round 2 begins after 1a/1b/1c are merged. Read the pack README and spec first.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The
resolution pipeline needs a command-line entry point and an orchestrator that
opens a `match_run`, calls the stages in order, and finishes or fails the run
cleanly. This task builds that shell — **not** the stages themselves (those are
1e/1f/1g) and not the stage wiring (that is `task-1z`).

Reference: the spec's "The resolution pipeline" intro and "Error handling and
resilience".

## Dependencies

- **Depends on:** 1a (canonical schema), 1b (resolution schema) merged.
- **Blocks:** `task-1z`.
- **Parallel-safe with:** 1e, 1f, 1g.

## Files

- **Create:** `app/resolve/run.py` — the `ResolutionRun` orchestrator.
- **Create:** `app/resolve/cli.py` — argument parsing / entry point.
- **Create:** `app/resolve/staging.py` — staging-table swap helpers.
- **Create:** `tests/resolve/test_run.py`.

New files only. Do **not** create any `__init__.py` — `task-1z` owns those.

## Interface contract

`ResolutionRun` in `run.py`:

- `ResolutionRun(state_code: str, config: dict)` — constructor.
- `.start(session) -> MatchRun` — inserts a `match_run` row with
  `status="running"`, `config_json` = a snapshot of `config`, `started_at` set.
- `.finish(session, counts: dict)` — sets `status="completed"`, `finished_at`,
  and the counter columns.
- `.fail(session, error: str)` — sets `status="failed"`, `finished_at`; ensures
  no partial canonical write survives (drop this run's staging tables).
- `.run(session, stages: list[Stage])` — calls each stage in order, passing the
  `run_id`; on any exception, calls `.fail()` and re-raises.

A `Stage` is any callable `stage(session, run_id, config) -> dict` (the dict is
merged into the run counts). `task-1z` supplies the concrete stage list; this
task only defines the protocol and orchestration.

`staging.py`: helpers to create per-run staging tables and to **atomically swap**
a finished staging table over the live canonical table (rename-based swap inside
one transaction), plus `drop_run_staging(session, run_id)`.

`cli.py`: `python -m app.resolve run --state texas [--config path]` — builds the
config, opens a session, constructs a `ResolutionRun`, and calls `.run()` with
the stage list (the stage list is injected by `task-1z`; until then, an empty
list is acceptable and the CLI should run a no-op pipeline cleanly).

Determinism: seed any RNG from `config`; `config_json` must fully capture what
the run used.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_run.py`: failing tests that
  `.start()` writes a `running` `match_run`; `.finish()` flips it to `completed`
  with counts; `.fail()` flips it to `failed`.
- [ ] **Step 2** — Run; expect failure. Implement `run.py`. Run; pass. Commit.
- [ ] **Step 3** — Write a failing test that `.run()` with a stage that raises
  leaves the run `failed` and re-raises, and that `.run()` with two trivial
  stages calls them in order and merges their count dicts.
- [ ] **Step 4** — Implement the orchestration + `staging.py`. Run; pass. Commit.
- [ ] **Step 5** — Write a failing test that the atomic swap replaces a live
  table with a staging table in one transaction (no window where the table is
  missing). Implement; run; pass; commit.
- [ ] **Step 6** — Smoke-test `python -m app.resolve run --state texas` with an
  empty stage list — it should open and complete a `match_run` cleanly. Commit.

## Acceptance criteria

- [ ] `match_run` lifecycle (`running` → `completed` / `failed`) works and is
  tested; `config_json` captures the run config.
- [ ] A stage exception fails the run, drops its staging, and re-raises.
- [ ] The staging-table swap is atomic.
- [ ] `python -m app.resolve run --state texas` runs end-to-end with no stages.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1d-resolve-cli`. New files only. Do not implement
the stages or wire a concrete stage list — `task-1z` injects the stages. Do not
create any `__init__.py`.
