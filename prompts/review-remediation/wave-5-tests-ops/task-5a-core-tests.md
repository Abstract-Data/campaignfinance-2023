# Task 5a — Tests for the unified core and legacy validators

> **Wave 5, parallel. Branch `remediation/wave-5/task-5a-core-tests`.**
> Requires Wave 4 merged. Read the pack README, the Code Review Report
> (**P2-TEST-001**) and the Developer Assessment Report (risk **R10**).

## Context

CI now runs `tests/` (Wave 1 fixed the path), but the unified core
(`processor.py`, `builders.py`, `unified_database.py`, `unified_state_loader.py`)
and the legacy state validators had almost no direct tests. After Waves 2-4 made
these modules injectable and testable, this task backfills real coverage.

## Files

- **Create:** `tests/core/` test modules (processor, builders, database, loader)
- **Create:** `tests/states/` test modules (Texas + Oklahoma validators)

New test files only.

## What to implement

- **P2-TEST-001 (test part)** — Characterization + unit tests for the unified
  core: `process_record` / `process_record_stream` for one record of **each**
  `TransactionType`; the detail-builder registry; the version-snapshot helper;
  the builder lookup helpers against in-memory SQLite (now possible after the
  Wave 2 session injection); the loader's batch/session behaviour and its
  `stats` failure counter.
- **R10** — Unit / property-based tests (use Hypothesis — already a dependency)
  for the legacy state validators in `app/states/texas/validators/` and
  `app/states/oklahoma/validators/`, including the Base/Table split surfaces
  from Wave 4 task 4e.
- Aim to bring real measured coverage of `app/core/` and `app/states/` above the
  CI `--cov-fail-under` gate so the gate becomes a true signal.

## Steps

- [ ] **1** — Write the core test modules; run them — they should pass against
  the Wave-4 code (this is characterization, not TDD-for-new-code).
- [ ] **2** — Write the validator test modules; run them.
- [ ] **3** — `uv run pytest --cov=app --cov-report=term` — confirm the coverage
  number is real and clears the CI gate. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] Direct tests exist for `processor`, `builders`, `unified_database`,
  `unified_state_loader`, and the state validators.
- [ ] `uv run pytest` is green; measured `app/` coverage clears the CI gate.

## Collision protocol

New test files only. No other wave-5 task writes tests under `tests/core/` or
`tests/states/`.
