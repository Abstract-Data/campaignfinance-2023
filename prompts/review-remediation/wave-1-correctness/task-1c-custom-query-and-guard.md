# Task 1c — Restrict `run_custom_query`; narrow the `db_manager` guard

> **Wave 1, parallel. Branch `remediation/wave-1/task-1c-custom-query-and-guard`.**
> Read the pack README and the Code Review Report (**P1-SEC-002**, **P1-OPS-001**).

## Context

`app/core/unified_database.py` has two correctness/security defects: an
unrestricted arbitrary-SQL method, and a catch-all that silently hides the
missing-module bug task 1a fixes.

## Files

- **Modify:** `app/core/unified_database.py`
- **Create:** `tests/test_unified_database_guard.py`

## What to implement

- **P1-SEC-002** — `run_custom_query` (`unified_database.py:524-537`) passes a
  caller string straight to `session.exec(text(...))` — any caller can `DROP
  TABLE`. Restrict it: reject statements that do not start with `select` (after
  `lstrip().lower()`), and open the connection read-only
  (`execution_options(postgresql_readonly=True)`). The Code Review Report gives
  the exact recommended body. (If a grep shows nothing in `app/` calls it, you
  may instead delete the method — note which you chose in the commit.)
- **P1-OPS-001 (guard part)** — The module-level `db_manager` singleton
  (`unified_database.py:1199-1206`) is wrapped in `except (RuntimeError,
  Exception)` which silently sets `db_manager = None`, masking real bugs.
  Narrow it so only `RuntimeError` (genuine connection-unavailable) is caught;
  `ModuleNotFoundError`/`ImportError` must propagate loudly.
- While in this file: replace `ic()` calls with the project `Logger` and swap
  `datetime.utcnow()` for `datetime.now(timezone.utc)` if any appear in lines
  you edit (this file's share of P3-QUAL-001/002 — fuller cleanup is Wave 4).

## Steps

- [ ] **1** — `tests/test_unified_database_guard.py`: failing tests that
  `run_custom_query("DROP TABLE x")` raises `ValueError`, a `SELECT` is allowed,
  and a non-`RuntimeError` raised during manager construction propagates.
- [ ] **2** — Run; expect fail. **3** — Implement both changes. **4** — Run;
  pass. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] `run_custom_query` rejects non-SELECT and runs read-only (or is deleted).
- [ ] The singleton guard catches only `RuntimeError`; import errors propagate.

## Collision protocol

You own `unified_database.py` for Wave 1. Task 1a owns the new
`postgres_config.py` — do not create or edit it.
