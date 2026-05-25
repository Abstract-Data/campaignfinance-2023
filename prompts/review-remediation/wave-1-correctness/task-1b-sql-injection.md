# Task 1b — Fix SQL injection in `unified_state_loader.py`

> **Wave 1, parallel. Branch `remediation/wave-1/task-1b-sql-injection`.**
> Read the pack README, the Code Review Report (**P1-SEC-001**) and the
> Refactoring Report (**RF-ARCH-001**).

## Context

`app/core/unified_state_loader.py` builds raw SQL with f-string interpolation of
runtime values — a live SQL-injection vector (`officer['role']` and name strings
come from ingested filing data). One site is also a guaranteed runtime crash.

## Files

- **Modify:** `app/core/unified_state_loader.py`
- **Create:** `tests/test_unified_state_loader_sql.py`

## What to implement

- **P1-SEC-001 / RF-ARCH-001** — Replace the f-string `text()` queries at
  `unified_state_loader.py:394`, `:433`, `:439`, and `:491` with parameterized
  queries. Prefer the native SQLModel `select()`/`update()` API already used
  elsewhere in the file; where raw `text()` stays, use bound params
  (`:name` placeholders + a params dict). **Line 491 is additionally a bug** —
  `session.exec()` is passed a bare string, not `text()`/`select()`; it crashes
  at runtime. The reports give exact before/after snippets.
- While in this file: replace any `ic()` calls with the project `Logger`, and
  narrow any bare `except Exception` you touch to specific types
  (`SQLAlchemyError`, `KeyError`) — these are this file's share of P3-QUAL-001 /
  P2-MNT-001. (The deeper exception/N+1 rework of this file is Wave 4 — only fix
  what sits in the lines you are already editing.)

## Steps

- [ ] **1** — Write `tests/test_unified_state_loader_sql.py`: failing tests that
  officer-linking handles a name containing an apostrophe (`O'Brien`) and a
  `role` containing `'` without error, and that the line-491 path executes.
- [ ] **2** — Run the tests; expect fail (or error on the bare-string bug).
- [ ] **3** — Convert all four queries to parameterized form; fix line 491.
- [ ] **4** — Run tests; expect pass. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] Zero f-string-interpolated SQL remains in `unified_state_loader.py`
  (`grep -n "text(f\"" app/core/unified_state_loader.py` is empty).
- [ ] Line 491 no longer passes a bare string to `session.exec`.
- [ ] Apostrophe-in-name / apostrophe-in-role tests pass.

## Collision protocol

You own `unified_state_loader.py` for Wave 1. No other wave-1 task touches it.
