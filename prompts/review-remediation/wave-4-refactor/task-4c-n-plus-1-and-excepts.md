# Task 4c — Eliminate N+1 queries; narrow exception handling

> **Wave 4, parallel. Branch `remediation/wave-4/task-4c-n-plus-1-and-excepts`.**
> Requires Wave 3 merged. Read the pack README, the Code Review Report
> (**P2-PERF-001**, **P2-MNT-001**) and the Refactoring Report (**RF-CPLX-003**).

## Context

The officer-linking path in `unified_state_loader.py` opens a **new `Session`
per `(tx_person, officer)` pair** (`_person_matches_officer` `:411-413`,
`_create_officer_link` `:429-431`), and the `builders.py` lookup helpers open a
fresh session per record. On a full Texas load this is N+1 connection churn that
exhausts the pool. The same code has 20+ broad `except Exception` handlers that
`ic`-print or return `None`/`False`, silently dropping data-quality defects.

## Files

- **Modify:** `app/core/builders.py` (the Wave-3 split-out builder module)
- **Modify:** `app/core/unified_state_loader.py`
- **Create:** `tests/test_loader_performance.py`

## What to implement

- **P2-PERF-001 / RF-CPLX-003** — Open **one session per file/batch** and thread
  it through the helpers (the builder already takes an injected session after
  Wave 2 task 2a — use it). Before the row loop, **pre-load** committees /
  entities / persons into in-memory dicts keyed by `filer_id` /
  `normalized_name` with a single `SELECT` each; replace the per-row lookups
  (`_find_committee_by_filer_id`, `_find_entity`, the officer-match loop) with
  dict lookups. Flatten the 4-level officer-linking nesting with guard clauses.
- **P2-MNT-001** — Replace every broad `except Exception` in these two files
  with the specific expected types (`SQLAlchemyError`, `ValidationError`,
  `KeyError`), log through the project `Logger` at `warning`/`error`, and let
  unexpected exceptions propagate. Row-level failures are **aggregated into the
  loader's existing `stats` counter**, not silently dropped.

## Steps

- [ ] **1** — `tests/test_loader_performance.py`: failing tests — a multi-row
  load issues a bounded number of `SELECT`s (assert via a query counter / spy),
  not one per row; and a row that fails validation increments a `stats` failure
  counter rather than vanishing.
- [ ] **2** — Run; expect fail. **3** — Implement session-per-batch + pre-loaded
  dict lookups; narrow the excepts; wire the stats counter. **4** — Run; pass.
  Full suite green. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] No `with ... get_session()` (or equivalent) inside a per-row/per-officer
  loop in either file.
- [ ] Lookups go through pre-loaded dicts; query count is bounded per batch.
- [ ] No bare `except Exception` remains in `builders.py` or
  `unified_state_loader.py`; row failures land in `stats`.

## Collision protocol

You own `builders.py` and `unified_state_loader.py` for Wave 4. Task 4d creates
`app/core/value_objects.py` — import and use its `PersonName`/`AddressParts`/
`Officer` types where they tidy these helpers, but do not create that file.
