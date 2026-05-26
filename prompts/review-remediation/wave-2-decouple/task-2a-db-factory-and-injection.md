# Task 2a — `db_manager` factory + builder session injection

> **Wave 2, parallel. Branch `remediation/wave-2/task-2a-db-factory-and-injection`.**
> Requires Wave 1 merged. Read the pack README, the Code Review Report
> (**P2-ARC-002**) and the Refactoring Report (**RF-SMELL-005**).

## Context

`unified_database.py` instantiates `UnifiedDatabaseManager()` at **import time**,
which runs `SQLModel.metadata.create_all` — importing the module connects to a
DB and runs DDL as a side effect. The resulting global `db_manager` is reached
into deep inside `unified_sqlmodels.py` via in-method imports (a workaround for
a circular dependency), making the builder impossible to unit-test without a
live database.

## Files

- **Modify:** `app/core/unified_database.py`
- **Modify:** `app/core/unified_sqlmodels.py`
- **Create:** `tests/test_db_factory.py`

## What to implement

- **P2-ARC-002** — Replace the import-time global `db_manager`
  (`unified_database.py:1199-1206`) with a `get_db_manager()` factory (lazily
  constructs and caches). Move `SQLModel.metadata.create_all`
  (`unified_database.py:62`) out of `__init__` into an explicit `bootstrap()`
  method. Importing the module must no longer touch a database.
- **RF-SMELL-005** — In `unified_sqlmodels.py`, the `UnifiedSQLModelBuilder`
  lookup helpers (`_find_committee_by_filer_id` `:1258`, `_find_entity` `:1285`,
  `_find_campaign` `:1340`, `_find_address_by_fields` `:1415`) use in-method
  `from app.core.unified_database import db_manager` imports. Change
  `UnifiedSQLModelBuilder.__init__` to accept an injected `session` (or a
  lookup-repository object); the helpers use that session. This removes the
  circular import — the builder no longer imports the database module.
- Update the builder's call sites (`UnifiedSQLDataProcessor`,
  `production_loader.py` if it constructs the builder) to pass a session. If a
  call site is owned by another wave-2 task, coordinate via the integration
  task — but the loader is not a wave-2 task, so update it here if needed.
- Narrow any bare `except Exception` in the helpers you touch (this is part of
  P2-MNT-001 that becomes possible once lookups are injectable).

## Steps

- [ ] **1** — `tests/test_db_factory.py`: failing tests that importing
  `app.core.unified_database` does **not** connect to a DB; `get_db_manager()`
  returns a cached instance; `UnifiedSQLModelBuilder(session=...)` performs a
  lookup against an in-memory SQLite session with no global `db_manager`.
- [ ] **2** — Run; expect fail. **3** — Implement factory + `bootstrap()` +
  builder injection; rewire helpers and call sites. **4** — Run; pass.
  `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] Importing `unified_database` runs no DDL and opens no connection.
- [ ] `get_db_manager()` and `bootstrap()` exist; no import-time global.
- [ ] `UnifiedSQLModelBuilder` takes an injected session; no in-method
  `import db_manager` remains; the circular import is gone.
- [ ] The builder is unit-testable against in-memory SQLite (proven by a test).

## Collision protocol

You own `unified_database.py` and `unified_sqlmodels.py` for Wave 2. Task 2b
owns `unified_models.py`/`unified_integration.py`; task 2c owns logging. No
overlap.
