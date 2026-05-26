# Task 2b — Retire the dead duplicate model layer

> **Wave 2, parallel. Branch `remediation/wave-2/task-2b-retire-dead-layer`.**
> Requires Wave 1 merged. Read the pack README and the Refactoring Report
> (**RF-SMELL-002**, the `unified_models.py` / `unified_integration.py` part).

## Context

Two parallel "unified model" layers coexist: `unified_sqlmodels.py` is the live
ORM layer; `unified_models.py` (490 lines) is a separate dataclass-style layer
used only by `unified_integration.py`, whose `integrate_with_*` functions are
labelled "Example of integrating". `unified_integration.py:211`'s
`create_unified_database_schema()` returns a hand-written SQL DDL string that has
**drifted** from the SQLModel schema (it lists a non-existent
`unified_persons.transaction_id`). This is drift-prone dead weight.

## Files

- **Delete:** `app/core/unified_models.py`
- **Modify or delete:** `app/core/unified_integration.py`

## What to implement (RF-SMELL-002 part)

1. **Confirm dead.** `grep -rn "unified_models\|unified_integration\|integrate_with_\|create_unified_database_schema" app/ scripts/ tests/`.
   Verify no CLI, loader, or test on a runtime path imports them. (`vulture`
   may help if installed.) Wave 1's task 1d also edited `unified_models.py` for
   the `__post_init__` fix — that is fine; it is being deleted now.
2. **Delete `unified_models.py`.**
3. **`unified_integration.py`:** delete the dead `integrate_with_*` example
   functions and the drifted hand-written `create_unified_database_schema()`
   DDL string. If the whole module is dead, delete it; if a small part is live,
   keep only that and delete the rest.
4. Remove any now-dangling imports/`__init__.py` re-exports of the deleted
   symbols.

## Steps

- [ ] **1** — Run the grep; record results in the commit message proving the
  symbols are unused on runtime paths.
- [ ] **2** — Delete `unified_models.py`; delete the dead parts of
  `unified_integration.py`.
- [ ] **3** — `uv run pytest` — confirm nothing broke. `ruff check --fix`.
  Commit.

## Acceptance criteria

- [ ] `app/core/unified_models.py` no longer exists.
- [ ] No dead `integrate_with_*` / hand-written-DDL code remains.
- [ ] `uv run pytest` is green; no import errors.

## Collision protocol

You own `unified_models.py` and `unified_integration.py`. Task 2a owns
`unified_sqlmodels.py`/`unified_database.py` — if you find a live importer of a
deleted symbol there, flag it for the 2z integration task, do not edit it.
