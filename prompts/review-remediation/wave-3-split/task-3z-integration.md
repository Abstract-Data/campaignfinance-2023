# Task 3z — Wave 3 integration

> **Wave 3, serial — runs LAST, after 3a–3b merge.**
> Branch `remediation/wave-3/task-3z-integration`. Read the pack README.

## Context

Task 3a split the `unified_sqlmodels.py` god-module into `enums.py`,
`constants.py`, `models/`, `builders.py`, `processor.py`. This task rewires
every remaining importer across the repo and verifies behaviour is unchanged.

## What to do

1. **Rewire all importers.** `grep -rn "unified_sqlmodels" app/ scripts/ tests/`
   — update every import to the new module locations (or confirm the re-export
   shim still serves them; prefer updating to the real modules and then deleting
   the shim).
2. **Wire the shared constants.** `production_loader.py:36-48` holds a duplicate
   record-type `frozenset`; replace it with an import of `RecordType` /
   `RECORD_TYPE_CODES` from `app/core/constants.py` (RF-MAGIC-001). Check for any
   other duplicate record-type literals and point them at the constant.
3. **Verify no behaviour change.** Run `uv run pytest tests app/tests` — the
   suite must pass exactly as before Wave 3 (this wave is restructuring only).
4. Delete the `unified_sqlmodels.py` shim if all importers now use the real
   modules.

## Steps

- [ ] **1** — Rewire all `unified_sqlmodels` importers across `app/`, `scripts/`,
  `tests/`.
- [ ] **2** — Replace the `production_loader.py` record-type frozenset with the
  shared constant.
- [ ] **3** — `uv run pytest tests app/tests` — green, unchanged. Delete the
  shim if unused. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] Every importer uses the new `enums`/`constants`/`models`/`builders`/
  `processor` modules; the shim is gone.
- [ ] The record-type vocabulary is defined once (`constants.py`) and imported
  everywhere (no duplicate frozenset in `production_loader.py`).
- [ ] Full test suite green with no behaviour change.

## Collision protocol

Cut after 3a–3b merge. Expected to touch importers across the repo — runs alone.
