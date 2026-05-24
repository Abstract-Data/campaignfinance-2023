# Task 2z — Wave 2 integration: absolute-imports migration

> **Wave 2, serial — runs LAST, after 2a–2c merge.**
> Branch `remediation/wave-2/task-2z-integration`. Read the pack README and the
> Code Review Report (**P3-QUAL-004**).

## Context

Legacy modules import `from abcs ...`, `from funcs ...`, `from logger ...`,
which only works because `app/_path_setup.py` injects entries onto `sys.path`.
Some files even mix `from abcs import X` and `from app.logger import Logger` in
one file. This repo-wide import rewrite is done here, serially, because it
touches import lines across many files and would collide with any parallel task.

## Files

- **Modify:** every module using legacy `from abcs/funcs/logger/states ...` imports
- **Delete:** `app/_path_setup.py`

## What to implement (P3-QUAL-004)

1. Find every legacy import:
   `grep -rn "^from abcs\|^from funcs\|^from logger\|^from states\|^import abcs\|^import funcs" app/`.
2. Rewrite each to an absolute `app.*` path (`from abcs import X` →
   `from app.abcs import X`, etc.).
3. Delete `app/_path_setup.py` and remove any import of it / `sys.path`
   manipulation that referenced it.
4. Verify Wave 2's other changes integrate: `get_db_manager()` factory (2a),
   the deleted `unified_models.py` (2b), and the new logging (2c) all import
   cleanly together.

## Steps

- [ ] **1** — Run the grep; rewrite all legacy imports to absolute `app.*`.
- [ ] **2** — Delete `_path_setup.py`; remove its importers.
- [ ] **3** — `python -c "import app.core.unified_database, app.core.unified_sqlmodels"`
  — confirm clean import with no `sys.path` hack.
- [ ] **4** — `uv run pytest tests app/tests` — full suite green. Commit.

## Acceptance criteria

- [ ] No `from abcs`/`from funcs`/`from logger`/`from states` (non-`app.`)
  imports remain (`grep` is empty).
- [ ] `app/_path_setup.py` is deleted; nothing references it.
- [ ] The full test suite is green; Wave 2 backlog items (P2-ARC-002,
  RF-SMELL-005, RF-SMELL-002 dead-layer, P2-OPS-002, P3-QUAL-004) are all done.

## Collision protocol

Cut after 2a–2c merge. Expected to touch many files (the import rewrite) — runs
alone.
