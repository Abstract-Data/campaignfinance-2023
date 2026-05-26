# Task 1z — Wave 1 integration

> **Wave 1, serial — runs LAST, after 1a–1h merge.**
> Branch `remediation/wave-1/task-1z-integration`. Read the pack README.

## Context

Tasks 1a–1h fixed correctness, security, and hygiene defects in disjoint files.
This task does the cross-cutting cleanup and verifies the wave.

## What to do

1. **Repo-wide unused-import sweep (RF-DEAD-003).** Run
   `uv run ruff check --fix .` to clear unused and redundant imports across the
   repo (e.g. the function-local re-import in `unified_database.py:281`, unused
   `Generator`/`Decimal`/`json` in `unified_state_loader.py:8`). Review the diff.
2. **Verify the Postgres path is whole.** With `app/states/postgres_config.py`
   now restored (1a) and the guard narrowed (1c), confirm
   `python -c "from app.core.unified_database import db_manager"` no longer
   raises `ModuleNotFoundError`.
3. **Verify no regressions from the deletions.** Confirm nothing imports the
   now-deleted `app/main.py` / `app/funcs/depreciated.py`.
4. **Cross-cutting residue check.** `grep -rn "ic(" app/core/` and
   `grep -rn "except Exception" app/core/unified_state_loader.py app/op.py
   app/core/unified_database.py` — confirm the wave-1 tasks cleared the
   instances in the lines they edited (deeper exception rework is Wave 4; only
   flag anything a wave-1 task clearly missed in code it touched).
5. **Run the full suite** the new CI now runs: `uv run pytest tests app/tests`.

## Steps

- [ ] **1** — `uv run ruff check --fix .`; review and commit the import sweep.
- [ ] **2** — Run the import/guard checks above; fix any gap.
- [ ] **3** — `uv run pytest tests app/tests` — must be green. Commit.
- [ ] **4** — Tag the wave: confirm every Wave 1 backlog row is satisfied
  (P1-OPS-001, P1-SEC-001/002/003, P1-ARC-001, RF-SMELL-001, RF-DEAD-001/002/003,
  P3-QUAL-002/003/005, P2-TEST-001 CI part, R6/R7/R8).

## Acceptance criteria

- [ ] `uv run pytest tests app/tests` is fully green.
- [ ] No `ModuleNotFoundError` on importing the unified DB layer.
- [ ] `ruff check` reports no unused imports repo-wide.
- [ ] Every Wave 1 backlog item is verifiably done.

## Collision protocol

Cut this branch after 1a–1h are merged. This task is expected to touch many
files (the ruff sweep) — that is why it runs alone.
