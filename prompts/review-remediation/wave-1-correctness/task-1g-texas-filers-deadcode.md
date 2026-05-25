# Task 1g — Strip commented-out dead code from validators

> **Wave 1, parallel. Branch `remediation/wave-1/task-1g-texas-filers-deadcode`.**
> Read the pack README and the Refactoring Report (**RF-DEAD-001**).

## Context

Two validator files carry large commented-out code blocks that belong in
version-control history, not the working tree:

- `app/states/texas/validators/texas_filers.py` — ~333 lines of commented-out
  field definitions (around `:282-558`).
- `app/states/oklahoma/validators/ok_expenditure.py` — an entire class body is
  commented out (exact lines: read the file to confirm, ~40–80 lines).

Both obscure live code and trigger false-positive code-smell scans.

## Files

- **Modify:** `app/states/texas/validators/texas_filers.py`
- **Modify:** `app/states/oklahoma/validators/ok_expenditure.py`

## What to implement (RF-DEAD-001)

### `texas_filers.py`

1. Identify every commented-out *code* block (not real explanatory docstrings or
   comments) — the bulk is the ~333-line region near `:282-558`.
2. Delete those lines. Keep genuine docstrings and short explanatory comments
   that describe live code.
3. Run `ruff check --fix` to clear any unused imports exposed by the deletion.

### `ok_expenditure.py`

1. Locate the commented-out class body.
2. Delete it. If the class itself is dead, note it in the commit message but do
   **not** delete the class file here — flag to the wave-1 integration task to
   decide if the whole module should be removed (it may be a stub in progress).
3. Run `ruff check --fix` on the file.

Do **not** restructure these validators — the address-extraction mixin
(RF-DRY-003/RF-DRY-004) is Wave 3 and the Base/Table split (P2-ARC-001) is
Wave 4.

## Steps

- [ ] **1** — Read both files; mark all commented-out code regions.
- [ ] **2** — Delete them. Run `uv run ruff check --fix` on each file.
- [ ] **3** — `uv run pytest` for any Texas/Oklahoma-validator tests; confirm
  green. Commit.

## Acceptance criteria

- [ ] No commented-out code blocks remain in `texas_filers.py` (only real docs).
- [ ] The commented-out class body is gone from `ok_expenditure.py`.
- [ ] Both files still import and existing tests pass.

## Collision protocol

You own `texas_filers.py` and `ok_expenditure.py` for Wave 1. No other wave-1
task touches either file.
