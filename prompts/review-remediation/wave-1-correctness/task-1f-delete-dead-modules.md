# Task 1f — Delete dead modules `app/main.py` and `app/funcs/depreciated.py`

> **Wave 1, parallel. Branch `remediation/wave-1/task-1f-delete-dead-modules`.**
> Read the pack README, the Code Review Report (**P1-ARC-001**, **P3-QUAL-003**)
> and the Refactoring Report (**RF-CPLX-002**, **RF-SMELL-006**).

## Context

`app/main.py` (~215 lines) is non-importable dead code — it references undefined
names (`engine`, `filers`, `reports`, `TECCategory`, `select`, `Session`, `ic`,
`itertools`), runs side effects at import, and is **not** the real entry point
(the `cf` console script points elsewhere). `app/funcs/depreciated.py` is a
misspelled dead module. Both mislead contributors.

## Files

- **Delete:** `app/main.py`
- **Delete:** `app/funcs/depreciated.py`

## What to implement (P1-ARC-001, P3-QUAL-003, RF-CPLX-002, RF-SMELL-006)

1. Confirm nothing imports either module: `grep -rn "import main\b\|from app.main\|from app import main\|depreciated" app/ scripts/ tests/`. The reports state both are dead; verify.
2. If a grep hit exists, fix that importer (it is broken already) — note it in the commit.
3. `git rm app/main.py app/funcs/depreciated.py`.
4. If `app/funcs/__init__.py` re-exports anything from `depreciated`, remove that line.

If any genuinely-needed experiment exists in `app/main.py`, the reports say move
it to a `notebooks/` or `scratch/` dir excluded from packaging — but the reports
assess it as ~80% commented scratch, so deletion is expected.

## Steps

- [ ] **1** — Run the grep above; record results in the commit message.
- [ ] **2** — Delete both files; fix any importer found.
- [ ] **3** — `uv run pytest` — confirm nothing broke. Commit.

## Acceptance criteria

- [ ] `app/main.py` and `app/funcs/depreciated.py` no longer exist.
- [ ] `uv run pytest` is still green; no import errors introduced.

## Collision protocol

You own these two deletions. Task 1g strips commented code from
`texas_filers.py` — different file. If you find an importer in a file another
wave-1 task owns, flag it rather than editing that file.
