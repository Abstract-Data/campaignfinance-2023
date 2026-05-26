# Task 1d — Replace inert `__post_init__`; fix duplicate field & `utcnow`

> **Wave 1, parallel. Branch `remediation/wave-1/task-1d-inert-postinit`.**
> Read the pack README, the Refactoring Report (**RF-SMELL-001**, **RF-DEAD-002**)
> and the Code Review Report (**P3-QUAL-005**, **P3-QUAL-002**).

## Context

Seven model classes define `__post_init__` to normalize data (`.strip()`,
`.upper()` on city/state/names). `__post_init__` is a `dataclasses` hook —
SQLModel/Pydantic **never call it**, so the normalization silently never runs.
This is a Critical correctness bug: downstream dedup will mis-match on case and
whitespace.

## Files

- **Modify:** `app/core/unified_sqlmodels.py`
- **Modify:** `app/core/unified_models.py`
- **Create:** `tests/test_model_normalization.py`

## What to implement

- **RF-SMELL-001 / P3-QUAL-005** — Replace each inert `__post_init__` with a
  Pydantic v2 `@model_validator(mode="after")` (or per-field `@field_validator`)
  so the strip/upper normalization actually runs. Locations:
  `unified_sqlmodels.py:172` (`UnifiedAddress`), `:235` (`UnifiedPerson`),
  `:296` (`UnifiedCommittee`); `unified_models.py:50, 74, 116, 154`. Keep the
  normalization logic identical; only change the hook.
- **RF-DEAD-002** — `UnifiedTransactionPerson` declares `state_id` twice on
  consecutive lines (`unified_sqlmodels.py:460-461`). Delete the second.
- **P3-QUAL-002** — Replace every `datetime.utcnow` `default_factory` in
  `unified_sqlmodels.py` (e.g. `:139`, `:159-160`, and all other tables) with
  `lambda: datetime.now(timezone.utc)`. Import `timezone`.
- **Modern typing** — While in `unified_sqlmodels.py` and `unified_models.py`,
  convert any `Optional[X]` → `X | None` and `Union[X, Y]` → `X | Y` (PEP 604
  / Python 3.10+ syntax preferred in this 3.12 codebase). `ruff check
  --select UP007 --fix` handles most of this automatically.
- **`asyncio.run()` in `__init__` check** — Before committing, run
  `grep -n "asyncio\.run(" app/core/unified_sqlmodels.py app/core/unified_models.py`.
  If any hit is inside an `__init__` method, replace it with a deferred
  `async def _async_init()` classmethod that callers `await`; document the
  location in the commit. If not found in your files, note it so the `1z`
  integration task can run the repo-wide search.

## Steps

- [ ] **1** — `tests/test_model_normalization.py`: failing tests, e.g.
  `UnifiedAddress(state=" tx ").state == "TX"`, `UnifiedPerson` name trimming,
  `UnifiedCommittee` normalization — one assertion per former `__post_init__`.
- [ ] **2** — Run; expect fail. **3** — Convert all 7 hooks; fix the dup field;
  swap `utcnow`. **4** — Run tests; pass. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] No `def __post_init__` remains in either file
  (`grep -rn "__post_init__" app/core/unified_sqlmodels.py app/core/unified_models.py` empty).
- [ ] Normalization is proven by passing tests for all 7 former hooks.
- [ ] `UnifiedTransactionPerson` declares `state_id` once.
- [ ] No `datetime.utcnow` remains in `unified_sqlmodels.py`.
- [ ] No `Optional[X]` or `Union[X, Y]` remain in either file (use `X | None` / `X | Y`).
- [ ] `asyncio.run()` check completed and result noted in commit message.

## Collision protocol

You own `unified_sqlmodels.py` and `unified_models.py` for Wave 1. Tasks 1b/1c
own other core files. Do not restructure these modules — that is Waves 2-4;
make only the three changes above.
