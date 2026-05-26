# Task 3a — Split the `unified_sqlmodels.py` god-module

> **Wave 3, parallel. Branch `remediation/wave-3/task-3a-split-god-module`.**
> Requires Wave 2 merged. Read the pack README and the Refactoring Report
> (**RF-SMELL-002**, **RF-MAGIC-001**, **RF-MAGIC-002**).

## Context

`app/core/unified_sqlmodels.py` is 1,856 lines — a god-module holding ~22 table
classes, enums, `UnifiedSQLModelBuilder`, and `UnifiedSQLDataProcessor`. It must
be split so later waves can refactor its parts in parallel. This is a
**mechanical move-and-reimport** task — no behaviour change.

## Files

- **Create:** `app/core/models/` package (`__init__.py` + table modules),
  `app/core/enums.py`, `app/core/builders.py`, `app/core/processor.py`,
  `app/core/constants.py`
- **Modify:** `app/core/unified_sqlmodels.py` (reduce to a thin re-export shim,
  or delete after rewiring), and update imports across `app/` and `scripts/`
  that referenced it.

## What to implement

- **RF-SMELL-002 (split part)** — Move code out of `unified_sqlmodels.py`:
  enums → `app/core/enums.py`; the table classes → `app/core/models/` (grouped
  sensibly, each file < 600 lines); `UnifiedSQLModelBuilder` → `builders.py`;
  `UnifiedSQLDataProcessor` → `processor.py`. To avoid breaking every importer
  at once, leave `unified_sqlmodels.py` as a thin shim that re-exports the moved
  names (or update all importers and delete it — your call; note which).
- **RF-MAGIC-001** — Create a `RecordType` enum (or `RECORD_TYPE_CODES`
  constant) for `RCPT/EXPN/LOAN/PLDG/DEBT/CRED/TRVL/ASSET` and a
  `PLACEHOLDER_NAMES` frozenset (`"NON-ITEMIZED CONTRIBUTOR"`, `"UNKNOWN"`,
  `"ANONYMOUS"`, …) in `app/core/constants.py`; replace the bare-string copies
  in `unified_sqlmodels.py:1546-1555` and `:1066`. (The duplicate in
  `production_loader.py:36-48` is rewired by 3z.)
- **RF-MAGIC-002** — Add `AMOUNT_BUCKETS` and a reusable `MONEY_TYPE`
  (`Numeric(15,2)`) to `constants.py`; use `MONEY_TYPE` in the moved table
  modules instead of repeating the literal.
- **RF-MAGIC-003** — Locate the hardcoded `"TX"` default state code in the
  multi-state survivorship stage (search `grep -rn '"TX"' app/resolve/` and
  `app/core/`). Add a `DEFAULT_STATE: str | None = None` sentinel to
  `constants.py`; update the survivorship stage to require an explicit
  caller-supplied state rather than silently defaulting to Texas. This is a
  correctness risk for any non-Texas ingestion run.

This is a pure restructuring — do **not** change builder/processor logic
(Wave 4 does that). Verify behaviour is identical: the test suite must pass
unchanged.

## Steps

- [ ] **1** — Create `constants.py` and `enums.py`; move enums + add the
  `RecordType`/`PLACEHOLDER_NAMES`/`AMOUNT_BUCKETS`/`MONEY_TYPE` constants.
- [ ] **2** — Create `models/`, move table classes; `builders.py` / `processor.py`,
  move the two classes. Keep a re-export shim at `unified_sqlmodels.py`.
- [ ] **3** — `uv run pytest tests app/tests` — must pass **unchanged**
  (behaviour-preserving). `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] No module exceeds ~600 lines; `unified_sqlmodels.py` is a shim or gone.
- [ ] `enums.py`, `constants.py`, `models/`, `builders.py`, `processor.py` exist.
- [ ] `RecordType`/`PLACEHOLDER_NAMES`/`AMOUNT_BUCKETS`/`MONEY_TYPE` are defined
  once and used in the moved code.
- [ ] `DEFAULT_STATE` sentinel exists in `constants.py`; survivorship stage no
  longer has a hardcoded `"TX"` default (grep clean).
- [ ] The full test suite passes with no behaviour change.

## Collision protocol

You own `unified_sqlmodels.py` and the new `app/core/` modules. Task 3b owns the
Texas validators — disjoint. Repo-wide importer rewiring is shared with 3z: do
the obvious in-`app/core` imports yourself; 3z fixes `scripts/` and the
`production_loader.py` record-type constant.
