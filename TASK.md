# TASK — Green up CI: clear pre-existing ruff debt + fix x86 test-collection crash

## Problem
`main` CI is red for two reasons pre-dating the vectorized/Alembic work:
1. **Ruff (Quality job):** 60 repo-wide errors. Breakdown by rule:
   - 35× F821 — `app/states/texas/validators/direct_expenditures.py`: a class of bare,
     un-annotated names. Dead (imported nowhere, no auto-discovery) AND broken (raises
     `NameError` on import — never worked).
   - 1× F821 — `app/states/texas/validators/texas_address.py:31`: `list['TECPersonName']`
     string forward-ref, `TECPersonName` never imported (real latent bug).
   - 21× I001 — import-block ordering across scripts/tests/app (mechanical).
   - 1× F401 (unused import), 1× F841 (`original_zip` in tx_validation_funcs.py), 1× E402.
2. **Tests (collection abort):** `tests/test_op_secrets.py` imports `app.op`, which does
   `from onepassword.lib.aarch64.op_uniffi_core import Error` — hardcodes `aarch64`, so on
   the x86_64 CI runner the path is `x86_64` → `ModuleNotFoundError` → pytest collection aborts.

Neither was introduced by PRs #52–#54 (those files are ruff-clean; local suite is 982 green).

## Files in scope
- DELETE `app/states/texas/validators/direct_expenditures.py` (user-confirmed: dead+broken).
- `app/states/texas/validators/texas_address.py` — add `TYPE_CHECKING` import of `TECPersonName`.
- `app/op.py` — replace the arch-hardcoded `Error` import with arch-agnostic resolution
  (reuse onepassword's own `platform.machine()`-resolved `onepassword.core.core`).
- `app/states/texas/funcs/tx_validation_funcs.py` — remove unused `original_zip` (F841) + I001.
- Remaining I001/F401/E402 sites: `scripts/verify_ingest.py`, `scripts/reset_and_reingest.py`,
  `scripts/debug/*.py`, `tests/resolve/test_phase0_reconciliation.py`, `app/states/texas/*`,
  `app/funcs/db_loader.py`, `app/core/load_context.py`, `app/core/enums.py`,
  `app/abcs/abc_download.py`, `app/abcs/abc_category.py`, `app/states/texas/texas_search.py`.
  Apply `ruff check --fix` (import sorting is safe-fixable); hand-fix the rest.

## Behavior to preserve
- No runtime behavior change. `app.op` must still expose the SAME `Error` class on this
  (aarch64) machine AND import successfully on x86_64. Deleting direct_expenditures.py must
  not break any import (verified: zero references, no discovery). texas_address change is
  TYPE_CHECKING-only (no runtime/circular-import effect).

## Checks to run (evidence required before "done")
1. `uv run ruff check .` → **0 errors** (was 60).
2. `uv run python -c "import app.op; print(app.op.Error)"` → succeeds, prints the Error class.
3. `uv run python -c "import platform; print(platform.machine())"` + confirm the new op.py
   import uses arch detection (code reads x86_64/aarch64 via onepassword.core), so x86 CI imports.
4. `uv run pytest tests/test_op_secrets.py -q` → collects + passes (was collection-abort on x86).
5. `uv run pytest -q` → full suite still green (no regression from the deletion / edits).
6. task-critic PASS; record gate verdict.

## Gates
- ruff 0 errors; full suite green; op.py import arch-agnostic & verified; task-critic PASS.

## SCOPE EXPANSION (user-approved after first CI run on PR #55)
The first CI run on #55 was still red. Investigation found the lint+op fix was correct
(`ruff check .` = 0; op collection-abort gone) but Quality + Tests fail for TWO more
pre-existing reasons, both unrelated to #52–#54. User chose: reformat in this PR + investigate.

1. **Quality also runs `ruff format --check .`** (separate from `ruff check`): 140 files
   unformatted repo-wide. FIX: `uvx ruff@latest format .` (140 reformatted) — committed as a
   dedicated style commit. `ruff format --check` now clean; full suite 982 still green (format
   is behavior-preserving).
2. **Tests job (`ci-tests.yml`) had 23 failures — three root causes, all pre-existing on main:**
   a. **Git LFS (18 failures):** `.gitattributes` has `*.csv filter=lfs`; the golden fixtures
      `tests/resolve/golden/*.csv` are LFS pointers. `ci-tests.yml` checked out WITHOUT
      `lfs: true` (unlike ci-resolve-tests.yml), so CI read 3-line pointer files →
      `KeyError: 'label'` / "got 2 pairs". FIX: add `lfs: true` to the ci-tests checkout.
   b. **Non-hermetic resolve CLI smoke tests (5):** `test_resolve_cli.py::TestMainSmoke`
      claims "uses in-memory SQLite" but never passed `--sqlite`, so the CLI targeted Postgres
      (its documented default: unreachable PG + non-interactive → exit 1, never silent SQLite
      fallback). Passed locally only because dev Postgres is reachable. FIX: add `--sqlite`.
   c. **Opportunistic discovery smoke test (1):** `test_discover_real_texas_directory_when_present`
      asserted instead of skipping when `tmp/texas` exists empty on CI (tmp/texas is untracked).
      FIX: skip when discovery finds no files.

## Added checks (evidence)
7. `uvx ruff@latest format --check .` → clean (was 140 to reformat).
8. `uv run pytest tests/resolve/test_resolve_cli.py::TestMainSmoke tests/resolve/test_file_discovery.py tests/resolve/test_match_quality.py -q` → all pass.
9. CI on PR #55 green (Quality + Tests). [pending re-run after push]
