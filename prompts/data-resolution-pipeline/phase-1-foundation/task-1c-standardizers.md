# Task 1c — Standardizers + Stage 1 (feature-prep)

> **Phase 1, round 1. Parallel-safe with 1a, 1b.** Blocks round 2 and `task-1z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres, Polars).
Stage 1 of the resolution pipeline turns messy source records into clean,
comparable matching features. This task delivers the standardizer functions and
the stage-1 runner that writes them into the `resolution_input` staging table.

`usaddress` and `usaddress-scourgify` are already project dependencies. Add
`probablepeople` (person-vs-business name parsing) and `jellyfish` (phonetic
codes) via `uv add` if not present.

Reference: the spec's "The resolution pipeline → 1. Standardize / feature-prep".

## Dependencies

- **Depends on:** Phase 0 merged.
- **Blocks:** `task-1e`, `task-1f`, `task-1z`.
- **Parallel-safe with:** 1a, 1b.

## Files

- **Create:** `app/resolve/standardize/names.py`, `addresses.py`, `orgs.py`,
  `phonetics.py` — pure standardizer functions.
- **Create:** `app/resolve/standardize/staging.py` — the `ResolutionInput`
  staging-table SQLModel (this task owns it).
- **Create:** `app/resolve/standardize/stage1.py` — the stage-1 runner.
- **Create:** `tests/resolve/test_standardize.py` (use **Hypothesis** for the
  property-based tests below).
- **Modify (declared shared-file exception):** `pyproject.toml` / `uv.lock` —
  via `uv add probablepeople jellyfish`. This is the one shared file this task
  edits; no other Phase 1 round-1 task touches it.

New files only, plus the declared `uv add` above. Do **not** create any
`__init__.py` — `task-1z` owns those.

## Interface contract

Pure functions (deterministic, no I/O):

- `standardize_name(raw: str) -> StandardizedName` — parsed parts: `first`,
  `middle`, `last`, `suffix`, plus `is_organization: bool` (via
  `probablepeople`).
- `standardize_address(raw: str | dict) -> StandardizedAddress` — `line_1`,
  `line_2` (unit/suite preserved), `city`, `state`, `zip5`, `zip4`,
  `parse_status` (`parsed`/`partial`/`unparsed`). Uses `usaddress` + `scourgify`.
- `normalize_org_name(raw: str) -> str` — lower-cased, punctuation-stripped,
  legal-suffix-stripped (`LLC`, `INC`, `CO`, `CORP`, `LP`, `&`/`and`).
- `phonetic_code(token: str) -> str` — metaphone, via `jellyfish`.

`ResolutionInput` (`resolution_input`) — staging table, one row per source
record being resolved: `id` PK, `run_id` (int), `source_type`, `source_id`
(str), `entity_type`, the standardized name parts, standardized address parts,
`normalized_org`, phonetic codes, and `raw_name`/`raw_address` for audit.

`build_resolution_input(session, run_id, state_code) -> int` — stage-1 runner:
reads `unified_persons`, `unified_committees`, and `unified_entities` for the
state, standardizes each, writes `ResolutionInput` rows tagged with `run_id`,
returns the count. Use Polars for the bulk transform where it helps.

## Steps (TDD)

- [ ] **Step 1** — Write Hypothesis property tests in `test_standardize.py`:
  (a) `standardize_name` / `standardize_address` / `normalize_org_name` are
  **idempotent** (standardizing the output again equals the output);
  (b) `normalize_org_name("Acme, L.L.C.")` == `normalize_org_name("ACME LLC")`;
  (c) a unit/suite survives `standardize_address`. Run; expect failure.
- [ ] **Step 2** — Implement `names.py`, `addresses.py`, `orgs.py`,
  `phonetics.py`. Run; expect pass. Commit.
- [ ] **Step 3** — Write a failing test for `ResolutionInput` creating via
  `create_all` and for `build_resolution_input()` producing one staging row per
  source record on a small seeded fixture.
- [ ] **Step 4** — Implement `staging.py` and `stage1.py`. Run; pass. Commit.
- [ ] **Step 5** — Add a test that an unparseable address yields
  `parse_status="unparsed"` and does **not** raise. Run; pass; commit.

## Acceptance criteria

- [ ] All four standardizers are pure, deterministic, and idempotent (Hypothesis
  tests pass).
- [ ] `build_resolution_input()` writes one `resolution_input` row per source
  record, tagged with `run_id`.
- [ ] Unparseable input degrades to `unparsed`, never raises.
- [ ] Any new dependency is added to `pyproject.toml` via `uv add`.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-1/task-1c-standardizers`. New files only — including the
`resolution_input` table, which is yours alone — plus the one declared
`pyproject.toml` / `uv.lock` edit from `uv add`. Do not create any `__init__.py`.
