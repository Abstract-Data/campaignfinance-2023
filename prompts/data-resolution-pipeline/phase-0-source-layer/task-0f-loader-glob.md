# Task 0f — Directory-glob ingestion (pick up every `tmp/<state>/` file)

> **Phase 0, round 1. Parallel-safe with 0a–0e.** Blocks `task-0z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The loader
config (`scripts/loaders/loader_config.py`) enumerates files by named key and
lists only 10 Texas files. Fourteen files in `tmp/texas/` are never referenced —
including `assets` and `cand` (which have models) and all the `_ss`/`_t`
variants (special-session and re-reported records). This task replaces the
hand-maintained manifest with directory-glob discovery so every file in a
state's directory is found and tagged with its TEC record type.

The `_ss` / `_t` variant files reuse their parent record's schema — they need
**no new model**, only discovery (`cont_ss`→`RCPT`, `cover_t`→`CVR1`, etc.).

## Dependencies

- **Depends on:** none — Phase 0 round 1.
- **Blocks:** `task-0z`.
- **Parallel-safe with:** 0a–0e (they touch only `app/core/source_models/`;
  this task touches only the loader scripts).

## Files

- **Create:** `scripts/loaders/file_discovery.py` — discovery + filename→record-type map.
- **Modify:** `scripts/loaders/loader_config.py` — add a state-directory entry
  alongside the existing keyed entries (do not delete the existing keys).
- **Modify:** `scripts/loaders/production_loader.py` — add a `load_state(state)`
  path that iterates discovered files. Leave `load_file()` intact.

**This is the only Phase-0 task that edits the loader scripts.**

## Interface contract

`scripts/loaders/file_discovery.py` exports:

- `FILENAME_RECORD_TYPES: dict[str, str]` — maps a filename prefix to a TEC
  record type, e.g. `{"contribs": "RCPT", "cont_ss": "RCPT", "cont_t": "RCPT",
  "expend": "EXPN", "expn_t": "EXPN", "expn_catg": "EXCAT", "cover": "CVR1",
  "cover_ss": "CVR1", "cover_t": "CVR1", "notices": "CVR2", "purpose": "CVR3",
  "credits": "CRED", "debts": "DEBT", "loans": "LOAN", "pledges": "PLDG",
  "pldg_ss": "PLDG", "pldg_t": "PLDG", "travel": "TRVL", "assets": "ASSET",
  "cand": "CAND", "filers": "FILER", "finals": "FINL", "spacs": "SPAC"}`.
  Confirm every prefix against `tmp/texas/CFS-ReadMe.txt`.
- `discover_state_files(state: str) -> list[DiscoveredFile]` — globs
  `tmp/<state>/*.parquet` and `*.csv`, returning, for each, a `DiscoveredFile`
  with `path: Path` and `record_type: str` (resolved from the filename prefix;
  files whose prefix is unknown are returned with `record_type = "UNKNOWN"` and
  logged, never silently dropped).

`production_loader.load_state(state)` iterates `discover_state_files(state)` and
loads each file. **Record-type dispatch (routing a record to the right model
builder) is wired by `task-0z`, not here** — this task only ensures every file
is *found* and *typed*. Until 0z lands, an unrecognized record type may be
skipped with a logged warning.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_file_discovery.py`: a failing test
  that `discover_state_files("texas")` returns one entry per file in
  `tmp/texas/` and that `cont_ss`, `cover_t`, `pldg_t`, `expn_catg`, `notices`,
  `purpose`, `spacs` are each present with the expected record type.
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_file_discovery.py -v`;
  expect failure.
- [ ] **Step 3** — Implement `file_discovery.py` per the interface contract.
- [ ] **Step 4** — Run the test; expect pass. Commit.
- [ ] **Step 5** — Add a `texas` state-directory entry to `loader_config.py`;
  add `load_state()` to `production_loader.py` (iterate discovered files, call
  the existing per-file load path for each).
- [ ] **Step 6** — Add a test that `load_state("texas")` is callable and visits
  every discovered file (a dry-run / counting test is fine — a full load is
  `task-0z`'s job). Run; pass; commit.

## Acceptance criteria

- [ ] `discover_state_files("texas")` finds **every** parquet file in
  `tmp/texas/` (23 at time of writing), each tagged with the correct record
  type; no file is dropped. Do not hard-code the count in the implementation —
  glob the directory.
- [ ] The `_ss` and `_t` variants resolve to their parent record type.
- [ ] Existing `loader_config.py` keys and `production_loader.load_file()` still
  work unchanged.
- [ ] Only the three files in the Files list are touched.

## Collision protocol

Branch `resolve/phase-0/task-0f-loader-glob`. You touch the loader scripts only;
0a–0e touch `app/core/source_models/` only — no overlap. Do not wire the
record-type → builder dispatch; that is `task-0z`'s, and wiring it now would
collide with 0z's edits to `production_loader.py`.
