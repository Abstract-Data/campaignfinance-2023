# Task C — Coverage verifier

> **Round 1. Parallel-safe with A, B, D.** Blocks `task-z`.
> Read the pack README before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, Polars, Postgres). After download
and conversion, the temp folder should hold a parquet file for every Texas TEC
record type. A broken or partial download must be caught **before** the pipeline
runs. This task adds a coverage verifier the `cf verify` command will call.

The authoritative record-type list is in `tmp/texas/CFS-ReadMe.txt` and is
summarized in the data-resolution spec's Appendix A
(`docs/superpowers/specs/2026-05-23-data-resolution-pipeline-design.md`).

## Dependencies

- **Depends on:** none — round 1.
- **Blocks:** `task-z`.
- **Parallel-safe with:** A, B, D.

## Files

- **Create:** `app/states/texas/texas_coverage.py` — the verifier.
- **Create:** `tests/cli/test_texas_coverage.py`.

New files only — no existing file is edited.

## Interface contract (must match the pack README)

```
verify_coverage(folder: Path) -> CoverageReport
```

- `CoverageRow` — a dataclass: `record_type: str`, `files: list[Path]`,
  `row_count: int`, `status: str` (`present` / `missing` / `empty`).
- `CoverageReport` — a dataclass: `rows: list[CoverageRow]` and a `ok: bool`
  property — `True` when every **required** record type is `present` and
  non-empty.

The module defines, as a constant, a filename-prefix → record-type map covering
the Texas TEC record types (`contribs`/`cont_ss`/`cont_t` → `RCPT`,
`expend`/`expn_t` → `EXPN`, `expn_catg` → `EXCAT`, `cover`/`cover_ss`/`cover_t` →
`CVR1`, `notices` → `CVR2`, `purpose` → `CVR3`, `credits` → `CRED`,
`debts` → `DEBT`, `loans` → `LOAN`, `pledges`/`pldg_ss`/`pldg_t` → `PLDG`,
`travel` → `TRVL`, `assets` → `ASSET`, `cand` → `CAND`, `filers` → `FILER`,
`finals` → `FINL`, `spacs` → `SPAC`) and a `REQUIRED_RECORD_TYPES` set (the
types whose absence should fail verification — at minimum the transaction and
filer types: `RCPT`, `EXPN`, `LOAN`, `FILER`, `CVR1`). Confirm prefixes against
`tmp/texas/CFS-ReadMe.txt` when it is available; if that file is absent in your
environment, the inline map above is the authoritative fallback — do not block
on it.

`verify_coverage` globs the folder for `*.parquet`, groups files by record type,
counts rows per type (Polars), and builds one `CoverageRow` per record type in
the map. A type with no file is `missing`; a type whose files total zero rows is
`empty`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/cli/test_texas_coverage.py`: a failing test
  that, given a temp folder with parquet files covering every required record
  type, `verify_coverage()` returns a `CoverageReport` with `ok is True` and a
  `present` row per type.
- [ ] **Step 2** — Run `uv run pytest tests/cli/test_texas_coverage.py -v`;
  expect failure.
- [ ] **Step 3** — Implement the prefix map, `REQUIRED_RECORD_TYPES`,
  `CoverageRow`, `CoverageReport`, and `verify_coverage()`. Run; pass. Commit.
- [ ] **Step 4** — Add failing tests: a folder missing a required type yields
  that row `missing` and `ok is False`; a zero-row parquet yields `empty`; a
  missing **non-required** type does not flip `ok`. Implement; run; pass; commit.

## Acceptance criteria

- [ ] `verify_coverage()` matches the README contract.
- [ ] Every mapped record type produces a `CoverageRow` with an accurate
  `status` and `row_count`.
- [ ] `ok` is `False` iff a required record type is missing or empty.
- [ ] The prefix map covers the `_ss`/`_t` variants (mapped to their parent
  type).
- [ ] No existing file is modified.

## Collision protocol

Branch `cli/task-c-verifier`. New files only. A owns the downloader, B creates
the converter, D owns `app/cli/` — no overlap.
