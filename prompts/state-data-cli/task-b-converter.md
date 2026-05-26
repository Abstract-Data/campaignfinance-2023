# Task B — CSV → parquet converter

> **Round 1. Parallel-safe with A, C, D.** Blocks `task-z`.
> Read the pack README before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, Polars, Postgres). The Texas
downloader extracts date-suffixed CSV/`.txt` files into the temp folder, but the
resolution pipeline and the loaders read **parquet**. There is no CSV→parquet
conversion in the codebase today. This task adds it as a focused module the
`cf convert` command will call.

## Dependencies

- **Depends on:** none — round 1.
- **Blocks:** `task-z`.
- **Parallel-safe with:** A, C, D.

## Files

- **Create:** `app/states/texas/texas_converter.py` — the converter.
- **Create:** `tests/cli/test_texas_converter.py`.

New files only — no existing file is edited.

## Interface contract (must match the pack README)

```
convert_folder(
    folder: Path,
    *,
    overwrite: bool = False,
    keep_csv: bool = True,
    on_progress: Callable[[Path], None] | None = None,
) -> ConvertResult
```

- Walks `folder` for `*.csv` and `*.txt` files; writes a sibling `.parquet`
  with the same stem for each, using **Polars**
  (`pl.read_csv(...).write_parquet(...)`).
- `overwrite=False` skips a CSV whose `.parquet` already exists; `True` redoes it.
- `keep_csv=False` deletes a source CSV after its parquet is written
  successfully; `True` leaves it.
- `on_progress`, if given, is called with each file `Path` as it is processed
  (the CLI uses it to advance a progress bar).
- Returns `ConvertResult` — a dataclass with `converted: int`, `skipped: int`,
  `failed: list[tuple[Path, str]]` (path, error message), and a `ok: bool`
  property (`True` when `failed` is empty).

TEC CSVs have encoding and quoting quirks. Read with explicit options and an
encoding fallback (try `utf-8`, then `latin-1`/`cp1252`). A file that still
fails to parse is recorded in `failed` and the converter **continues** — one bad
file never aborts the run. Log each failure via `app/logger.py`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/cli/test_texas_converter.py`: a failing test
  that, given a temp folder with a small valid CSV, `convert_folder()` writes a
  `.parquet` of the same stem and returns `ConvertResult` with `converted == 1`,
  `ok is True`, and the parquet readable by Polars with the expected row count.
- [ ] **Step 2** — Run `uv run pytest tests/cli/test_texas_converter.py -v`;
  expect failure.
- [ ] **Step 3** — Implement `ConvertResult` and `convert_folder()`. Run; pass.
  Commit.
- [ ] **Step 4** — Add failing tests: `overwrite=False` skips an
  already-converted file (`skipped == 1`); `keep_csv=False` removes the source
  CSV; a malformed CSV lands in `failed`, `ok is False`, and the run still
  processes the other files. Implement; run; pass; commit.
- [ ] **Step 5** — Add a test that `on_progress` is called once per file.
  Run; pass; commit.

## Acceptance criteria

- [ ] `convert_folder()` matches the README contract.
- [ ] Every CSV/`.txt` in the folder becomes a same-stem parquet; the parquet is
  valid and round-trips the row count.
- [ ] `overwrite` and `keep_csv` behave as specified.
- [ ] A malformed file is recorded in `failed` and never aborts the run;
  encoding fallback is attempted.
- [ ] No existing file is modified.

## Collision protocol

Branch `cli/task-b-converter`. New files only. A owns the downloader, C creates
the verifier, D owns `app/cli/` — no overlap.
