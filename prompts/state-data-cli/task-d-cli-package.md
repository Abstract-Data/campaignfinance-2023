# Task D — The `cf` Typer CLI package

> **Round 1. Parallel-safe with A, B, C.** Blocks `task-z`.
> Read the pack README before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, `rich`). This task builds the
`cf` umbrella command-line tool — a thin Typer package whose commands call the
logic modules from tasks A (downloader), B (converter), and C (verifier). The
CLI holds **no business logic**: it parses arguments, calls a logic function,
and renders the result with `rich`.

You build against the **interface contracts in the pack README**. Because A/B/C
may not be merged yet, import each logic module **lazily inside the command
function** (also good for CLI startup speed), and unit-test the commands with
the logic functions monkeypatched.

## Dependencies

- **Depends on:** none for building — round 1, against the README contracts.
- **Blocks:** `task-z`.
- **Parallel-safe with:** A, B, C.

## Files

- **Create:** `app/cli/__init__.py`, `app/cli/__main__.py`, `app/cli/main.py`,
  `app/cli/state.py`, `app/cli/download.py`, `app/cli/convert.py`,
  `app/cli/verify.py`, `app/cli/prepare.py`.
- **Create:** `tests/cli/test_commands.py`.
- **Modify (declared shared-file exception):** `pyproject.toml` — `uv add typer`
  and add the `[project.scripts]` entry. **Only `task-d` edits `pyproject.toml`
  in round 1.**

## What to build

1. **`uv add typer`**; add to `pyproject.toml` under `[project.scripts]`:
   `cf = "app.cli.main:app"`.
2. **`app/cli/state.py`** — `class State(str, Enum)` with `texas = "texas"`; a
   helper `resolve_state(state: State) -> StateContext` returning the state's
   `StateConfig` (`TEXAS_CONFIGURATION` from `app.states.texas`) and its temp
   folder `Path`. An unknown value is rejected by Typer's enum handling with a
   clear error.
3. **`app/cli/main.py`** — the root `typer.Typer()` app (`app`), with help text,
   `--version`, and the four commands registered. `__main__.py` calls `app()`
   so `python -m app.cli` works.
4. **Command modules** — one per verb, each registered on the root app:
   - `download.py` — `cf download <state> [--overwrite/-o] [--headless/--no-headless] [--out PATH]`.
     Lazily imports `TECDownloader` + `DownloadError` + `TEXAS_CONFIGURATION`;
     constructs the downloader; runs `download(overwrite=..., headless=...)`
     inside a `rich` status spinner; on `DownloadError` prints a clear message
     and `raise typer.Exit(code=1)`.
   - `convert.py` — `cf convert <state> [--overwrite] [--keep-csv/--no-keep-csv]`.
     Lazily imports `convert_folder`; drives it with a `rich` progress bar via
     the `on_progress` callback; prints the `ConvertResult` summary; exits
     non-zero if `not result.ok`.
   - `verify.py` — `cf verify <state>`. Lazily imports `verify_coverage`; renders
     the `CoverageReport` as a `rich` table (record type, files, rows, status);
     exits non-zero if `not report.ok`.
   - `prepare.py` — `cf prepare <state> [--overwrite] [--headless/--no-headless]
     [--skip-download]`. Calls the download, convert, and verify command logic
     in order; stops at the first failure, naming the stage that failed.
5. Every command and option carries a clear help string. Quiet by default; a
   root `--verbose/-v` flag raises log level.

## Steps (TDD)

- [ ] **Step 1** — `uv add typer`; create `app/cli/state.py` and a minimal
  `main.py` with `--version`. Write `tests/cli/test_commands.py` with a failing
  test that `CliRunner` invoking `cf --version` exits 0. Run; make it pass;
  commit.
- [ ] **Step 2** — Write failing `CliRunner` tests for each command with the
  logic functions **monkeypatched** (a fake `download`/`convert_folder`/
  `verify_coverage`): assert `cf download texas` exits 0 on success and 1 when
  the fake raises `DownloadError`; `cf convert texas` exits 1 when the fake
  result has `ok is False`; `cf verify texas` exits 1 on a not-ok report;
  `cf prepare texas` stops at the first failing stage.
- [ ] **Step 3** — Run; expect failure.
- [ ] **Step 4** — Implement `main.py` and the four command modules. Run; pass.
  Commit.
- [ ] **Step 5** — Add a test that an unknown state (`cf download utah`) exits
  non-zero with a usage error. Run; pass; commit.

## Acceptance criteria

- [ ] `cf` is registered in `[project.scripts]`; `python -m app.cli` also works.
- [ ] All four commands exist with the options in the README design summary and
  clear `--help` text.
- [ ] Commands exit 0 on success, non-zero on failure; `prepare` stops at the
  first failing stage.
- [ ] Logic modules are imported lazily; command tests pass with the logic
  monkeypatched.
- [ ] `pyproject.toml` is the only existing file modified.

## Collision protocol

Branch `cli/task-d-cli-package`. You own all of `app/cli/` plus the one declared
`pyproject.toml` edit. A/B/C own their own modules — do not import them at
module top-level (lazy import inside commands), so this package is testable
before they merge.
