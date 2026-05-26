# State Data CLI — Implementation Prompt Pack

> **For agentic workers:** Each `task-*.md` is a complete, self-contained work
> order for **one agent**. Hand the whole file to a fresh agent. Steps use
> `- [ ]` checkbox syntax. Recommended driver: `superpowers:subagent-driven-development`
> or `superpowers:executing-plans`.

**Goal:** Build `cf` — an umbrella Typer command-line tool for the
`campaignfinance` project — that downloads Texas campaign-finance data, converts
it to parquet, and verifies coverage, leaving the data prepared to run the
resolution pipeline.

**Architecture:** A thin `app/cli/` Typer package whose commands call into three
logic modules: a refactored (non-interactive) Texas downloader, a new CSV→parquet
converter, and a new coverage verifier. The CLI holds no business logic.

**Tech stack:** Python 3.12, `uv`, Typer (new dependency), `rich` (present),
Polars (present), Selenium (present), pytest.

## Design summary

`cf` is installed as a console script (`[project.scripts]` →
`cf = "app.cli.main:app"`) and also runs as `python -m app.cli`. Four verb
commands, each taking a state as a positional argument (a `State` enum —
`texas` only today, more is a one-line addition):

- `cf download texas` — run the Selenium scraper; fetch + extract the TEC data.
  Options: `--overwrite/-o`, `--headless/--no-headless`, `--out PATH`.
- `cf convert texas` — convert the extracted CSVs to parquet. Options:
  `--overwrite`, `--keep-csv/--no-keep-csv`.
- `cf verify texas` — check coverage; print a table; non-zero exit if a required
  record type is missing.
- `cf prepare texas` — runs download → convert → verify in order, stopping at
  the first failure. Option: `--skip-download`.

It is an umbrella CLI: `cf load` / `cf resolve` are natural future subcommands,
designed for but not built here.

## How to use this pack

1. Read this README, then assign each `task-*.md` to its own agent.
2. **Round 1 (parallel):** `task-a`, `task-b`, `task-c`, `task-d` — four
   collision-free tasks, dispatch concurrently.
3. **Then `task-z` integration** — runs after round 1 merges.

```
round 1 (parallel):  task-a  task-b  task-c  task-d
then:                task-z  integration
```

## Interface contracts

Round-1 tasks are parallel because they agree on these contracts up front.
`task-d` (the CLI) builds against them and unit-tests with the logic functions
mocked; `task-z` wires the real modules together.

**Downloader (`task-a`)** — `app/states/texas/texas_downloader.py`:
`TECDownloader(config)` constructed with `TEXAS_CONFIGURATION` (from
`app.states.texas`). Method `download(*, overwrite: bool = False,
headless: bool = False) -> Path` — runs the scrape, extracts into the config's
temp folder, returns that folder. **Non-interactive** (no `input()`); raises
`DownloadError` on failure.

**Converter (`task-b`)** — `app/states/texas/texas_converter.py`:
`convert_folder(folder: Path, *, overwrite: bool = False, keep_csv: bool = True,
on_progress: Callable[[Path], None] | None = None) -> ConvertResult`.
`ConvertResult` carries `converted: int`, `skipped: int`,
`failed: list[tuple[Path, str]]`, and `ok: bool` (`True` when `failed` is empty).

**Verifier (`task-c`)** — `app/states/texas/texas_coverage.py`:
`verify_coverage(folder: Path) -> CoverageReport`. `CoverageReport` carries
`rows: list[CoverageRow]` (each: `record_type`, `files`, `row_count`, `status`
∈ `present`/`missing`/`empty`) and `ok: bool` (`True` when every **required**
record type is present and non-empty).

## Collision protocol

1. **Parallel tasks own disjoint files.** `task-a` edits the downloader files;
   `task-b` and `task-c` create one new module each; `task-d` owns the entire
   new `app/cli/` package.
2. **`task-d` is the only round-1 task that edits `pyproject.toml`** (adds
   `typer` via `uv add` and the `[project.scripts]` entry).
3. Each task works on its own branch: `cli/task-<x>-<slug>`.
4. `task-d` imports the `task-a/b/c` modules **lazily inside each command
   function** so the CLI is importable and unit-testable (with the logic mocked)
   before those modules merge.
5. If a task must edit a file another parallel task owns: stop and flag it.

## Conventions

- **TDD:** failing test → see it fail → implement → see it pass → commit. One
  green step, one commit. Conventional Commit messages.
- CLI tests use Typer's `CliRunner`; tests live under `tests/cli/`; logic-module
  tests under `tests/states/` (or `tests/`). Run with `uv run pytest`.
- Logic modules log via `app/logger.py` and return result objects — they do not
  print. All `rich` rendering happens in the CLI layer.
- No interactive prompts anywhere.
- Do not edit `CLAUDE.md` or anything under `.claude/`.
