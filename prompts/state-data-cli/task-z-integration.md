# Task Z — Integration: wire the real modules, end-to-end `prepare`

> **Serial — runs LAST.** Depends on A, B, C, D all merged.
> Read the pack README before starting.

## Context

Tasks A–D delivered the downloader refactor, the converter, the verifier, and
the `cf` CLI in isolation — the CLI built against the README's interface
contracts with the logic monkeypatched. This task confirms the real modules
connect, proves `cf prepare texas` end-to-end, and documents the CLI.

## Dependencies

- **Depends on:** A, B, C, D (all merged).
- **Blocks:** nothing — this is the last task.

## Files

- **Create:** `tests/cli/test_prepare_integration.py`.
- **Modify (only if an interface gap is found):** the relevant `app/cli/`
  command module or a logic module — to fix a real mismatch against the README
  contract.
- **Modify:** `README.md` (project root) — add a short "CLI" section.

## What to do

1. **Verify the contracts held.** Confirm each command's lazy import resolves
   the real module and that the real `download()` / `convert_folder()` /
   `verify_coverage()` signatures and return types match what the command
   modules expect. Fix any genuine mismatch here (and note the deviation in the
   commit) rather than reopening a parallel task.
2. **Confirm the entry point.** `uv sync`, then verify `cf --help` and
   `cf --version` run via the installed console script, and `python -m app.cli`
   works.
3. **End-to-end `prepare` test** — `tests/cli/test_prepare_integration.py`:
   with the Selenium download **mocked** (patch `webdriver.Chrome` and seed the
   temp folder with a few small fixture CSVs as if a download had extracted
   them), run `cf prepare texas` via `CliRunner` and assert: convert produces
   parquet, verify reports coverage, the command exits 0; then seed a folder
   missing a required record type and assert `cf prepare texas` exits non-zero
   naming the verify stage. Do **not** run a live scrape in the test.
4. **Document.** Add a CLI section to the project `README.md`: install
   (`uv sync`), the four commands, and a one-line `cf prepare texas` example.

## Steps (TDD)

- [ ] **Step 1** — `uv sync`; confirm `cf --help`, `cf --version`, and
  `python -m app.cli` all run. Commit any wiring fix.
- [ ] **Step 2** — Write `tests/cli/test_prepare_integration.py` (failing) for
  the mocked-download happy path through `prepare`.
- [ ] **Step 3** — Make it pass — fixing any real interface mismatch in the
  command modules or logic modules. Commit.
- [ ] **Step 4** — Add the failing-coverage case (missing required type →
  non-zero exit). Make it pass. Commit.
- [ ] **Step 5** — Run the full `uv run pytest tests/cli/` suite; confirm green.
  Add the CLI section to `README.md`. Commit.

## Acceptance criteria

- [ ] `uv run pytest tests/cli/` is fully green, including the end-to-end
  `prepare` integration test.
- [ ] `cf` runs as an installed console script and as `python -m app.cli`.
- [ ] `cf prepare texas` chains download → convert → verify, exits 0 on success
  and non-zero (naming the stage) on failure — verified with the download
  mocked.
- [ ] The project `README.md` documents the CLI.

## Collision protocol

Branch `cli/task-z-integration`, cut after A–D are merged. This task may edit
shared files to close a real interface gap — fix and note it; do not reopen a
parallel task.
