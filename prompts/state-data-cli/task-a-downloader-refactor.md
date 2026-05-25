# Task A — Downloader refactor: make the Texas scraper non-interactive

> **Round 1. Parallel-safe with B, C, D.** Blocks `task-z`.
> Read the pack README before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, Selenium, Postgres). The Texas
downloader (`TECDownloader` in `app/states/texas/texas_downloader.py`, base
`FileDownloaderABC` in `app/abcs/abc_download.py`) drives a Selenium scrape of
ethics.state.tx.us, downloads the TEC CSV zip, and extracts date-suffixed files.
It works, but it is a *script*, not a library: it calls `input()` and
`sys.exit()` when the temp folder is missing, prints `icecream` debug noise, and
its download-wait loop is an unbounded `while True`. This task makes it a clean,
non-interactive callable the new `cf` CLI can drive — a **targeted refactor, not
a rewrite**. The Selenium navigation flow itself stays as-is.

## Dependencies

- **Depends on:** none — round 1.
- **Blocks:** `task-z`.
- **Parallel-safe with:** B, C, D.

## Files

- **Modify:** `app/abcs/abc_download.py` — remove the interactive prompt.
- **Modify:** `app/states/texas/texas_downloader.py` — non-interactive
  `download()`, `headless` flag, wait-loop timeout, `DownloadError`.
- **Modify (only if the signature change breaks them):** `app/main.py`,
  `app/states/texas/__init__.py` — existing callers of `download()`. No other
  round-1 task touches these files, so they are safe for you to fix.
- **Create:** `tests/cli/test_texas_downloader.py`.

## Interface contract (must match the pack README)

`TECDownloader(config)` — constructed with `TEXAS_CONFIGURATION` from
`app.states.texas`. Method:

```
download(*, overwrite: bool = False, headless: bool = False) -> Path
```

Runs the scrape, extracts into `config.TEMP_FOLDER`, returns that `Path`. No
interactive input. Raises `DownloadError` (a new exception class in
`texas_downloader.py`) on any failure (Chrome missing, navigation failure,
download timeout).

## What to change

1. **Remove the interactive prompt.** `FileDownloaderABC.check_if_folder_exists`
   calls `input()` and `sys.exit()`s on "n". Replace it: if the temp folder is
   missing, **create it** (`mkdir(parents=True, exist_ok=True)`) — it is a temp
   folder, auto-creation is correct. No prompt, no `sys.exit`.
2. **Replace `icecream`.** Swap every `ic(...)` call for `app/logger.py` logging
   (`Logger(__name__)`). Debug-level for the chatty lines, info for milestones.
3. **Wire `headless`.** The Selenium `Options` already has a commented-out
   `--headless` argument — make `download()` take a `headless` parameter and add
   the argument when `True`.
4. **Bound the wait loop.** The `while True` that polls for `*.crdownload`
   files has no timeout. Add a max-wait (e.g. config or a constant, ~10 min)
   with a polling sleep; on timeout raise `DownloadError`.
5. **Typed failures.** Define `DownloadError(Exception)`; wrap the Selenium and
   extraction steps so failures raise it with a clear message instead of
   leaking a raw Selenium traceback.
6. **Update existing callers.** The new `download()` signature is keyword-only
   and drops `read_from_temp`. Grep the codebase for `.download(` and
   `read_from_temp`, and fix any caller (notably in `app/main.py` and
   `app/states/texas/__init__.py`) so nothing breaks.

Do **not** change the navigation sequence, the zip extraction, or the
date-suffix renaming — those work. Keep `read()` as-is.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/cli/test_texas_downloader.py`: a failing test
  that constructing `TECDownloader` with a config whose temp folder does not
  exist **creates the folder** and does not call `input()` (no prompt, no
  exit). Use a temp directory.
- [ ] **Step 2** — Run `uv run pytest tests/cli/test_texas_downloader.py -v`;
  expect failure.
- [ ] **Step 3** — Refactor `abc_download.py` to auto-create the folder; remove
  the prompt. Run; pass. Commit.
- [ ] **Step 4** — Add a failing test: `download()` accepts `headless=True` and
  builds Selenium `Options` with the headless argument (assert via a mocked
  `webdriver.Chrome` — patch it; do not launch a real browser).
- [ ] **Step 5** — Implement the `headless` parameter, `icecream`→logger swap,
  the wait-loop timeout, and `DownloadError`. Run; pass. Commit.
- [ ] **Step 6** — Add a failing test that a simulated wait-loop timeout raises
  `DownloadError` (mock the folder polling). Implement; run; pass; commit.
- [ ] **Step 7** — Grep for existing `.download(` / `read_from_temp` callers;
  update any that break under the new keyword-only signature. Run `uv run
  pytest` over the touched areas; commit.

## Acceptance criteria

- [ ] No `input()` / `sys.exit()` in the download path; a missing temp folder is
  auto-created.
- [ ] `download(*, overwrite, headless)` matches the README contract and returns
  the temp folder `Path`.
- [ ] `headless=True` runs Chrome headless; the wait loop has a hard timeout;
  failures raise `DownloadError`.
- [ ] No `icecream` calls remain in the touched files; logging goes through
  `app/logger.py`.
- [ ] The Selenium navigation flow, extraction, and renaming are unchanged.
- [ ] No file outside the Files list is modified.

## Collision protocol

Branch `cli/task-a-downloader-refactor`. You own the downloader files; B/C
create new modules; D owns `app/cli/`. No overlap.
