# Task 5b — Scraper hardening

> **Wave 5, parallel. Branch `remediation/wave-5/task-5b-scraper-hardening`.**
> Requires Wave 4 merged. Read the pack README and the Developer Assessment
> Report (risk **R2**).

## Context

The Texas downloader drives a Selenium browser against the live Texas Ethics
Commission portal. When the portal's markup changes, the scraper breaks
silently — a high-impact, high-likelihood risk for a pipeline whose input
depends on it.

## Files

- **Modify:** `app/states/texas/texas_downloader.py`
- **Create:** `tests/states/test_texas_downloader.py`

## What to implement (R2)

- **Markup-drift detection** — before/while driving the download, assert the
  page elements the scraper depends on are present; if an expected
  selector/link is missing, raise a clear, specific error (e.g.
  `ScraperMarkupError`) naming what changed, rather than failing obscurely or
  silently producing no data.
- **Download-failure alerting** — when a download yields no file, times out, or
  the markup check fails, log an `error`-level message through the project
  `Logger` and exit non-zero, so a scheduled run surfaces the failure.
- **Fixture-based scraper tests** — add tests that exercise the parsing/markup-
  check logic against **saved HTML fixtures** (a known-good page and a
  drifted/changed page) with the Selenium WebDriver mocked. Do not run a live
  scrape in CI.
- Narrow any bare `except` and replace `ic()` with `Logger` in this file.

## Steps

- [ ] **1** — Save a known-good fixture of the relevant TEC page(s) under
  `tests/states/fixtures/`. Write failing tests: the markup check passes on the
  good fixture and raises `ScraperMarkupError` on a drifted fixture.
- [ ] **2** — Run; expect fail. **3** — Implement the markup-drift check,
  failure alerting, and `ScraperMarkupError`. **4** — Run; pass.
  `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] The downloader verifies expected markup and raises a specific error on
  drift.
- [ ] A failed/empty download logs at `error` level and exits non-zero.
- [ ] Fixture-based tests cover the good and drifted cases with Selenium mocked.

## Collision protocol

You own `app/states/texas/texas_downloader.py` and `tests/states/test_texas_downloader.py`.
Task 5a writes other `tests/states/` modules — distinct filenames, no collision.
