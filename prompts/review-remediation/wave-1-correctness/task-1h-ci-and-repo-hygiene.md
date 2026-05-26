# Task 1h — CI fix + repo hygiene

> **Wave 1, parallel. Branch `remediation/wave-1/task-1h-ci-and-repo-hygiene`.**
> Read the pack README, the Code Review Report (**P2-TEST-001**) and the
> Developer Assessment Report (risks **R6**, **R7**, **R8**).

## Context

CI computes its `--cov-fail-under=70` gate against `app/tests/` (one 24-line
file) while the real suites in `tests/` never run — the gate is a false signal.
The repo also commits a dev SQLite DB and dual lockfiles, and has no dependency
vulnerability scanning.

## Files

- **Modify:** `.github/workflows/ci-tests.yml`
- **Create:** `.github/workflows/` dependency-scan workflow (or extend `ci-quality.yml`)
- **Modify:** `.gitignore`
- **Delete:** `poetry.lock`, `campaignfinance_dev.db` (and any other committed `*.db`)

## What to implement

- **P2-TEST-001 (CI part)** — Change `ci-tests.yml` so pytest runs `tests/`
  (and `app/tests/` if you want both): `uv run pytest tests app/tests`. Keep the
  coverage gate but point `--cov` at the `app` package so the number is real.
- **R6** — Add dependency vulnerability scanning to CI: a `pip-audit` (or
  `uv`-native audit) step, plus a Dependabot config (`.github/dependabot.yml`)
  for `uv`/pip. Optionally emit an SBOM artifact (`cyclonedx-py`).
- **R7** — Delete `poetry.lock`; the project standardizes on `uv` (`uv.lock`
  stays). Confirm no workflow or doc references `poetry`.
- **R8** — `git rm --cached campaignfinance_dev.db` (and any other `*.db`), then
  add `*.db` to `.gitignore`.

## Steps

- [ ] **1** — Edit `ci-tests.yml`; verify the YAML is valid.
- [ ] **2** — Add the dependency-scan step + `dependabot.yml`.
- [ ] **3** — `git rm` the lockfile and the dev DB; update `.gitignore`.
- [ ] **4** — Run `uv run pytest tests` locally to confirm the suite the new CI
  will run is green. Commit.

## Acceptance criteria

- [ ] CI runs the `tests/` suite; the coverage gate reflects real coverage.
- [ ] A dependency-vulnerability scan runs in CI; `dependabot.yml` exists.
- [ ] `poetry.lock` and committed `*.db` files are gone; `*.db` is gitignored.

## Collision protocol

You own `.github/`, `.gitignore`, and the lockfile/DB deletions. No other wave-1
task touches these. Do not edit `pyproject.toml` (later waves manage deps).
