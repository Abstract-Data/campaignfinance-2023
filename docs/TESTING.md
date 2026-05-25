# Testing

How the `campaignfinance` test suite is organized, how to run it, and what
"tested" means for this project.

## Running the suite

```bash
uv sync                       # install/sync dependencies first
uv run pytest                 # full suite
uv run pytest -v --tb=short   # verbose, short tracebacks
uv run pytest -x              # stop on first failure (fast feedback loop)
uv run pytest -k "name"       # run tests matching an expression
```

Run a single directory while iterating:

```bash
uv run pytest tests/resolve   # entity-resolution pipeline only
uv run pytest tests/cli       # CLI commands only
uv run pytest app/tests       # core unit tests only
```

## Test layout

Tests live in two roots — `tests/` (integration and feature tests) and
`app/tests/` (unit tests close to the code). Current layout:

| Location | Scope | Covers |
|----------|-------|--------|
| `tests/resolve/` | Entity-resolution pipeline | Standardization, blocking, classify, fastpath, scoring, clustering, survivorship, canonical schema, loader glob, golden-file fixtures |
| `tests/cli/` | `cf` CLI | `download` / `convert` / `verify` / `prepare` commands; Texas converter, downloader, coverage; prepare integration |
| `tests/verify/` | Tool-config verification | `check_agents_md.py` — asserts AGENTS.md keeps its required sections (also run in CI) |
| `app/tests/` | Core unit tests | Ingestion and validation internals, property-based tests |
| `conftest.py` (root) | Shared config | Puts the project root on `sys.path` so scripts import cleanly |

When you add a test, place integration/feature tests under the matching
`tests/<area>/` directory and pure unit tests under `app/tests/`.

## Property-based testing

The ingestion and validation code is exercised with [Hypothesis]. Property tests
generate many record shapes rather than relying on hand-picked examples — they
catch header-normalization and type-coercion edge cases that fixed fixtures
miss.

```bash
uv run pytest app/tests/ --hypothesis-show-statistics
```

Use Hypothesis for any code that parses or validates external data (file
readers, validators, field mapping). Use plain example-based tests for
deterministic logic and for CLI behavior.

## Fixtures

Prefer `pytest` fixtures for shared setup — temp directories, sample records,
configured downloaders — so tests stay isolated and readable. `tests/resolve/`
keeps golden-file fixtures under `tests/resolve/golden/` for stable
end-to-end expectations.

Tests must not touch real state portals, the production PostgreSQL database, or
files in `tmp/`. Downloaders are exercised against mocked temp folders; database
tests run against SQLite or a disposable schema.

## Coverage gate

CI runs pytest with branch coverage and **fails under 70%**:

```bash
uv run pytest app/tests --cov=app --cov-branch --cov-report=term-missing --cov-fail-under=70
```

70% is the current floor, not the goal — raise it as coverage improves rather
than letting it drift. Coverage detail is reported to Codecov.

## What CI runs

| Workflow | Runs |
|----------|------|
| `ci.yml` | Umbrella — triggers quality + tests + resolve + report on push/PR |
| `ci-quality.yml` | `ruff check` + `ruff format --check` |
| `ci-tests.yml` | `pytest app/tests` with branch coverage (`--cov-fail-under=70`) on Python 3.12 and 3.13 |
| `ci-resolve-tests.yml` | `pytest tests/resolve -m "not integration"` on Python 3.12 (fast resolve suite; no coverage gate) |
| `ci-resolve-integration.yml` | `workflow_dispatch` only — `pytest tests/resolve -m integration` (Texas data + Postgres when configured) |
| `ci-report.yml` | Posts a sticky CI summary comment on PRs |
| `tool-config-verify.yml` | `check_agents_md.py`, hook presence, symlink integrity |

PRs gate on `app/tests` coverage and the fast resolve suite. Full Texas load, Postgres-backed resolve, and publish are **not** PR blockers — see [`RUNBOOK.md`](RUNBOOK.md) → Phase 0 / resolution manual gate.

## Resolve test tiers

| Tier | Command | When |
|------|---------|------|
| **Fast (default CI)** | `uv run pytest tests/resolve -m "not integration" -v --tb=short` | Every PR; no `tmp/texas` or production DB required |
| **Integration (optional)** | `uv run pytest tests/resolve -m integration` | Local or `ci-resolve-integration.yml` dispatch; requires `tmp/texas` and Postgres when tests need it |
| **Full local** | `uv run pytest tests/resolve` | Developer machine with golden CSVs tracked in git |

Tests marked `@pytest.mark.integration` are excluded from default CI. They smoke-check real state files and optional Postgres paths documented in the runbook.

## Definition of "tested"

A change is adequately tested when: new behavior has tests that fail without the
change and pass with it; parsing/validation code has at least one Hypothesis
property; the full `uv run pytest` is green; and coverage stays at or above the
gate. See `AGENTS.md` → `## Definition of Done`.

[Hypothesis]: https://hypothesis.readthedocs.io
