# Campaign Finance

Multi-state campaign finance data pipeline: download state ethics commission
extracts, validate and normalize into a unified cross-state schema, load into
PostgreSQL, and resolve duplicate entities for analytics.

**States in progress:** Texas (most complete), Oklahoma, Ohio. **Stack:** Python
3.12, uv, SQLModel, Pydantic v2, Polars, Splink, PostgreSQL (prod) / SQLite
(dev).

## What this repo does

1. **Acquire** — Selenium scrapers download CSV bundles from state portals
   (`app/states/`, `app/workflows/`).
2. **Prepare** — The `cf` CLI converts CSV to parquet, verifies required record
   types, and chains download → convert → verify.
3. **Ingest** — Schema-driven `GenericFileReader` plus per-state SQLModel
   validators (`app/ingest/`, `app/abcs/`).
4. **Unify** — `app/core/` maps state fields to shared tables (transactions,
   contributions, entities, committees, persons, addresses).
5. **Load** — `scripts/loaders/production_loader.py` streams batches into the
   database with deduplication caches.
6. **Resolve** — `app/resolve/` standardizes names/addresses, blocks candidate
   pairs, scores with Splink, clusters merges, and publishes canonical entities
   plus crosswalks for downstream analytics.

## Documentation

| Document | Purpose |
|----------|---------|
| [AGENTS.md](AGENTS.md) | Agent instructions, commands, guardrails |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Narrative system design |
| [docs/ARCHITECTURE-DIAGRAM.md](docs/ARCHITECTURE-DIAGRAM.md) | Mermaid pipeline + unified ERD |
| [docs/DATA_RELATIONSHIPS.md](docs/DATA_RELATIONSHIPS.md) | Full ERD (unified → canonical → views) |
| [docs/RUNBOOK.md](docs/RUNBOOK.md) | Operations and troubleshooting |
| [docs/adr/](docs/adr/) | Architecture decision records |

## Setup

Install dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

`uv sync` installs the `cf` console script and project package. `probablepeople`
pulls in `doublemetaphone`; the lockfile pins **1.2** (prebuilt wheels for
Python 3.12) via `[tool.uv] override-dependencies` because 1.1 only ships an
sdist that fails to compile on 3.12.

Copy `.env.example` to `.env` for local database and 1Password SDK settings.
Never commit `.env`.

## State Data CLI (`cf`)

The `cf` command prepares Texas campaign finance data for the resolution pipeline.

| Command | Description |
|---------|-------------|
| `cf download texas` | Download and extract TEC CSV data via Selenium |
| `cf convert texas` | Convert extracted CSV/txt files to parquet |
| `cf verify texas` | Verify required record types are present |
| `cf prepare texas` | Run download → convert → verify in order |

Common options:

- `cf download texas --headless --overwrite --out /path/to/data`
- `cf convert texas --overwrite --no-keep-csv`
- `cf prepare texas --skip-download` — convert and verify only

### Example: prepare Texas data

```bash
uv run cf prepare texas
```

This downloads the latest Texas Ethics Commission CSV bundle, converts files
under `tmp/texas/` to parquet, and prints a coverage table. The command exits
non-zero if any required record type (`RCPT`, `EXPN`, `LOAN`, `FILER`, `CVR1`)
is missing or empty.

Use `--out PATH` on `download` or `prepare` to write files to a custom directory
instead of the default `tmp/texas/`.

```bash
uv run cf --version
uv run python -m app.cli --help
```

Both `uv run cf` and `uv run python -m app.cli` work after `uv sync`; you do not
need `PYTHONPATH=.:app`.

## Loading and resolution

```bash
# Load a sample into the dev database
uv run python scripts/loaders/production_loader.py testing texas_sample

# Run resolve pipeline (see docs/superpowers/specs/ for design)
uv run python -m app.resolve --help
```

## Tests

```bash
uv run pytest tests/ app/tests/ -x
uv run pytest tests/resolve -m "not integration"
```

See [docs/TESTING.md](docs/TESTING.md) for the full test strategy.
