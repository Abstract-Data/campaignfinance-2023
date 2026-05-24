# Campaign Finance

Multi-state campaign finance data pipeline: download state portal extracts, convert to parquet, verify coverage, and load into PostgreSQL for cross-state analysis.

## Setup

Install dependencies with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

`uv sync` installs the `cf` console script and project package. `probablepeople` pulls in `doublemetaphone`; the lockfile pins **1.2** (prebuilt wheels for Python 3.12) via `[tool.uv] override-dependencies` because 1.1 only ships an sdist that fails to compile on 3.12.

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

This downloads the latest Texas Ethics Commission CSV bundle, converts files under `tmp/texas/` to parquet, and prints a coverage table. The command exits non-zero if any required record type (`RCPT`, `EXPN`, `LOAN`, `FILER`, `CVR1`) is missing or empty.

Use `--out PATH` on `download` or `prepare` to write files to a custom directory instead of the default `tmp/texas/`.

```bash
uv run cf --version
uv run python -m app.cli --help
```

Both `uv run cf` and `uv run python -m app.cli` work after `uv sync`; you do not need `PYTHONPATH=.:app`.

## Tests

```bash
uv run pytest tests/cli/ -v
```
