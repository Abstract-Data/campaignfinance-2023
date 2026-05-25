# DEPLOYMENTS.md

How campaignfinance is built, versioned, and run. This is a data-processing
pipeline, not a long-running service — "deployment" means running loaders
against a target database.

## Environments

| Environment | Database | Purpose |
|-------------|----------|---------|
| development | SQLite (local) | Iteration, unit tests, schema experiments |
| staging | PostgreSQL (non-prod) | Full-volume validation before a production load |
| production | PostgreSQL (prod) | Authoritative campaign-finance dataset |

Environment-specific agent guardrails: see `AGENTS.md` (base, dev) and
`AGENTS.staging.md` (staging).

## Build

- `uv sync` installs the locked dependency set from `uv.lock`.
- `Dockerfile` builds a container image for reproducible pipeline runs.

## Running a load

```
# schema + connection (uses get_db_manager / POSTGRES_* from .env)
uv run cf bootstrap

# scrape → convert → verify (Texas today)
uv run cf prepare texas

# load discovered parquet under tmp/<state> into Postgres
uv run cf load texas --preset production

# optional in-process cadence (stdlib scheduler; SIGTERM finishes current cycle)
uv run cf schedule texas --interval-hours 24
```

Legacy script entry (still supported):

```
uv run python scripts/loaders/production_loader.py testing texas
```

Host cron is the preferred production scheduler: invoke `cf prepare` and
`cf load` on a fixed schedule instead of a long-running `cf schedule` process
when possible.

Always run a load against **staging** at full volume before production. A
production load is a human-only action (see `docs/GUARDRAILS.md` — Privilege
Boundaries).

## Versioning

- Project version is tracked in `pyproject.toml`.
- `CHANGELOG.md` records notable changes; `release-please` automates changelog
  and version bumps from Conventional Commit messages.

## Monitoring

- The `Logger` class ships logs to PaperTrail plus local files under `app/logs/`.
- Watch row counts logged at each pipeline stage — an unexplained drop is a
  data-integrity incident (see `docs/GUARDRAILS.md`).

## Rollback

- Schema changes must have a documented downgrade path before a production load.
- Keep the prior dataset recoverable (database backup or snapshot) before any
  destructive reload.
- If a load produces suspect data, stop, restore from backup, and diagnose
  before re-running.
