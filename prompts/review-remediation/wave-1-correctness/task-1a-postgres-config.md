# Task 1a — Restore `postgres_config.py`

> **Wave 1, parallel. Branch `remediation/wave-1/task-1a-postgres-config`.**
> Read the pack README and the Code Review Report (finding **P1-OPS-001**).

## Context

`campaignfinance` (Python 3.12, SQLModel, Postgres). `app/core/unified_database.py:14`
imports `from app.states.postgres_config import PostgresConfig`, but that file
**does not exist** — only a stale `.pyc` remains, so the entire PostgreSQL path
fails at import. This task restores it.

## Files

- **Create:** `app/states/postgres_config.py`
- **Create:** `tests/test_postgres_config.py`

New files only — no collision with any wave-1 peer.

## What to implement (P1-OPS-001)

Recreate `PostgresConfig` as a `pydantic-settings` `BaseSettings` class reading
the `POSTGRES_*` variables documented in `.env.example` (`host`, `port`, `db`,
`user`, `password`, `pool_size`, `max_overflow`, `pool_timeout`, `pool_recycle`).
`password` is a `SecretStr`. Provide a `database_url` property. Use
`env_prefix="POSTGRES_"`, `env_file=".env"`. The Code Review Report gives the
exact recommended class body — follow it. The class must expose every attribute
`UnifiedDatabaseManager.__init__` uses (`unified_database.py:42-58`):
`.validate_connection()`, `.database_url`, `.pool_size`, `.max_overflow`,
`.pool_timeout`, `.pool_recycle`.

## Steps

- [ ] **1** — Write `tests/test_postgres_config.py`: failing test that
  `PostgresConfig` loads from env vars, `database_url` is well-formed, and
  `password` is a `SecretStr` (not exposed in `repr`).
- [ ] **2** — Run `uv run pytest tests/test_postgres_config.py -v`; expect fail.
- [ ] **3** — Implement `app/states/postgres_config.py`.
- [ ] **4** — Run the test; expect pass. Run `uv run ruff check --fix` on both
  files. Commit.

## Acceptance criteria

- [ ] `from app.states.postgres_config import PostgresConfig` succeeds.
- [ ] Every attribute `unified_database.py:42-58` references exists.
- [ ] `password` is `SecretStr`; not leaked in `repr`/`str`.
- [ ] Test passes; only the two new files were created.

## Collision protocol

New files only. The matching change to the `db_manager` singleton guard in
`unified_database.py` is **task 1c's** — do not touch `unified_database.py`.
