# Database migrations (Alembic)

This project uses [Alembic](https://alembic.sqlalchemy.org/) for versioned schema management.
Before Alembic, schema was created by `SQLModel.metadata.create_all` (which only ever **adds
missing tables**, never alters existing ones) plus hand-maintained additive-column shim lists —
so any non-additive change, or any column not hand-registered, broke existing databases. Alembic
closes that gap: changes are versioned and applied to existing databases on deploy.

## Apply migrations (deploy step)

```bash
uv run cf migrate            # = alembic upgrade head, against the configured database
# or directly:
uv run alembic upgrade head
```

- **Fresh database** → the baseline revision (`0001_baseline`) builds the full schema
  (`create_all` + the Fix-7 partial/functional unique indexes, Postgres only).
- **Existing database** → pending revisions are applied in order. The baseline is idempotent
  (create_all skips existing tables; indexes use `IF NOT EXISTS`), so running it on a
  pre-Alembic database is safe and simply records the version.

`cf migrate` is the deploy mechanism — run it whenever the schema may have changed.

## Add a schema change (the forward pattern)

1. Edit the SQLModel models (`app/core/models/`, `app/core/source_models/`, `app/resolve/models/`).
2. Autogenerate a revision (diffs the models against a database at `head`):

   ```bash
   uv run alembic revision --autogenerate -m "add <thing>"
   ```

3. **Review** the generated `migrations/versions/*.py` — autogenerate misses some things
   (server defaults, partial/functional indexes, enum changes, data migrations). Hand-edit as
   needed; for raw DDL use `op.execute(...)`.
4. `uv run cf migrate` to apply locally; commit the revision file.

## Relationship to the app bootstrap

The in-process bootstrap (`UnifiedDatabaseManager.bootstrap` / `production_loader._get_session`)
still uses `create_all` (+ the additive-column shims) for speed — the test suite spins up many
throwaway SQLite databases and per-test Alembic runs would be slow. This stays consistent with
Alembic **by construction**: the baseline revision *is* `create_all` + the dedup indexes, and a
PG-gated test (`tests/core/test_alembic_migrations.py`) asserts `alembic upgrade head` produces a
schema identical to the bootstrap. Alembic is the source of truth for **deployed Postgres schema
evolution**; `create_all` is the local/test fast path.

The legacy additive-column shims (`_UNIFIED_ADDITIVE_COLUMNS`, resolve `_ADDITIVE_COLUMNS`) remain
as belt-and-braces and still patch the columns they list. **New** columns should be added via an
Alembic revision (above), not by extending the shim lists.

## Configuration

- `alembic.ini` — no hardcoded URL; `migrations/env.py` resolves it from `PostgresConfig` (the app
  default), overridable with `alembic -x dburl=postgresql+psycopg2://...`.
- `migrations/env.py` imports every model module so `SQLModel.metadata` is complete for autogenerate.

> Note: a broken **system** `/opt/homebrew/bin/alembic` shim (old python3.9 shebang) may spam
> `bad interpreter` on some shells — always invoke via `uv run alembic` / `uv run cf migrate`.
