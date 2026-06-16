# TASK — Migration gap: introduce a real migration framework (Alembic)

## Problem ([[schema-drift-no-migration]])
No migration framework. Schema = `SQLModel.metadata.create_all` (creates MISSING TABLES only —
never ALTERs) + two hand-maintained additive-column shim lists + raw-DDL dedup indexes:
- `app/core/unified_database.py`: `_UNIFIED_ADDITIVE_COLUMNS` (3 cols: report committee/treasurer
  name-at-filing, persons.dedup_addr_key) + `_apply_dedup_indexes` (partial unique indexes).
- `app/resolve/run.py`: `_ADDITIVE_COLUMNS` (resolution_input links, canonical_entity.employer).
Every NEW post-deploy column must be hand-added to a shim list or it breaks existing DBs; non-additive
changes (type/constraint/drop/rename) have NO path at all. CI misses it (always create_all fresh).

## DECISION: Full Alembic baseline (user-chosen).

## STATUS — foundation DONE + verified (branch `feat/alembic-migrations`, commit 83c9e9bb)
- alembic added (1.18.4); `alembic.ini` + `migrations/env.py` (target_metadata=SQLModel.metadata
  after importing all model modules; URL from PostgresConfig, `-x dburl=` override) + script template.
- Baseline `0001_baseline`: create_all + Fix-7 dedup indexes (PG-only). VERIFIED `alembic upgrade
  head` on a fresh DB == bootstrap schema (33 tables, 0 col/index mismatch; only alembic_version extra).
- Inert/safe: nothing auto-invokes Alembic yet, so the test suite is unaffected.

## REMAINING (the integration step — behavior change, do next):
1. Wire production bootstrap to Alembic: deploy runs `alembic upgrade head`; on a fresh app-bootstrap
   (create_all path, kept FAST for the ~975 sqlite tests) `alembic stamp head` so future deltas apply.
   DECISION inside: do NOT route every _get_session through alembic (per-test overhead) — keep
   create_all for sqlite tests, use alembic for Postgres deploys/existing DBs.
2. Retire the additive-shim lists (`_UNIFIED_ADDITIVE_COLUMNS`, resolve `_ADDITIVE_COLUMNS`) once a
   migration covers their columns; or keep as belt-and-braces no-ops. Update [[schema-drift-no-migration]].
3. Add a test: `alembic upgrade head` (fresh) == create_all schema; document the workflow (a MIGRATIONS.md).

## Original plan / risks below.

## Plan (assuming standard Alembic baseline adoption)
1. Add Alembic (`alembic.ini`, `migrations/env.py`, `versions/`). Wire `env.py` to the project's
   SQLModel `target_metadata` + DB URL resolution (reuse `get_db_manager` / `resolve_runtime_database_url`).
   Handle the offline/online + the project's import-time table registration.
2. **Baseline migration** capturing the CURRENT full schema (tables + the additive columns + the
   partial-unique dedup indexes + resolve tables). Verify `alembic upgrade head` on an empty DB
   produces a schema byte-equal to `create_all` + the shims (diff via reflection).
3. **Existing DBs**: `alembic stamp head` marks them at-baseline (schema already present). Document.
4. **Bootstrap rewire**: `UnifiedDatabaseManager.bootstrap()` / `production_loader._get_session` run
   `alembic upgrade head` (idempotent) instead of / in addition to create_all. Keep create_all as a
   fallback for test/sqlite if Alembic-on-sqlite is awkward.
5. Retire the additive shims into the migration history (or keep as a belt-and-braces no-op) once the
   baseline + an "additive cols" migration cover them; update [[schema-drift-no-migration]].
6. Establish the forward pattern: `alembic revision --autogenerate` for future model changes.

## Risks / unknowns to resolve during impl
- The "migrations" + "alembic runs" PreToolUse gate will ASK for confirmation — expected.
- sqlite (tests) vs Postgres (prod) — partial indexes + some DDL are PG-only; the baseline migration
  must branch on dialect (or guard PG-only DDL), mirroring the current `_apply_dedup_indexes` logic.
- Autogenerate vs the raw-DDL indexes / any schema-qualified models — verify autogenerate doesn't try
  to drop them; may need `include_object` filters in env.py.
- The vectorized engine + tests bootstrap via `_get_session`; ensure the rewire keeps them green.

## Gates
1. `alembic upgrade head` on empty DB == create_all+shims schema (reflection diff empty).
2. Full test suite green (bootstrap rewire must not break the 975-test suite).
3. ruff clean; no f-string SQL; task-critic PASS.
