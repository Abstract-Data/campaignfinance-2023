# TASK — Alembic migration: dedup legacy unified_transactions + apply strict unique index

## Problem ([[legacy-transaction-dedup-deferred]])
Pre-Wave-2 (insert-only + FileOrigin-guard) loads produced duplicate
`(state_id, transaction_type, transaction_id)` groups. Wave 2 added the strict partial unique
index `uix_transactions_state_type_sourceid` (in `_DEDUP_INDEXES`, applied at bootstrap). Existing
polluted DBs (campaign_finance ≈ 888 groups; campaignfinance_elt_spike ≈ 17,707) can't get the
index — `_apply_dedup_indexes` crashes on the dup key. Fresh loads are clean (natural-key upsert).
There's a working one-off cleanup (`scripts/dedup_unified_transactions.py`, dry-run by default) but
no migration, so `cf migrate` / `alembic upgrade head` can't bring an existing DB current.

## DECISION (user-chosen): turn the dedup into an Alembic revision `0002`, after `0001_baseline`.

## Key ordering insight
`0001_baseline.upgrade()` creates the unique index via `_DEDUP_INDEXES` (IF NOT EXISTS). But existing
polluted DBs are `alembic stamp 0001_baseline`'d (baseline never RUNS on them) — so they never got the
index and still hold dups. So `0002` must: **dedup first, THEN create the index** — a no-op on fresh
DBs (0 dups; index already exists from 0001 → IF NOT EXISTS), the real fix on stamped-polluted DBs.

## Plan
1. New revision `migrations/versions/<date>_0002_dedup_legacy_transactions.py`:
   - `revision="0002_dedup_legacy_transactions"`, `down_revision="0001_baseline"`.
   - `upgrade()`: PG-only (skip sqlite, like baseline — sqlite never had the index). INLINE the
     static SQL (migrations are frozen/self-contained — do NOT import from scripts/):
     a. temp table `_doomed_txn` ← non-surviving dup ids (row_number()>1 per group, keep lowest id);
     b. purge children deepest-FK-first — loan_guarantors (via unified_loans/unified_debts) → the 10
        transaction_id children (unified_transaction_persons/_versions, contributions, expenditures,
        loans, debts, credits, travel, assets, pledges); then purge parents; drop the temp table;
     c. `CREATE UNIQUE INDEX IF NOT EXISTS uix_transactions_state_type_sourceid ...`.
   - `downgrade()`: `DROP INDEX IF EXISTS uix_transactions_state_type_sourceid` (note: cannot restore
     deleted rows; downgrade only removes the constraint).
2. Child-table list verified current (2026-06-16): the 10 children + loan_guarantors grandchild match
   the live SQLModel.metadata FK graph exactly (same set the script uses).

## Files in scope
- ADD `migrations/versions/<date>_0002_dedup_legacy_transactions.py`.
- ADD `tests/core/test_dedup_migration.py` (PG-gated, mirrors `tests/core/test_alembic_migrations.py`).
- (optional) note the migration in `MIGRATIONS.md`.
- Do NOT change `0001_baseline`, `_DEDUP_INDEXES`, or `scripts/dedup_unified_transactions.py`.

## Behavior to preserve
- Fresh-DB `alembic upgrade head` stays byte-equal to bootstrap (0002 is a no-op there: 0 dups, index
  already present). The ~982 sqlite test suite is unaffected (0002 skips on sqlite).
- Survivor = lowest `id` per group (matches the script + the upsert's first-wins).
- No FK orphans: every purged parent's children removed first (NO ACTION FKs).

## Checks to run (evidence required)
1. PG-gated test: seed a "legacy polluted" DB (schema via create_all, NO unique index, with planted
   dup groups + children), `stamp 0001_baseline` → `upgrade head`; assert: dup groups → 0, only
   lowest-id survivors remain, doomed children purged, index exists, non-dup rows untouched.
2. PG-gated idempotency/fresh-path: `upgrade head` on a clean DB → index present, row counts unchanged,
   re-running the dedup SQL purges 0 rows.
3. `uvx ruff@latest check . && uvx ruff@latest format --check .` clean.
4. Full suite green (`uv run pytest -q`): 982 passed / 5 skipped unchanged (0002 no-ops on sqlite).
5. `uv run alembic history` shows 0001 → 0002 linear; `alembic upgrade head` on a fresh PG DB succeeds.
6. task-critic PASS.

## Risks
- The migrations/alembic-run PreToolUse gate will ASK on commit — expected.
- Temp table inside Alembic's transaction: use explicit `DROP TABLE _doomed_txn` (don't rely on
  ON COMMIT DROP / commit timing).
- All SQL static (no identifier interpolation) — satisfies the SQL-injection hook.
