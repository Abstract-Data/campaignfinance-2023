# Runbook

Operational quick reference for the campaignfinance data pipeline: loading,
entity resolution, and troubleshooting.

## Quick reference

```bash
uv sync
uv run pytest tests/resolve -m "not integration"   # fast resolve suite (CI parity)
uv run pytest tests/resolve                        # full resolve suite locally
uv run pytest app/tests --cov=app --cov-fail-under=70
uv run ruff check app/resolve/ tests/resolve/
uv run cf prepare texas
uv run python scripts/loaders/production_loader.py
uv run python -m app.resolve run --state texas
uv run python -m app.resolve publish --state texas
```

## Database reset / full reload

```bash
# Full reset: truncate unified tables → bootstrap → vectorized re-ingest
uv run python scripts/reset_and_reingest.py

# Preview only — no DB writes
uv run python scripts/reset_and_reingest.py --dry-run

# Truncate + bootstrap without re-ingest
uv run python scripts/reset_and_reingest.py --skip-ingest

# Legacy ORM ingest path (debugging only)
uv run python scripts/reset_and_reingest.py --engine orm
```

`scripts/reset_and_reingest.py` truncates unified tables, runs `bootstrap()`, then
calls `run_vectorized()` (same engine as `cf load` default). Use `--engine orm` only
for debugging ORM parity; production reloads should use the vectorized default.

## Phase 0 / resolution manual gate

Run this sequence on a developer machine (or via `ci-resolve-integration.yml`
`workflow_dispatch`) before treating a Texas + resolution release as verified.
PR CI does **not** execute these steps.

1. **Prepare state data**
   ```bash
   uv run cf prepare texas
   ```

2. **Load source layer**
   ```bash
   uv run python scripts/loaders/production_loader.py
   ```
   Use the appropriate preset/state directory for your environment (see
   `scripts/loaders/production_loader.py` and `docs/STATES.md`).

3. **Confirm Phase 0 row counts and report linkage**
   - Non-zero rows in Phase 0 tables (`unified_reports`, pledges, lookups, etc.)
   - Transactions carry `report_id` where expected
   - Reconciliation: declared report totals vs summed transactions within tolerance
     (`reconcile_report_totals` in `app.core.source_models`; automated test when
     `test_phase0_reconciliation.py` is present)

4. **Run resolution pipeline**
   ```bash
   uv run python -m app.resolve run --state texas
   ```
   Expect a completed `match_run` and populated crosswalk/canonical staging.

5. **Publish resolved views**
   ```bash
   uv run python -m app.resolve publish --state texas
   ```

6. **Optional integration pytest**
   ```bash
   uv run pytest tests/resolve -m integration
   ```
   Skips cleanly when `tmp/texas` or Postgres is unavailable; use after steps 1–5
   when data and `DATABASE_URL` are configured.

## Debug commands

```bash
# Resolve suite with verbose failures
uv run pytest tests/resolve -m "not integration" -v --tb=short

# Golden-set / match quality (requires tracked CSVs under tests/resolve/golden/)
uv run pytest tests/resolve/test_match_quality.py -v

# Phase integration tests (SQLite fixtures; no full Texas load)
uv run pytest tests/resolve/test_phase0_integration.py tests/resolve/test_phase1_integration.py -v
```

## Common issues and fixes

### Issue: Resolve publish fails on `invalid input value for enum entitytype`

**Symptoms:**
- Survivorship crashes inserting `canonical_entity` with `entity_type='committee'`
- Postgres error: `invalid input value for enum entitytype: "committee"`

**Fix:**
- Ensure `app/resolve/models/canonical.py` uses `CanonicalEntityTypeType` (uppercase
  bind to shared `entitytype` enum).
- Re-run stage 7 only if stages 1–6 completed: survivorship publish for the
  existing `match_run` id.

### Issue: SQL blocking with custom `blocking_rules` in config

**Symptoms:**
- `ValueError: SQL blocking has no static key expression for rule '...'`

**Fix:**
- Default Postgres runs use whitelisted SQL rules only. For custom rules set
  `blocking_backend: python` in the resolve config JSON, or add the rule to
  `_RULE_BLOCK_KEY_SQL` in `app/resolve/blocking_sql.py`.

### Issue: Resolve CLI cannot see Postgres (`match_run` stuck, no pairs)

**Symptoms:**
- `uv run python -m app.resolve run` uses SQLite when env vars are missing

**Fix:**
```bash
uv run --env-file .env python -m app.resolve run --state texas -v
```

_Add other recurring operational issues here as they are validated (see
`AGENTS.md` → Updating the Runbook)._
