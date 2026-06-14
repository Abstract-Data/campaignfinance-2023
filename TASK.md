# TASK — Address 25%-subset test findings (2026-06-14)

## Goal
Fix the four findings surfaced by the 2026-06-14 subset end-to-end test, in
priority order. Each fix must be reflection/idempotency-safe and run identically
on sqlite (tests) and postgres (real).

## Findings & acceptance criteria

### #1 Schema drift / no migration  (HIGH)
- **Problem:** wave-2 (`unified_reports.committee_name_at_filing`,
  `treasurer_name_at_filing`) and wave-3 (`canonical_entity.employer`) columns
  exist only via a fresh `create_all`; existing DBs break on insert. Unified layer
  has no additive-column shim; resolve's `_ADDITIVE_COLUMNS` omits `employer`.
- **Fix:** add a unified additive-column shim (mirror
  `app/resolve/run.py::_ensure_additive_columns`) invoked from
  `UnifiedDatabaseManager.bootstrap()`; add `canonical_entity.employer` to resolve
  `_ADDITIVE_COLUMNS`.
- **Files:** `app/core/unified_database.py`, `app/resolve/run.py`.
- **Done when:** on a DB pre-created WITHOUT the new columns, bootstrap/ensure adds
  them (idempotent re-run = no-op); report + canonical inserts succeed. New tests.

### #3 CAND duplicate rejects  (MED) — DONE (enrichment linkage)
- **Root cause:** CAND records are candidate↔expenditure *linkages*, not standalone
  transactions. `expendInfoId` is the EXPENDITURE id; cand has 62K internal dup
  expendInfoIds and overlaps the expend files, so loading as EXPENDITURE double-counts
  and collides on the dedup index.
- **Decision (user):** proper enrichment linkage.
- **Implemented:** CAND removed from `TRANSACTION_RECORD_TYPES`; new
  `ENRICHMENT_RECORD_TYPES` + `_persist_cand_link` in `scripts/loaders/production_loader.py`
  resolves the candidate person and attaches `UnifiedTransactionPerson(role=CANDIDATE)`
  to the matching expenditure (idempotent on the natural key; unlinked when the
  expenditure isn't in the load).
- **Evidence (subset_load2):** rejected=0 (was 556), ingest_errors=0, 565 candidate
  links (328 distinct candidates), no duplicate EXPENDITURE rows. 6 new unit tests.

### #2 Bulk loader throughput  (LARGE) — DONE (bounded fix + scope note)
- **Problem:** per-row ORM persist ~111 txn/s; full load infeasible.
- **Implemented:** raised the production preset to batch_size=2000/commit_frequency=2000
  (was 500/20 — one fsync per 20 rows). Measured 74→111 rows/s going cf 20→5000 on
  the subset run; this captures the I/O batching win.
- **Scope note (deferred):** the ~111 rows/s ceiling is CPU-bound per-row ORM object
  construction (UnifiedTransaction + persons + detail + entity/address dedup). Only a
  COPY-based bulk path removes it (a real rewrite: flatten the object graph, manage
  ids, type-specific COPY). Documented in the loader_config comment. NOT attempted
  here.
- **Files:** `scripts/loaders/loader_config.py`.

### #4 address_occupancy empty  (MED) — DONE
- **Root cause:** `canonical_address_id` was only populated by a separate
  `--pass-type address` invocation; the default entity run left it NULL, so the
  occupancy view was always empty.
- **Implemented:** appended an address-link stage (stage 8) to the entity pass
  (`_run_address_link_stage` in `app/resolve/cli.py`) that builds canonical_address
  + backfills the FK. Returns non-`_COUNTER_COLS` keys so it does NOT clobber
  survivorship's `canonical_out` in match_run.
- **Evidence (run_id=11):** single entity run → canonical_address 22,498,
  canonical_entity.canonical_address_id set on 27,260; match_run.canonical_out=68,073
  (entity count, not addr); after publish, address_occupancy = 27,260 rows. New tests.

## Checks to run (evidence required before done)
- `uv run ruff check` clean on changed files.
- `uv run pytest` targeted suites green (core loader, resolve run, survivorship).
- Re-run the subset ingest + resolve to confirm #1/#3/#4 behave on real data.
- task-critic PASS.

## Behavior to preserve
- sqlite test path unchanged where possible; all DDL reflection-guarded + idempotent.
- No change to existing column semantics; additive only.
