# Prompt: DB Bloat Remediation + Upload/Matching Efficiency
# Version: 1.0.0
# Model: claude-sonnet-4-6
# Last Updated: 2026-06-17
# Maintainer: John Eakin / Abstract Data

## Context

You are working on the **campaignfinance** Python ETL pipeline (SQLModel + Pydantic v2 +
SQLAlchemy + Alembic, Postgres 16, Polars vectorized ingest, Splink/DuckDB resolve).
The local Postgres DB bloats to **~90 GB**, which is far larger than the live data
warrants. The goal of this task is to **cut DB size**, **speed up upload + matching**,
and **preserve all linkage while avoiding duplication** — without losing any
information the pipeline actually consumes.

A diagnosis already established the root causes (verified against the code):

1. **`raw_data` provenance blob** — `unified_transactions.raw_data` and
   `unified_reports.raw_data` each store `json.dumps(...)` of the full original source
   row (~42 TEC columns) as `Text`. This is the single largest heap/TOAST consumer and
   is duplicated on every re-load.
2. **Resolve audit accumulation** — `match_decision`, `scored_pairs`, `candidate_pairs`,
   `resolution_input`, `merge_edges` are cleaned only `WHERE run_id = <current run>`, so
   every historical `run_id` persists forever. No retention policy.
3. **Over-indexing** — ~76 field-level `index=True` + ~50 explicit `Index()` = ~120+
   indexes; 19 tables carry a `unique`-indexed 36-char UUID **string** column.
4. **Dead-tuple / TOAST bloat** — delete-by-run_id + append re-loads generate dead
   tuples that Postgres never returns to the OS without `VACUUM FULL` / `pg_repack`.

**Decision already made:** DROP `raw_data` (do not move it to a cold table). Source
parquet already preserves provenance for debugging.

### Key files

- `app/core/models/tables.py` — `UnifiedTransaction` (`raw_data` at ~L385),
  `IngestError` (`raw_data` ~L1094 — **KEEP**), `UnifiedTransactionIndexes` (~L1108)
- `app/core/source_models/reports.py` — `UnifiedReport.raw_data` (~L103) +
  `committee_name_at_filing` / `treasurer_name_at_filing`
- `app/core/source_models/reports_ingest.py` — report insert (~L95-112, at-filing cols
  set at insert), `backfill_report_at_filing()` (~L160)
- `app/core/builders.py` — `build_transaction()` writes `raw_data` (~L84)
- `app/core/ingest_vectorized/common.py` — `raw_json_expr()` (~L391),
  `full_address_lookup()` (~L228-277), Postgres COPY writer (~L475-544)
- `app/core/ingest_vectorized/families/*.py` — `flat_txns.py` (~L210/224),
  `flat_txns_detail.py`, `detail_children.py` (~L996/1009), `reports.py` (~L110/131)
- `app/core/ingest_vectorized/campaigns.py` — `_transaction_frame()` (~L125-148) +
  `_office_expr()` (~L151-166): **the only consumer of `unified_transactions.raw_data`**
- `app/resolve/stages/*.py`, `app/resolve/reverse.py` — resolve staging lifecycle
- `migrations/` — Alembic; latest is `0002_dedup_legacy_transactions`
- `app/cli/` — `cf` Typer CLI

> **Project rules (CLAUDE.md): MANDATORY.** Run `gitnexus_impact({target, direction:
> "upstream"})` on **every** symbol before editing it and report the blast radius. Warn
> on HIGH/CRITICAL risk before proceeding. Run `gitnexus_detect_changes()` before every
> commit. Use `gitnexus_rename` for any rename. After committing, refresh the index with
> `npx gitnexus analyze` (add `--embeddings` if `.gitnexus/meta.json` shows
> `stats.embeddings > 0`).

---

## Objective

After this work:

- `raw_data` no longer exists on `unified_transactions` or `unified_reports`; every
  feature those columns fed is sourced another way and ingest + campaign + report
  derivation still pass.
- Resolve runs no longer accumulate unbounded audit rows; a retention command exists.
- Reclaimable dead-tuple/index/TOAST space is returned to the OS.
- The hot ingest lookups are state-scoped, not full-table scans.
- DB size is measured before and after, with a documented reduction.
- No transaction/person/committee/address linkage is broken and no new duplication is
  introduced (the existing partial-unique dedup indexes still hold).

Work in the phases below **in order**. Phases 0–3 are the high-value, low-risk core.
Phases 4–5 are follow-ups; do them only after 0–3 are verified green.

---

## Phase 0 — Measure first (no schema changes)

Capture a baseline so the win is provable. Add a small read-only helper (e.g.
`scripts/db_size_report.py` or a `cf db size` subcommand) that runs:

```sql
SELECT relname AS table,
       pg_size_pretty(pg_total_relation_size(c.oid))                              AS total,
       pg_size_pretty(pg_relation_size(c.oid))                                    AS heap,
       pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid))    AS toast_plus_idx,
       s.n_dead_tup, s.n_live_tup
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
WHERE c.relkind = 'r' AND n.nspname = 'public'
ORDER BY pg_total_relation_size(c.oid) DESC
LIMIT 30;
```

Also dump per-index size and usage for the index-pruning phase:

```sql
SELECT i.relname AS index, t.relname AS table,
       pg_size_pretty(pg_relation_size(i.oid)) AS size, s.idx_scan
FROM pg_stat_user_indexes s
JOIN pg_class i ON i.oid = s.indexrelid
JOIN pg_class t ON t.oid = s.relid
ORDER BY pg_relation_size(i.oid) DESC;
```

Save both outputs to `docs/` (e.g. `docs/db-bloat-baseline-2026-06-17.md`). Record total
DB size: `SELECT pg_size_pretty(pg_database_size(current_database()));`

---

## Phase 1 — Drop `unified_transactions.raw_data` (handle the campaign dependency first)

`campaigns.py` is the **only** consumer of the persisted column — `_transaction_frame()`
SELECTs `raw_data` from the DB and `_office_expr()` parses office/district out of it with
`json_path_match`. You must rewire this **before** dropping the column, or campaign
finalization breaks.

### 1a. Rewire campaign derivation off `raw_data`

Choose ONE (1a-i preferred):

**1a-i (preferred): derive from the in-memory source frame during ingest.** In the
`flat_txns` family pass the original parquet columns are already in memory. Compute the
office/district/campaign source values there and persist them into **narrow, nullable,
un-indexed** columns on `unified_transactions` — e.g. `campaign_office_src`,
`campaign_district_src`, `campaign_name_src` (short `String`/`Text`, NULL for rows with
no campaign data). These are kilobytes vs. the full JSON blob. Then change
`_transaction_frame()` to SELECT those three columns and `_office_expr()` to read them
directly (drop the `json_path_match` over `_raw`).

**1a-ii (alternative): inline campaign finalization.** Move `finalize_campaigns()` to run
inside the same ingest pass where the source frame is live, eliminating the DB round-trip
and the need to persist anything. Larger refactor (touches dispatcher/finalize ordering);
only take this if 1a-i proves awkward.

Verify the office/district/campaign_name values produced are **identical** to the current
`json_path_match` output on a sample state (golden compare).

### 1b. Stop writing `raw_data` on transactions

Remove `raw_data` population from:
- `app/core/builders.py` `build_transaction()` (~L84)
- `app/core/ingest_vectorized/families/flat_txns.py` (~L210, ~L224 column list)
- `app/core/ingest_vectorized/families/flat_txns_detail.py` (~L996/L1009 if present)
- `app/core/ingest_vectorized/families/detail_children.py` (~L996/L1009)

Keep `raw_json_expr()` in `common.py` **only if** still used transiently in-memory
(e.g. for `ingest_errors`); otherwise leave it but ensure nothing routes its output to a
persisted `raw_data` transaction column.

### 1c. Drop the column from the model + migration

Remove the `raw_data` field from `UnifiedTransaction` in `tables.py` (~L385). Generate an
Alembic migration:

```bash
alembic revision -m "drop unified_transactions.raw_data + add campaign source cols"
```

In `upgrade()`: `ADD COLUMN` the three narrow campaign source columns (1a-i), then
`ALTER TABLE unified_transactions DROP COLUMN raw_data;`. In `downgrade()`: re-add
`raw_data` as nullable `Text` and drop the campaign columns. (Do not attempt to
reconstruct dropped JSON on downgrade — note this is lossy in the migration docstring.)

> **Do NOT** drop `IngestError.raw_data` (tables.py ~L1094). It only holds failed rows
> and is the whole point of the error-audit table.

---

## Phase 2 — Drop `unified_reports.raw_data`

The at-filing columns it feeds are **already set at insert** in
`reports_ingest.py:110-111` (`committee_name_at_filing=raw.get("filerName")`,
`treasurer_name_at_filing=...`). The column is redundant for new rows; the only other use
is `backfill_report_at_filing()` for legacy rows.

### 2a. Guarantee both report writers populate the at-filing columns at ingest

There are **two** report writers — confirm both set `committee_name_at_filing` and
`treasurer_name_at_filing` directly from source columns (not via later JSONB backfill):
- ORM: `app/core/source_models/reports_ingest.py` (~L95-112) — already does.
- Vectorized: `app/core/ingest_vectorized/families/reports.py` (~L110/131) — **verify**;
  if it only writes `raw_data`, add the two derived columns to its output frame.

### 2b. Backfill legacy rows, then drop

Run `backfill_report_at_filing()` (or fold an equivalent `UPDATE ... raw_data::jsonb ->>`
into the migration's `upgrade()`) so any existing rows with NULL at-filing columns are
populated **before** the column is dropped. Then stop writing `raw_data` in both writers
and drop the column + field via Alembic, same pattern as Phase 1c.

---

## Phase 3 — Resolve-run retention + space reclamation

### 3a. Add a retention command

Add `cf resolve prune` (Typer) that keeps the latest `run_id` (and optionally the last N,
default 1) and deletes older rows from, in FK-safe order:
`match_decision`, `merge_review`, `scored_pairs`, `candidate_pairs`, `merge_edges`,
`cluster_assignment`, `resolution_input`, and the per-run crosswalk rows
(`entity_crosswalk`, `address_crosswalk`, `campaign_crosswalk`) for stale runs — mirror
the existing `delete(...).where(run_id == ...)` patterns in `app/resolve/reverse.py` and
`app/resolve/stages/*.py`. Keep the `match_run` header rows unless `--purge-headers`.
Make it idempotent and transactional. Print rows deleted per table.

Optionally wire an end-of-pipeline call (post-publish) so each successful run prunes
prior runs automatically, behind a `--keep N` flag.

### 3b. Reclaim space (VACUUM FULL — offline, local DB)

After Phases 1–3a, dead tuples dominate. Document a runbook (`docs/db-reclaim.md`) and/or
a `cf db reclaim` helper that runs, **largest table first** (needs free disk ~= live size
of the biggest table):

```sql
VACUUM (FULL, ANALYZE) unified_transactions;
VACUUM (FULL, ANALYZE) unified_reports;
VACUUM (FULL, ANALYZE) match_decision;
VACUUM (FULL, ANALYZE) scored_pairs;
VACUUM (FULL, ANALYZE) candidate_pairs;
VACUUM (FULL, ANALYZE) resolution_input;
-- ... then remaining large tables from the Phase 0 report
```

`VACUUM FULL` takes an exclusive lock — fine for the local/dev DB. Do not run it inside a
transaction block or via the ORM session; use `AUTOCOMMIT` / a raw psycopg connection.
(If this ever needs to run against a shared/online DB, use `pg_repack` instead — note this
in the runbook but default to `VACUUM FULL` here.)

Re-run the Phase 0 measurement and record the new total in the baseline doc.

---

## Phase 4 — Speed: state-scope the hot ingest lookups

These are full-table scans with no `state_id` filter, re-run per family, not cached:

- `app/core/ingest_vectorized/common.py` `full_address_lookup()` (~L228-277) —
  `SELECT *` over the entire `unified_addresses` table into memory. Add a `state_id`
  predicate and/or cache the result once per run on the `FamilyContext` so the dim and
  detail families reuse it instead of re-materializing.
- Per-family id-map reads (`families/filer.py` ~L248, `finalize.py` ~L49/113) — add
  `WHERE state_id = ?` so FK resolution scans only the current state, not all states.

Confirm output parity (same FK assignments) on a sample state before/after. These are
behavior-preserving performance changes; run `gitnexus_impact` on
`full_address_lookup` and the touched family `run()`/`finalize` functions first.

---

## Phase 5 — Index diet + UUID type (follow-up, do after 0–4 are green)

### 5a. Drop unused indexes

Using the Phase 0 `idx_scan` report, drop indexes with `idx_scan = 0` that are not
backing a unique/dedup constraint or FK. Be conservative: never drop the partial-unique
dedup indexes (`uix_persons_*`, `uix_addresses_*`, `uix_transactions_state_type_sourceid`,
`uix_txperson_*`, `uix_entities_*`) — they prevent duplication. Do it as an Alembic
migration so it is reversible.

### 5b. (Larger) UUID string → native `uuid`

The 19 `uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True)` columns
store 36-char text with a unique index each. Converting to Postgres native `uuid` (16
bytes) roughly halves those 19 unique indexes and their heap footprint. This is a wide,
higher-risk change (touches every model + all FKs/joins referencing `uuid`). Scope it
separately: run `gitnexus_impact` on each affected model, write a dedicated migration with
`ALTER COLUMN ... TYPE uuid USING uuid::uuid`, and verify all joins. **Treat as its own
PR.** Where an integer PK already provides identity and the surrogate `uuid` is unused,
prefer dropping the column outright over converting it.

---

## Implementation checklist

- [ ] **Phase 0:** Baseline size + per-index usage saved to `docs/`.
- [ ] **Phase 1a:** Campaign office/district/name derivation produces values identical to
      the old `json_path_match` path on a sample state (golden compare).
- [ ] **Phase 1b/c:** No code writes `unified_transactions.raw_data`; column dropped;
      Alembic up/down both run clean. `IngestError.raw_data` untouched.
- [ ] **Phase 2:** Both report writers set at-filing columns at ingest; legacy rows
      backfilled; `unified_reports.raw_data` dropped; report ingest still passes.
- [ ] **Phase 3a:** `cf resolve prune` deletes only stale `run_id` rows, FK-safe,
      idempotent; latest run intact; counts reported.
- [ ] **Phase 3b:** `VACUUM FULL` runbook/helper works; new DB size recorded and
      **materially smaller** than baseline.
- [ ] **Phase 4:** Address/id-map lookups state-scoped; FK parity verified; ingest faster.
- [ ] **Phase 5:** (if done) only zero-scan, non-constraint indexes dropped; dedup
      indexes preserved; migration reversible.
- [ ] **Linkage integrity (run after everything):**
      `SELECT COUNT(*) FROM unified_transactions WHERE committee_id IS NULL;` and
      transaction→report, transaction→person, contribution entity-direction checks are
      unchanged from before the work.
- [ ] **No new duplication:** row counts for `unified_persons`, `unified_addresses`,
      `unified_entities`, `unified_transactions` are unchanged after a re-ingest of the
      same source files (dedup indexes still hold).
- [ ] Full test suite green (`pytest`); ruff clean.

---

## Notes / guardrails

- **Order matters:** rewire/derive-replacement (1a, 2a) lands and is verified **before**
  the column drop in the same migration. Never drop a column another module still reads.
- Run `gitnexus_impact` upstream on: `build_transaction`, `_transaction_frame`,
  `_office_expr`, `finalize_campaigns`, `full_address_lookup`, the report insert builder,
  and any family `run()` you touch. Report blast radius; stop and warn on HIGH/CRITICAL.
- Each Alembic migration must have a working `downgrade()` (lossy re-add of `raw_data` as
  nullable `Text` is acceptable — document it).
- Run `gitnexus_detect_changes()` before each commit; confirm scope matches expectation.
- After committing, refresh the GitNexus index (`npx gitnexus analyze`, `--embeddings` if
  present).
- Keep changes in reviewable slices: one PR per phase (Phase 5b strictly separate).
- Do not touch `.claude/`, `CLAUDE.md`, or `.git/` write-protected paths.
