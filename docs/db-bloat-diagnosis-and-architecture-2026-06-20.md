# DB Bloat — Diagnosis & Target Architecture (2026-06-20)

How ~20 GB of source files becomes ~70 GB in Postgres, why, and how to do everything
the pipeline needs without that 70 GB resident in Postgres.

> Note on measurement: the two existing baselines (`docs/db-bloat-baseline-2026-06-17.md`,
> `-19.md`) were captured against a near-empty test database (15 MB total, all tables 0
> live tuples), so they don't show the 70 GB. Run `scripts/db_bloat_triage.sql` against the
> **full** local DB to confirm the byte breakdown below. The diagnosis is derived from the
> schema and loader code, which is sufficient to explain the amplification.

---

## Part 1 — Debug Report: where the bytes go

### Reproduction
- **Expected:** loading ~20 GB of source CSVs yields a database of roughly the same order.
- **Actual:** the database grows to ~70 GB (~3.5×).
- **Scope:** Texas dominates — **41.5 M source rows across 134 CSVs** (~39.8 M of them
  contributions + expenditures), plus FEC (~2 GB) and Oklahoma (~0.5 GB).

### Root cause — five compounding amplifiers

The schema is a normalized OLTP model (SQLModel class-table inheritance + full surrogate
keys + change-tracking + history tables) applied to a bulk analytical dataset. Each of the
following multiplies the on-disk footprint; they stack.

**1. Row fan-out from class-table inheritance (~3–4× the row count).**
One source transaction is stored as a parent `unified_transactions` row **plus** a subtype
child (`unified_contributions` / `unified_expenditures` / `unified_loans` / …), **plus**
one or more `unified_transaction_persons` link rows, and it references deduped
`unified_persons` / `unified_entities` / `unified_addresses`. 40 M source rows become
~120–160 M physical rows. Every extra row pays Postgres's ~28-byte tuple header + line
pointer before any data.

**2. Index amplification (indexes ≈ 1.5–2.5× the heap).**
`unified_transactions` carries **~16 indexes** (`scripts/db_bloat_triage.sql` step 3 lists
them). Some are outright redundant — e.g. `ix_unified_transactions_transaction_type`
**and** `idx_transactions_type` both index `transaction_type`; `idx_transactions_id` and
`ix_transactions_source_id` overlap. On a 40 M-row table, each secondary index is hundreds
of MB to a few GB. The baseline's `idx_scan` columns show many of these indexes are never
used.

**3. `uuid String(36)` surrogate + UNIQUE index on every table.**
Every major table has `uuid: str = Field(..., unique=True, index=True)` in addition to the
integer PK. That's ~37 bytes in the heap **and** a ~50–60-byte unique-btree entry per row,
on every one of ~150 M rows. The UUIDs aren't the join keys — they're pure overhead here,
and they're stored as text rather than the native 16-byte `uuid` type.

**4. Entity-resolution materialization (a second full copy + a pairwise blow-up).**
Resolution re-materializes every resolvable entity into `resolution_input` (~40 M rows,
~8 indexes incl. phonetic/zip blocking keys), then generates `candidate_pairs` /
`scored_pairs`. Blocking is quadratic within a block, so pair tables can exceed the input.
This is a large slice of the 70 GB and is *intermediate* data that need not be durable.

**5. Append semantics + MVCC dead tuples (the multiplier that makes it unbounded).**
This is the smoking gun, and it's **pipeline-wide, not just transactions**. Nearly every
fact and dimension write passes **`conflict_cols=None`**, which in `common.py` takes the
direct-COPY path with **no `ON CONFLICT`** — i.e. plain append:

- `flat_txns.py:306,321` → `UnifiedTransaction`
- `flat_txns_detail.py:871,900,936` → `UnifiedContribution`, `UnifiedExpenditure`, `UnifiedTransactionPerson`
- `detail_children/builders.py` → `UnifiedLoan`, `UnifiedDebt`, `UnifiedCredit`, `UnifiedTravel`, `UnifiedAsset`, `UnifiedPledge`, `LoanGuarantor`
- `detail_children/dims.py`, `filer.py`, `cand.py` → `UnifiedAddress`, `UnifiedPerson`, `UnifiedEntity`, `UnifiedCommitteePerson`
- `campaigns.py:214,233` → `UnifiedCampaign`, `UnifiedCampaignEntity`

Only the small dimensions upsert (`UnifiedCommittee` on `filer_id`, `UnifiedReport` on
`report_ident`). So every load (or overlapping multi-file run) re-inserts the entire fact
+ dimension fan-out rather than upserting. This is exactly why migration
`0002_dedup_legacy_transactions` exists ("Pre-Wave-2 loads produced those duplicates"), and
why `flat_txns_dims.py:893` notes the creator "wrote with conflict_cols=None and NO
anti-join." Re-running the load doesn't replace data — it stacks it. And because the churn
leaves dead tuples, autovacuum reclaims space to the free-space map but **never shrinks the
file** without `VACUUM FULL`, so on-disk size reflects the high-water mark of every load
you've ever run.

### Why 20 GB → 70 GB is fully consistent
A single clean load already lands around 45–55 GB from amplifiers 1–3 (parent + child +
link rows, each with their indexes and a text UUID). Resolution (4) pushes past 70 GB.
If the load has been run more than once without truncating (5), the fact tables physically
duplicate on top of that. Step 5 of the triage SQL (`total_rows` vs `distinct_source_keys`)
will tell you how much of your current DB is literal duplicate rows.

---

## Part 2 — Immediate triage (stop the bleeding, no redesign)

These recover most of the space within the current schema. Run the triage SQL first to
quantify, then:

1. **Make the load idempotent across every family, not just transactions.** Replace
   `conflict_cols=None` with the natural source key on each write — transactions on
   `["state_id","transaction_type","transaction_id"]` (the columns behind
   `uix_transactions_state_type_sourceid`), and the same pattern for the subtype children,
   persons, entities, addresses, and links — with `update_cols=[]` for first-write-wins.
   Re-runs then upsert instead of append. This is the single highest-leverage fix: it caps
   the database at one copy of the data. *Run `gitnexus_impact` on the `write_frame` callers
   before editing — they're on the hot ingest path.*
2. **Drop redundant / never-scanned indexes.** Remove the duplicate `transaction_type` and
   `transaction_id` indexes and any `idx_scan = 0` index that isn't enforcing a constraint
   (triage steps 3–4). Expect a multi-GB drop on `unified_transactions` alone.
3. **Reclaim dead space once:** `VACUUM (FULL, ANALYZE) unified_transactions;` (and the
   other large tables), or `pg_repack` to avoid the exclusive lock. This is what actually
   returns bytes to the OS after dedup.
4. **Treat resolution tables as ephemeral:** `TRUNCATE resolution_input, candidate_pairs,
   scored_pairs` between runs (or build them in a scratch schema you drop). They're
   rebuildable from the durable tables.
5. **Switch `uuid` columns to native `uuid` type** (16 B vs 37 B + smaller index), or drop
   the column where the integer PK already suffices. Schema change → Alembic revision.

---

## Part 3 — Target Architecture: do the work without 70 GB in Postgres

### Key insight
There is **no FastAPI / serving layer in `app/`** — Postgres is currently a *processing*
store, not a query-serving one. And the stack already ships **Polars + DuckDB + PyArrow**
(`pyproject.toml`), with resolution already running on DuckDB (`score_splink_duckdb.py`).
So the bulk facts don't need to live in Postgres at all. Split storage by job:

```
            ┌──────────────┐   download/validate   ┌─────────────────────────┐
  state     │  raw CSVs    │ ────────────────────▶ │  STAGING  (ephemeral)    │
  portals   │  (~20 GB)    │     Polars/pyarrow     │  Parquet on local disk   │
            └──────────────┘                        │  partitioned by          │
                                                    │  state / type / year     │
                                                    └───────────┬─────────────┘
                                                                │ DuckDB reads Parquet
                                                                │ directly (zero-copy,
                                                                │ columnar, compressed)
                                   ┌────────────────────────────▼─────────────────────┐
                                   │  PROCESSING  (DuckDB, in-process)                  │
                                   │  • normalize / fan-out logic                       │
                                   │  • Splink entity resolution (already DuckDB)       │
                                   │  • candidate_pairs / scored_pairs live HERE,       │
                                   │    in a temp DuckDB file, dropped after the run     │
                                   └────────────────────────────┬─────────────────────┘
                                                                │ publish ONLY the
                                                                │ resolved/canonical layer
                                   ┌────────────────────────────▼─────────────────────┐
                                   │  SERVING  (Postgres — small)                       │
                                   │  • canonical_entity / crosswalks / canonical_*     │
                                   │  • curated marts the app/users actually query      │
                                   │  • indexed for the queries you really run          │
                                   └────────────────────────────────────────────────────┘
```

### What lives where
- **Staging = Parquet on disk, not Postgres.** Columnar + dictionary/RLE compression
  typically stores this data in **3–6 GB** vs 20 GB of CSV and vs ~50 GB of Postgres heap.
  No tuple headers, no surrogate UUIDs, no per-row index entries. DuckDB queries Parquet in
  place — you don't "load" it anywhere.
- **Processing = DuckDB, in-process.** The expensive transient tables (`resolution_input`,
  `candidate_pairs`, `scored_pairs`) become DuckDB temp tables in a scratch `.duckdb` file
  that you delete when the run ends. They never touch Postgres, never leave dead tuples.
- **Serving = Postgres, only the canonical/resolved layer.** That's the data that benefits
  from relational integrity, concurrent access, and an eventual API — and it's a small
  fraction of the 150 M raw rows (deduped entities + crosswalks + curated marts). Index
  *that* for the handful of access patterns you actually serve.

### Why this is a small change, not a rewrite
You already have the hard parts: Polars ingest, a DuckDB Splink path, and a normalized
target schema. The move is to (a) land validated source as Parquet instead of COPYing raw
facts into Postgres, (b) run resolution against Parquet/DuckDB, and (c) write back only the
canonical layer. The existing `unified_*` SQLModel tables can stay as the *serving* schema
for resolved data; they just stop holding 40 M-row-times-fan-out raw facts.

### Trade-offs (made explicit)
| Decision | Gain | Cost / what you give up |
| --- | --- | --- |
| Parquet+DuckDB for bulk facts | ~10× smaller footprint; faster scans; no MVCC/vacuum churn | Two engines to operate; ad-hoc SQL over raw facts is DuckDB, not psql |
| Postgres = serving layer only | Small, fast, indexable for real queries | Need a clear publish step (DuckDB → Postgres) and a contract for what's "canonical" |
| Resolution tables ephemeral | Removes the biggest transient slice from durable storage | Must re-run resolution to reproduce a run (keep `match_run` provenance) |
| Native `uuid` / drop surrogate | Smaller heap+index on every table | Alembic migration; touch code that reads `.uuid` |
| Idempotent upsert load | Re-runs stop duplicating; bounded size | Slightly slower than blind COPY (staging temp + ON CONFLICT) |

### What to revisit as it grows
- If a low-latency public API appears, the serving Postgres may need read replicas or a
  materialized-view refresh cadence — design the publish step now so that's a config change.
- If multi-state volume grows, partition the Parquet lake by `state/year` (already the
  natural key) and consider object storage (S3/R2) so DuckDB reads remotely.
- Keep `match_run` + crosswalks durable so a dropped-and-rebuilt resolution run stays
  auditable.

---

## Suggested sequence
1. Run `scripts/db_bloat_triage.sql` on the full DB → confirm duplicate rows + index split.
2. Part 2 triage (idempotent load, drop dead indexes, VACUUM FULL, truncate resolution) →
   recovers most of the 70 GB immediately, no redesign.
3. Part 3: land staging as Parquet and move resolution off Postgres → keeps it from coming
   back, and makes Postgres a small serving store.
