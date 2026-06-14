# Vectorized Ingest Rewrite — Implementation Plan

Status: DRAFT (2026-06-14)
Related: `docs/adr/0004-elt-unify-spike.md` (ELT/set-based spike — the prior art)

## Problem & goal

The current loader builds one graph of ORM objects per source row
(`UnifiedTransaction` + person + entity + address + `UnifiedTransactionPerson` +
detail + guarantor children), each needing find-or-create lookups. Measured
ceiling: **~111 rows/s end-to-end, ~492 rows/s CPU floor** even cache-warm with no
writes. Full TX (~41.5M rows) ≈ **~4 days** to ingest; 25% ≈ **~25 h**.

**Goal:** a parallel, vectorized ingest engine that does the same work columnar
(Polars expressions in Rust) + bulk-loads via Arrow/COPY, targeting **10k–100k
rows/s** (full load in ~10–60 min). It must be **row-for-row equivalent** to the
current builders before it can replace them.

**Non-goals:** speeding up resolve (separate; pipeline becomes resolve-bound after
this). Changing the unified schema or dedup keys. Removing the ORM loader until the
equivalence gate is green (both ship side by side first).

## Architecture (one lazy query per source file)

```
pl.scan_parquet(file)                       # lazy, no load
  .with_columns(<parse exprs>)              # str/when/cast — replaces builders' field parse
  # dims (dedup + upsert + id map):
  #   persons/entities/committees/addresses resolved via unique() + ON CONFLICT upsert + join
  .join(committee_id_map, left_on="filerIdent", right_on="filer_id", how="left")
  .join(report_id_map,    left_on="reportInfoIdent", right_on="report_ident", how="left")
  .with_row_index("id", offset=<MAX(id)+1>)  # surrogate ids
  → write: ADBC write_database(engine="adbc")  OR  sink_csv → Postgres COPY, in FK order
```

Streaming throughout: `collect(engine="streaming")` / `sink_*` so we never hold a
file in RAM. **Hard rule: zero `map_elements`/`apply` in the hot path** — every
transform is a native expression, or it becomes the new ~500/s bottleneck.

### Write strategy
- Bulk path = Arrow-native: `DataFrame.write_database(table, engine="adbc")` (Arrow
  → COPY), or `sink_csv` → `psql COPY` (the pattern already in
  `app/resolve/stages/score.py`). NOT SQLAlchemy `executemany`.
- Insert in FK order: states → committees → reports → persons/entities/addresses →
  transactions → transaction_persons → detail tables → guarantors.

### Dedup / id allocation (the hard part)
- Build dimension frames with `unique(subset=<natural key>)`.
- Upsert dims first: `INSERT … ON CONFLICT DO NOTHING` against the existing unique
  indexes (`uix_persons_*`, `uix_entities_*`, `uix_addresses_*`,
  `uix_committees`…), then **read back** the id map and `join` it to assign FKs.
- Cross-file + re-run idempotency rides on those same natural-key indexes (same
  guarantee #2/#3 used). Transactions upsert on
  `uix_transactions_state_type_sourceid`.

## File-by-file mapping (builders → expressions)

| Current (row-by-row) | Vectorized replacement |
|---|---|
| `builders._parse_person_name` (l.122) | `str.strip_chars`/`strip_suffix`/`split` exprs producing first/last/suffix/org/full_name cols |
| `builders._parse_address_parts` (l.150) | `str` exprs → line_1/city/state/zip cols + normalized address key |
| `builders._parse_amount` (l.826) | `str.replace_all(r"[$,]","").cast(Decimal)` |
| `builders._parse_date` (l.845) | `str.to_date(fmt, strict=False)` |
| `builders._parse_boolean` (l.881) | `pl.col(x).is_in(["Y","y","true",...])` |
| `builders._determine_transaction_type` (l.897) | `when(recordType.is_in(...)).then(...)` chain; CAND routed out (enrichment) |
| `builders.build_person`/`build_committee`/`build_address` find-or-create | `unique()` dims + ON CONFLICT upsert + join (no per-row SELECT) |
| `builders._get_or_create_entity`/`_find_*` (l.470–826) | dim upsert + id-map join |
| `processor._build_participants`/`_attach_transaction_persons` (RECORD_TYPE_ROLE_MAP) | build a `transaction_persons` frame per role via the role map, concat |
| `processor.DETAIL_BUILDERS` (l.359) | `partition_by(transaction_type)` → per-subtype detail frame projection |
| `processor._build_guarantors` (l.119) | `struct` over guarantor blocks → `explode` into guarantor frame |
| `raw_data` JSON provenance | `pl.struct(all_cols).struct.json_encode()` column |
| CAND enrichment (`_persist_cand_link`) | join cand frame → expenditure id map → transaction_persons(role=CANDIDATE) frame |

Field mappings come from `app/core/unified_field_library.py` (already declarative —
drive the rename/select from it).

## Phases

- **P0 — Equivalence harness (FOUNDATION, do first).** Golden fixtures (a few k rows
  per record type incl. DEBT/TRVL/CAND/guarantor edge cases). Run the ORM loader →
  snapshot every unified table; provide a `diff_tables(orm, vectorized)` that asserts
  row-for-row equality (modulo ids/uuids/timestamps). This is the merge gate; nothing
  vectorized ships without it green.
- **P1 — Reference dims** (committees/reports/lookups) vectorized + ADBC write.
  Smaller, FK-root, validates the dim-upsert + id-map pattern.
- **P2 — Flat transactions** (RCPT/EXPN) end-to-end: parse → dims → joins → COPY,
  incl. transaction_persons. Hit the equivalence gate.
- **P3 — Detail + children** (LOAN/DEBT + guarantors, CRED/TRVL/ASSET/PLDG) via
  struct/explode + partition_by.
- **P4 — CAND enrichment** vectorized (join to expenditure id map).
- **P5 — Orchestration + flag:** new engine behind `--engine=vectorized`; run both on
  a full subset, diff, benchmark; flip default only when equivalent + faster.

## Risks
1. **Semantic equivalence** — builder edge cases (placeholder names, suffix
   handling, type inference). Mitigated by P0 harness + the row-diff gate.
2. **`map_elements` creep** — any UDF fallback kills the gain. Gate: grep the new
   module for `map_elements`/`apply`; CI fails if present in hot path.
3. **Id allocation under concurrency / re-run** — rely on natural-key upserts +
   read-back, never client-side max-id races.
4. **ADBC availability** — fallback to `sink_csv` + `COPY` if the ADBC driver isn't
   present in the target env.

## Acceptance
- P0 harness green: vectorized output == ORM output, row-for-row, on the golden set.
- Full-subset run: 0 unexpected `ingest_errors`; counts match the ORM path.
- Benchmark: ≥ 20× ingest throughput vs the row-by-row loader (target 10k+ rows/s).
- ruff clean; no `map_elements` in the ingest hot path.

## Effort & parallelization
- Rough order: **2–4 focused weeks**. P0 (~3–5 d) is sequential and on the critical
  path. P1–P4 are **independent behind the harness** (parallelizable). P5 sequential.
