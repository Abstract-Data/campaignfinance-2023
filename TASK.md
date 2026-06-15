# TASK — Postgres COPY write-path + ingest throughput benchmark

Plan: docs/design/vectorized-ingest-plan.md · Engine: app/core/ingest_vectorized/ ·
Gate: app/core/ingest_equivalence.py

## Goal
Give the vectorized engine a PostgreSQL COPY fast-path on its write boundary, then
benchmark its throughput against the ORM loader on a real Texas slice. This is the last
milestone before any default flip (plan P5). Target: vectorized ≥ 20× the ORM loader's
rows/s, with COPY proven to produce byte-identical rows to the equivalence-gated
bulk_upsert path.

## Files in scope
- `app/core/ingest_vectorized/common.py` — `write_frame`: add a Postgres branch.
  - `conflict_cols=None` → `COPY <table> (cols) FROM STDIN` directly (psycopg2 copy_expert).
  - `conflict_cols` set → `COPY` into a `CREATE TEMP TABLE ... ON COMMIT DROP` staging
    table, then `INSERT INTO <table>(cols) SELECT cols FROM stg ON CONFLICT (...) DO
    UPDATE/NOTHING`. Static SQL only (identifiers from the SQLAlchemy Table metadata,
    never user input; no f-string interpolation of values).
  - sqlite / other dialects → unchanged (bulk_upsert / core insert). Behavior preserved.
  - Honor `VECTORIZED_DISABLE_COPY=1` to force the legacy path (so the benchmark can diff
    COPY vs bulk_upsert on the same backend).
- `scripts/benchmarks/bench_ingest.py` (new) — the benchmark/evidence harness.
- (test) `tests/ingest_equivalence/test_write_frame_copy.py` — skipped unless a Postgres
  URL is available; asserts COPY path == bulk_upsert path for both insert and upsert.

## Behavior to preserve
- Equivalence harness stays the gate: COPY must produce identical rows to bulk_upsert.
- No `map_elements`/`.apply`; no f-string SQL (csv serialization + static COPY/INSERT).
- All existing sqlite-backed equivalence tests stay green (COPY branch is PG-only).

## Checks / evidence required before "done"
1. `uv run pytest tests/ingest_equivalence -q` → still green on sqlite (37+).
2. COPY correctness on real data: vectorized into two PG DBs (COPY on vs off),
   `diff_snapshots(resolve_fks=True)` over all tables == [] (the strongest COPY check).
3. Benchmark on a real Texas slice (all record types): print ORM rows/s, vectorized
   (COPY) rows/s, and the speedup. Headline number recorded here.
4. ruff clean on changed files.

## Results (DONE)
- COPY fast-path in `write_frame` (psycopg2 COPY direct insert; COPY→staging→INSERT…ON
  CONFLICT for upserts). `_inject_auto_columns` generalized to materialize ALL Python-side
  column defaults (the COPY path bypasses SQLAlchemy, so NOT-NULL defaults like
  `last_modified_at`, `is_forgiven` must be filled explicitly).
- COPY correctness: `tests/ingest_equivalence/test_write_frame_copy.py` (PG-gated) proves
  COPY == bulk_upsert deterministically for plain insert (NULLs/commas/quotes) AND
  ON CONFLICT DO UPDATE. sqlite suite: 39 passed. ruff clean.
- Benchmark (`scripts/benchmarks/bench_ingest.py`, real Texas slice, 69,552 source rows,
  all record types):
  - ORM loader:        434.4s → **160 rows/s**
  - vectorized + COPY: 11.1s  → **6,273 rows/s**  → **39.2× speedup** (target was ≥20×)
  - COPY vs bulk_upsert (psycopg2 executemany): 1.3× at this scale.

## Critical findings — BLOCKERS for a Postgres default-flip (NOT introduced here)
The benchmark proved the vectorized engine cannot currently complete a real **Postgres**
load with constraints enforced (the sqlite equivalence tests never exercised the PG-only
partial unique indexes / one-to-one uniques). All pre-existing, identical under COPY and
bulk_upsert:
1. **Dim dedup ≠ PG partial-unique semantics**: in-frame dedup misses case-folded orgs
   (`uix_persons_org_state` on `lower(organization)`) and address variants → duplicate
   persons/addresses (vec built 6,145 addresses vs ORM 2,803 with indexes relaxed).
2. **`unified_entities.person_id`/`committee_id` one-to-one violated**: one representative
   person assigned to >1 entity (`_apply_entity_links`).
3. **Campaigns not built**: ORM `_link_after_load` builds `unified_campaigns` /
   `unified_campaign_entities` (556 each); the vectorized engine builds 0.
The benchmark relaxes these constraints (`_relax_constraints`) to measure throughput.

## Next
Before any P5 default-flip: (a) make dim dedup match the PG partial-unique-index semantics
(case-folded org, address keys) so the engine loads PG with constraints ON; (b) enforce the
entity one-to-one representative; (c) add a vectorized campaign-build family. Then re-run the
benchmark with constraints ENFORCED and gate `diff_snapshots(ORM, vec) == []` on a real slice.
