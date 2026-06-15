# TASK — Vectorized FILER family (authoritative committees + officers)

Plan: docs/design/vectorized-ingest-plan.md · foundational committee-identity gap.

## Problem
The vectorized engine never ingests `filers_*.parquet`, so committees are created
incidentally from transaction `filerName` — missing the authoritative committee
name / type / status / address AND all committee officers (treasurer / assistant
treasurer / chair). This blocks correct campaign names downstream.

## Files in scope
- `app/core/ingest_vectorized/families/filer.py` (NEW) — `FilerWorker`
  (`record_types = {"FILER"}`, priority 0 so it runs FIRST). Produces
  `unified_committees` (authoritative fields + address), `unified_committee_persons`
  (officers), officer `unified_persons`, officer `unified_entities`.
- `app/core/ingest_vectorized/families/__init__.py` — register `filer`.
- `app/core/ingest_vectorized/families/detail_children.py` — committee write
  -> DO NOTHING (`update_cols=[]`) so FILER committee wins.
- `app/core/ingest_vectorized/families/flat_txns_dims.py` — committee write
  -> DO NOTHING (`update_cols=[]`) so FILER committee wins.
- `app/core/ingest_vectorized/common.py` — `write_frame`: distinguish
  `update_cols=[]` (DO NOTHING) from `update_cols=None` (DO UPDATE all).
- `app/core/upsert.py` — `bulk_upsert`: `update_cols=[]` -> `on_conflict_do_nothing`.
- `tests/ingest_equivalence/test_filer_family.py` (NEW) — full-fixture FILER gate.

## Behavior to preserve / side-preference rule
- FILER committee row wins over incidental transaction `filerName`: committee
  upserts in non-FILER families become ON CONFLICT DO NOTHING (first-occurrence
  wins + FILER ran first). Mirrors the ORM find-or-create.
- Officer emit rule: emit a `unified_committee_persons` row + its person/entity
  ONLY when the officer name (first/last/org) is present (mirrors `_upsert_officer`).
- Officer person/entity dedup uses the SAME `(entity_type, normalized_name, state)`
  key and `collapse_org_person_key` as contributor/payee dims, so an officer who is
  also a contributor collapses to one entity/person.
- Golden-fixture dim tables (addresses/persons/entities) stay green.
- Existing `write_frame` tests stay green; the DO-NOTHING option is covered by a new
  `write_frame` test.

## HARD RULES
- Pure Polars (no `map_elements`/`.apply`); parameterized SQLAlchemy core /
  `common.write_frame`; NO f-string SQL. Reuse `common.py`. Plain git. No rm -rf.

## Checks (evidence required before "done")
1. `uv run pytest tests/ingest_equivalence -q` -> all green.
2. NEW `tests/ingest_equivalence/test_filer_family.py`: load the FULL golden fixture
   set via the ORM loader AND via `run_vectorized`; assert
   `diff_snapshots(ORM, vec)` for `("unified_committees","unified_committee_persons")`
   == [] with `resolve_fks=True`, both sides non-empty.
3. `grep -rnE "map_elements|\.apply\(" app/core/ingest_vectorized/families/filer.py`
   -> none.
4. `uv run ruff check app/core/ingest_vectorized tests/ingest_equivalence` -> clean.
5. code-review skill run; findings fixed.

## Cannot run (report, do not fake)
- Real-Texas full load (no `tmp/texas` in this worktree): committees gaining
  authoritative FILER names + officers with constraints enforced. Deferred to parent.
