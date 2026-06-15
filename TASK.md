# TASK — Vectorized flat_txns detail/junction family (RCPT/EXPN)

Plan: docs/design/vectorized-ingest-plan.md · Gate: app/core/ingest_equivalence.py

## Goal
Extend the vectorized ingest engine with the flat_txns detail/junction family:
RCPT → `unified_contributions`, EXPN → `unified_expenditures`, and the
`unified_transaction_persons` junction, with entity/person FK linkage filled by
real surrogate-id joins against the already-written dim + transaction tables.

## Files in scope
- `app/core/ingest_vectorized/families/flat_txns_detail.py` (NEW)
- `app/core/ingest_vectorized/families/__init__.py` (import new module)
- `tests/ingest_equivalence/test_flat_txns_detail_family.py` (NEW)

## Behavior to preserve
- Pure Polars column expressions; NO `map_elements` / `.apply()`.
- No f-string / concatenated SQL — SQLAlchemy core parameterized `select` only.
- Mirror ORM builder/processor semantics EXACTLY:
  - Contribution created only when contributor entity AND recipient (committee)
    entity both exist; expenditure only when payer (committee) AND payee entity
    both exist.
  - transaction_persons: one row per non-null participant, RECIPIENT excluded
    (RCPT→CONTRIBUTOR, EXPN→PAYEE); entity_id = person.entity.id.
  - contribution_type / expenditure_type unmapped → None; is_anonymous default.
  - amount/date/description copied from the parent transaction row.
- Priority runs AFTER flat_txns_dims (9) and flat_txns (10) → priority 11.

## Checks to run (evidence for done)
1. `uv run pytest tests/ingest_equivalence -q` → all green (existing + new test).
2. New test: `diff_snapshots` over
   (`unified_contributions`,`unified_expenditures`,`unified_transaction_persons`)
   with `resolve_fks=True` == `[]`; both sides non-empty.
3. `grep -rnE "map_elements|\.apply\(" app/core/ingest_vectorized/families/flat_txns_detail.py` → empty.
4. `uv run ruff check app/core/ingest_vectorized tests/ingest_equivalence` → clean.
