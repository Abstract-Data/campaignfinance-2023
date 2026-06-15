# TASK — Vectorized detail_children family (LOAN/DEBT/CRED/TRVL/ASSET/PLDG)

## Goal
Extend the foundation-first vectorized ingest engine with the `detail_children`
family: a Polars-vectorized, row-for-row-equivalent (to the ORM loader) transform
for TEC record types LOAN, DEBT, CRED, TRVL, ASSET, PLDG — including the dim rows
they imply, their `unified_transactions` rows, their detail child rows, and
`loan_guarantors` for LOAN/DEBT.

## Files in scope
- `app/core/ingest_vectorized/families/detail_children.py` (new) — the family worker.
- `app/core/ingest_vectorized/families/__init__.py` — register the family (import).
- `app/core/ingest_equivalence.py` — harness fixes so `resolve_fks=True` works for
  id-less target tables (`unified_committees`) and gates `loan_guarantors`.
- `tests/ingest_equivalence/test_detail_children_family.py` (new) — the gate.

## Behavior to preserve
- Pure Polars column expressions only. NO `map_elements`, NO `.apply()`. Guarantors
  via `struct`/`explode`.
- No f-string / concatenated SQL — SQLAlchemy core parameterized/reflected only.
- Existing families (`reports`, `flat_txns`, `flat_txns_dims`) must still pass their
  gates in a full `run_vectorized` over the golden fixtures (shared dims must not
  collide — dim writes anti-join existing rows).
- ORM builder semantics replicated exactly: per-type field-resolution order
  (`_get_field_value`), `builder_date`/`builder_amount` dialect + fallbacks, debt/
  asset date fallback to `receivedDt`, travel amount/date from `parentAmount`/
  `parentDt`, travel `traveler_person_id` always NULL (traveller is a PAYEE role,
  detail reads CONTRIBUTOR), PLDG via `build_pledge` (NULL pledgor/recipient entity),
  detail rows skipped when the required entity is absent (loan/debt/credit).

## Checks to run
1. `uv run pytest tests/ingest_equivalence -q` — all green.
2. The detail_children gate: `diff_snapshots(resolve_fks=True)` restricted to the
   family's tables (the 6 detail tables + `loan_guarantors` + `unified_transactions`
   filtered to these record types) == `[]`, both sides non-empty.
3. `grep -rnE "map_elements|\.apply\(" app/core/ingest_vectorized/families/detail_children.py`
   → no code matches (docstring mention only).
4. `uv run ruff check app/core/ingest_vectorized tests/ingest_equivalence` → clean.

## Done when
All four checks pass and the family is registered (runs after the dim family,
priority 11) so a full vectorized run produces its dims + transactions + details
with FK linkage verified under `resolve_fks=True`.
