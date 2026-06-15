# TASK — Vectorized flat_txns detail/junction family (+ harness determinism fix)

Plan: docs/design/vectorized-ingest-plan.md · Gate: app/core/ingest_equivalence.py

## Goal
Land the vectorized **flat_txns detail/junction** family (RCPT/EXPN →
unified_contributions / unified_expenditures / unified_transaction_persons) with real
surrogate-id linkage, gated row-for-row against the ORM loader under `resolve_fks=True`.
This is the conflict-resolved integration of PR #41 onto the post-#40 base (its redundant
harness edit dropped; #40's superset harness kept).

## Files in scope
- `app/core/ingest_vectorized/families/flat_txns_detail.py` (new) — pure-Polars worker,
  priority 11; contributions/expenditures/junction; FKs filled by id-joins against the
  already-written dim + transaction tables (parameterized SQLAlchemy core; no f-string SQL;
  no map_elements/.apply).
- `app/core/ingest_vectorized/families/__init__.py` — register the family.
- `app/core/ingest_equivalence.py` — `_PRESENCE_ONLY_FKS`: reduce the non-deterministic
  `unified_entities.person_id` (entity's REPRESENTATIVE person, flush/hash-seed dependent)
  to a presence marker in `resolve_fks` output. Fixes pre-existing flakiness that the merged
  detail_children gate (#40) only passed under a lucky hash seed. Entity identity + address
  and the junction's DIRECT participant person stay strictly compared.
- `tests/ingest_equivalence/test_flat_txns_detail_family.py` (new) — the gate (relies on the
  harness fix; bespoke per-test canonicalization removed).

## Behavior to preserve
- Linkage mirrors the ORM builders/processor exactly (contributor/recipient/payer/payee
  entity resolution; junction person+entity).
- No map_elements/.apply; no f-string SQL; FK-ordered writes; shared foundation reused.

## Checks (green)
- `uv run pytest tests/ingest_equivalence -q` → 35 passed, deterministic across
  PYTHONHASHSEED in {0, 12345, 999}.
- `diff_snapshots(resolve_fks=True)` over (unified_contributions, unified_expenditures,
  unified_transaction_persons) == [], both sides non-empty.
- ruff clean; no map_elements/.apply in the family.

## Done when
flat_txns_detail registered + gated green deterministically, harness determinism fix in,
code-review clean, PR opened.

## Next — last family
cand (CAND enrichment: candidate<->expenditure link). Now unblocked — expenditures exist via
this family. Fan it out as the final gated PR, then run the Postgres COPY throughput
benchmark before any default flip.
