# TASK — Tighten the individual person dedup key (name + address)

User-AUTHORIZED pipeline change (incl. schema column + dedup-index change).
Measured: 78% of contributor records are over-merged because distinct-location
same-name people collapse to one person at ingest. Tighten the individual person
dedup key so the downstream resolve/Splink stage does the real merging.

## New dedup semantics (ORM is the contract; vectorized engine MUST match)
- Individual person dedup key -> `(lower(first), lower(last), state, address_key)`
  where `address_key = BuilderCache.address_key(address_dict)` (≥2 of
  street/city/state/zip populated, lowercased; None when too few fields -> FALL
  BACK to name-only, today's behavior, so addressless persons don't fragment).
- Org-persons UNCHANGED: `(lower(org), state)` with NULL `dedup_addr_key`.
- `dedup_addr_key` = denormalized "street|city|state|zip" (the address_key tuple
  joined by `|`) or NULL.

## Files in scope
- `app/core/load_cache.py` — `person_key(... , address_key=None)`; add
  `address_key_str(address_dict)` denormalizer used by both engines.
- `app/core/models/tables.py` — `UnifiedPerson.dedup_addr_key: str | None` (nullable VARCHAR).
- `app/core/builders.py` — `build_person`: build address BEFORE find-or-create;
  compute address_key; thread into `_find_person_by_name_state` + cache key; set
  `dedup_addr_key` on new person. `_find_person_by_name_state(..., address_key=None)`
  matches `dedup_addr_key` (NULL-aware) for individuals.
- `app/core/unified_database.py` — add `dedup_addr_key` to
  `_UNIFIED_ADDITIVE_COLUMNS`; change `uix_persons_name_state` to include
  `dedup_addr_key`.
- `app/resolve/run.py` — mirror the additive column if it lists persons columns.
- `app/core/ingest_vectorized/common.py` — `person_addr_key_expr(...)` denormalizer
  expr; extend `collapse_org_person_key` to also null `_pk_addr` for org-persons.
- `app/core/ingest_vectorized/families/{flat_txns_dims,detail_children,cand,flat_txns_detail}.py`
  — person dedup key gains `_pk_addr`; person write-frames set `dedup_addr_key`;
  `_person_id_map` read-backs key on the address-inclusive key.

## Behavior to preserve
- Org-person dedup unchanged (lower(org), state; NULL dedup_addr_key).
- `diff_snapshots(ORM, vec)` for dim tables stays `[]` — both engines change identically.
- Addressless persons (address_key None) stay name-only (no fragmentation).

## HARD RULES
- Pure Polars (no map_elements/.apply). Parameterized SQLAlchemy core only (no
  f-string SQL). Reuse common.py. Plain git. No rm -rf.

## Checks (evidence required before done)
1. `uv run pytest tests/ingest_equivalence -q` -> green; vec == ORM holds.
2. `grep -rnE "map_elements|\.apply\(" app/core/ingest_vectorized/` -> none in new code.
3. `uv run ruff check app/core tests/ingest_equivalence` -> clean.
4. code-review skill run; findings fixed.

## Cannot run here (no tmp/texas data in this worktree) — report, do not fake
- Real-Texas person-count ~1.9x increase.
- PG load with dedup indexes enforced completing without uix_persons_* violation.
