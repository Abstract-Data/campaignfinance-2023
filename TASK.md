# TASK — Blocker #2: one-to-one entity representative assignment (Postgres)

Plan: docs/design/vectorized-ingest-plan.md · 2 of 3 PG default-flip blockers.

## Problem
`unified_entities.person_id` UNIQUE is violated on real Postgres loads because one
person (suffix-EXCLUDED dedup key `(lower(first), lower(last))`) is assigned as
representative of MORE THAN ONE entity (entity `normalized_name` INCLUDES the suffix,
so "John Anderson" and "John Anderson JR" -> one person id, two entities). Three
families (`flat_txns_detail`, `detail_children`, `cand`) independently assign each
entity's representative, so the single shared person lands on both entities ->
`unified_entities_person_id_key` UniqueViolation.

## Files in scope
- `app/core/ingest_vectorized/finalize.py` (NEW) — `finalize_entity_representatives(session, state_id)`
- `app/core/ingest_vectorized/dispatcher.py` — call finalize after the family loop, before close
- `app/core/ingest_vectorized/families/flat_txns_detail.py` — remove `_apply_entity_links`
  call + `_entity_representatives`/`ent_updates` computation; KEEP `_apply_person_address`
- `app/core/ingest_vectorized/families/detail_children.py` — entity INSERT sets person_id/address_id NULL
- `app/core/ingest_vectorized/families/cand.py` — entity INSERT sets person_id NULL
- `tests/ingest_equivalence/test_entity_one_to_one.py` (NEW) — PG-gated regression

## Approach (the side-preference / determinism rule)
A SINGLE deterministic post-load step computes each person's entity key
(entity_type, normalized_name) EXACTLY as entity creation does — REUSE
`common.normalize_entity_name(common.full_name_expr(...))` (org path:
`normalize_entity_name(organization)`); type = ORGANIZATION when org present else
PERSON. Join persons -> PERSON/ORGANIZATION entities on (entity_type, normalized_name);
pick ONE rep per entity = MIN(person id) -> one-to-one. Entities with no matching
person (suffix-variant orphans) keep person_id NULL. COMMITTEE entities untouched
(keep committee_id). UPDATE via parameterized SQLAlchemy core `update` + `bindparam`
(NO f-string SQL).

## Behavior to preserve
- Golden fixture (no suffix-variant case) results unchanged; entity tests stay green.
- COMMITTEE-entity committee_id assignment unchanged in all three families.
- PERSON.address_id assignment (flat_txns_detail `_apply_person_address`) kept.
- Harness `_PRESENCE_ONLY_FKS` keeps entity person_id/address_id presence-only (min-id
  pick is one valid representative, compatible with presence-only comparison).

## HARD RULES
- Pure Polars expressions (no map_elements/.apply); parameterized SQLAlchemy core only
  (no f-string SQL). Reuse common.py helpers. Plain git (isolated worktree). No rm -rf.

## Checks (evidence required before "done")
1. `uv run pytest tests/ingest_equivalence -q` -> all green (currently 42).
2. PG gate harness (FK dropped, uniques + dedup indexes KEPT) at rows 4000 AND 8000:
   load COMPLETES (no `unified_entities_person_id_key` violation) AND
   "persons on >1 entity" == 0 AND "committee_id on >1 entity" == 0.
   (NOTE: requires real `tmp/texas` data; if absent, the golden-fixture-backed
   regression test is the substitute gate and the PG slice gate is deferred.)
3. New `tests/ingest_equivalence/test_entity_one_to_one.py` passes (skipif no local PG).
4. `uv run ruff check app/core/ingest_vectorized tests/ingest_equivalence` -> clean.
5. code-review skill run; findings fixed.

## Next
Blocker #3: vectorized campaign-build family. After #2, a full constraints-enforced
PG load + `diff_snapshots(ORM, vec) == []` becomes the combined gate.
