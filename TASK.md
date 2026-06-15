# TASK — Blocker #1: org-person dedup parity (Postgres partial-unique indexes)

Plan: docs/design/vectorized-ingest-plan.md · 1 of 3 PG default-flip blockers.

## Goal
Make the vectorized engine's person dedup match the ORM `BuilderCache.person_key` / the
`uix_persons_org_state` partial index, so it no longer inserts org-person duplicates that
collide on Postgres (the #1 blocker surfaced by the throughput benchmark, PR #44).

## Root cause
Families keyed persons on `(lower(org), lower(first), lower(last))`. But an org-person is
unique on `lower(org)` ALONE (`uix_persons_org_state … WHERE organization IS NOT NULL`; ORM
`person_key` → `("org", lower(org), state)`, ignoring first/last). Two org rows with the same
org but different incidental contact names survived the 3-key dedup yet collided on the
org-only index → UniqueViolation on a real PG load.

## Fix (files in scope)
- `app/core/ingest_vectorized/common.py`: `collapse_org_person_key(frame)` — nulls
  `_pk_fn`/`_pk_ln` where `_pk_org` is set (stored first/last untouched; only the dedup KEY).
- Applied after every person-key build / before every group_by/unique/id-map join:
  `flat_txns_dims` (RCPT, EXPN, RCPT-addr, EXPN-addr builders), `detail_children`
  (2 party builders + `_person_id_map`), `flat_txns_detail` (`_person_id_map`). `cand`
  already keyed org-persons on org alone (no change).

## Checks / evidence
- `tests/ingest_equivalence/test_org_person_dedup.py`: helper nulls fn/ln for org rows only;
  org case-variants with differing incidental names dedup to ONE; individuals unaffected.
- sqlite equivalence suite: 39 passed (no regression). ruff clean.
- PG (unique indexes ENFORCED, FK off): the `uix_persons_org_state`/`uix_persons_name_state`
  violation is ELIMINATED — the load now progresses past person dedup to the entity
  one-to-one constraint (blocker #2).

## Next
Blocker #2: `unified_entities.person_id`/`committee_id` one-to-one — three families
independently assign an entity's representative person; a shared person gets assigned to
>1 entity. Then blocker #3: vectorized campaign-build family. After #2, a full
constraints-enforced PG load + `diff_snapshots(ORM, vec) == []` becomes the combined gate.
