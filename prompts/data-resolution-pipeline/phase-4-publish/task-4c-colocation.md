# Task 4c — `co_located_with` association support

> **Phase 4, round 1. Parallel-safe with 4a, 4b, 4d.** Blocks `task-4z`.
> Read the pack README, the Phase 4 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Entities that
share an address are *not* the same entity — but sometimes you want to record
that they are related (a household, a shared office). The design says that is a
**relationship edge, never a merge**. This task lets such links be asserted via
the existing `UnifiedEntityAssociation` pattern.

Reference: the spec's "Address-as-shared-hub model" (`co_located_with`
association — entities stay linked but distinct, never merged).

## Dependencies

- **Depends on:** Phase 3 merged.
- **Blocks:** `task-4z`.
- **Parallel-safe with:** 4a, 4b, 4d.

## Files

- **Create:** `app/resolve/publish/colocation.py` — the co-location linker.
- **Create:** `tests/resolve/test_colocation.py`.

New files only. Do **not** create `app/resolve/publish/__init__.py` — `task-4z`
owns it.

## Interface contract

`colocation.py` exports:

- `find_colocated(session, canonical_address_id) -> list[CanonicalEntity]` — all
  canonical entities at one address.
- `assert_colocation(session, entity_id_a, entity_id_b, *, reason, asserted_by) ->
  association` — records a `co_located_with` link between two **distinct**
  canonical entities. It uses the existing `UnifiedEntityAssociation` pattern
  (add a `CO_LOCATED_WITH` value to the `AssociationType` enum if absent — note
  this small shared-file touch in your commit). It must **refuse** to link an
  entity to itself and must **never** merge — it only creates an association row.
- `suggest_colocations(session, canonical_address_id, *, max_address_frequency) ->
  list[tuple]` — for a *low-frequency* address (small `frequency`, i.e. not a
  registered-agent / large building), suggest entity pairs a human might want to
  link. Addresses above `max_address_frequency` yield no suggestions — a crowded
  address is not evidence of a household. Suggestions are **advisory**: this
  task never auto-asserts.

The boundary is strict: co-location produces *associations*, not merges. Nothing
here writes to the crosswalk or changes a `canonical_entity`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_colocation.py`: failing tests that
  `find_colocated` returns all entities at an address; `assert_colocation`
  creates a `co_located_with` association and refuses a self-link;
  `suggest_colocations` returns pairs for a low-frequency address and nothing
  for one above `max_address_frequency`.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Add `CO_LOCATED_WITH` to `AssociationType` if needed;
  implement `colocation.py`. Run; pass. Commit.
- [ ] **Step 4** — Add a test asserting that nothing in this module writes to
  `entity_crosswalk` or modifies a `canonical_entity` (associations only). Run;
  pass; commit.

## Acceptance criteria

- [ ] `find_colocated` / `assert_colocation` / `suggest_colocations` behave per
  contract.
- [ ] `assert_colocation` creates an association, never a merge; refuses
  self-links.
- [ ] High-frequency addresses produce no co-location suggestions.
- [ ] No crosswalk or canonical-entity row is written by this module.
- [ ] No file outside the Files list (and the one noted enum addition) is
  touched.

## Collision protocol

Branch `resolve/phase-4/task-4c-colocation`. New files only, plus — if needed —
a single value added to the `AssociationType` enum (note it in the commit
message). No other Phase 4 task touches that enum. Do not create
`publish/__init__.py`.
