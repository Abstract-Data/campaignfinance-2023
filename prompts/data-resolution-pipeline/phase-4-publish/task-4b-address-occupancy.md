# Task 4b — `address_occupancy` view

> **Phase 4, round 1. Parallel-safe with 4a, 4c, 4d.** Blocks `task-4z`.
> Read the pack README, the Phase 4 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). An address is
a shared hub — many canonical entities legitimately sit at one canonical
address. This task publishes the `address_occupancy` view so "who is active at
this address" is a single query, without ever implying those entities are the
same.

Reference: the spec's "Address-as-shared-hub model" (the `address_occupancy`
view).

## Dependencies

- **Depends on:** Phase 3 merged.
- **Blocks:** `task-4z`.
- **Parallel-safe with:** 4a, 4c, 4d.

## Files

- **Create:** `app/resolve/publish/occupancy.py` — the view definition + builder.
- **Create:** `tests/resolve/test_occupancy.py`.

New files only. Do **not** create `app/resolve/publish/__init__.py` — `task-4z`
owns it.

## Interface contract

`occupancy.py` exports `build_address_occupancy_view(session) -> str` — creates
(or replaces) the `address_occupancy` PostgreSQL view and returns its name.

Each row of `address_occupancy` is one `(canonical_address, canonical_entity)`
pairing:

- `canonical_address_id`, the standardized address columns
- `canonical_entity_id`, `entity_name`, `entity_type`
- `role` — how the entity relates to the address (e.g. `resident`/`registered`;
  derive from how the entity links to the address)
- `transaction_count` — count of transactions tied to that entity
- `first_seen_date`, `last_seen_date`

The view must make it trivial to answer both "all entities at address X" and
"how many distinct entities share address X" (the latter via a `COUNT` over the
view). It is read-only — purely a join over `canonical_address`,
`canonical_entity`, the crosswalk, and the transaction tables.

A shared address must list each entity as a **distinct row** — the view never
collapses or merges co-located entities.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_occupancy.py`: a failing test that,
  given three distinct canonical entities sharing one `canonical_address`,
  `build_address_occupancy_view()` creates a view returning three rows for that
  address, each with its own `canonical_entity_id` and `transaction_count`.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `build_address_occupancy_view()`. Run; pass. Commit.
- [ ] **Step 4** — Add a test that `COUNT(*) ... WHERE canonical_address_id = X`
  over the view returns the number of distinct entities at that address. Run;
  pass; commit.
- [ ] **Step 5** — Add an idempotency test (build twice, no error). Commit.

## Acceptance criteria

- [ ] `address_occupancy` exists; each row is one entity-at-address pairing.
- [ ] Multiple entities at one address appear as multiple distinct rows.
- [ ] The view answers "who is at address X" and "how many entities share X".
- [ ] View creation is idempotent.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-4/task-4b-address-occupancy`. New files only, under
`app/resolve/publish/`. Do not create `publish/__init__.py`. Read-only view —
alter no table.
