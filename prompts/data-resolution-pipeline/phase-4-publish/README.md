# Phase 4 — Publish + cross-state hook

**Goal:** Make the resolved graph analysis-ready — resolved views so consumers
query canonical entities directly — and leave a clean, unbuilt hook for
cross-state linking.

**Outcome when done:** Analysts can query contributions and expenditures by
canonical entity (not raw source rows); an `address_occupancy` view answers "who
is active at this address"; household/shared-office links can be asserted
without merging; and the cross-state extension point is documented and verified.

**Prerequisite:** Phase 3 merged (review queue and audit complete).

Reference: the spec's "Rollout phases → Phase 4", the `address_occupancy` view
and `co_located_with` association in the address-as-shared-hub section, and the
"Non-goals" note that cross-state linking is designed-for, not built.

## Rounds

**Round 1 — four disjoint publish concerns (run concurrently):**

| Task | Delivers | Owns |
|------|----------|------|
| `task-4a` | Resolved views / fact tables — transactions, contributions, expenditures joined through the crosswalk to canonical entities | `app/resolve/publish/views.py` |
| `task-4b` | `address_occupancy` view | `app/resolve/publish/occupancy.py` |
| `task-4c` | `co_located_with` association support (link, never merge) | `app/resolve/publish/colocation.py` |
| `task-4d` | Cross-state hook — verify `master_entity_id`, document the extension point (no resolution logic) | `app/resolve/publish/crossstate.py` |

**Then — `task-4z` integration:** publishes the views via the CLI and updates
the `docs/DATA_RELATIONSHIPS.md` ERD to show the canonical layer.

## Collision-freedom

- Round 1: four disjoint files under the new `app/resolve/publish/` package.
  `publish/__init__.py` is created by `task-4z`.
- No round-1 task edits a pipeline stage — publishing is purely additive,
  reading the canonical + crosswalk tables.
- `task-4z` is the only task that edits `docs/DATA_RELATIONSHIPS.md`.

## Verifying the phase

`task-4z` is done when `uv run pytest tests/resolve/` is green, the resolved
views return rows joined to canonical entities, the `address_occupancy` view
lists multiple entities at a shared address, and `docs/DATA_RELATIONSHIPS.md`
reflects the canonical layer.

## Note on `task-4d`

`task-4d` is deliberately small. The spec's non-goal is explicit: cross-state
linking is *designed for, not built*. The task verifies that the
`canonical_entity.master_entity_id` self-reference is sound and documents the
extension point — it must **not** implement cross-state resolution logic.
