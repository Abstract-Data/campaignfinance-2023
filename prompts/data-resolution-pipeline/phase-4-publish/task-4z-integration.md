# Task 4z — Phase 4 integration: publish views, update the ERD

> **Phase 4, serial — runs LAST.** Depends on 4a, 4b, 4c, 4d all merged.
> Read the pack README, the Phase 4 README, and the spec.

## Context

Round-1 tasks delivered the resolved views, the occupancy view, the co-location
linker, and the cross-state hook in isolation. This task wires them into the
CLI, publishes the views, and brings the project's relationship documentation up
to date with the canonical layer.

## Dependencies

- **Depends on:** 4a, 4b, 4c, 4d (all merged to the Phase 4 branch).
- **Blocks:** nothing — this is the final phase.

## Files

- **Create:** `app/resolve/publish/__init__.py` — make `publish` a package;
  export the builders.
- **Modify:** `app/resolve/cli.py` — add a `publish` subcommand that builds the
  resolved views and the `address_occupancy` view.
- **Modify:** `docs/DATA_RELATIONSHIPS.md` — add the canonical/resolution layer
  to the ERD and the data-flow diagrams.
- **Create:** `tests/resolve/test_phase4_integration.py`.

## What to build

1. **Package wiring** — `publish/__init__.py` exporting `build_resolved_views`,
   `build_address_occupancy_view`, and the co-location / cross-state helpers.
2. **`publish` subcommand** — `python -m app.resolve publish --state texas`
   builds all the views in one call and prints the view names created.
3. **Documentation** — extend `docs/DATA_RELATIONSHIPS.md`: add the
   `canonical_entity` / `canonical_campaign` / `canonical_address` /
   `canonical_name_history` tables, the crosswalk and audit tables, and the
   resolved views to the Mermaid ERD; add a short data-flow diagram for
   source → resolution → canonical.

## Steps (TDD)

- [ ] **Step 1** — Create `publish/__init__.py`; wire the `publish` subcommand
  into `cli.py`. Confirm `python -m app.resolve publish --state texas` runs.
  Commit.
- [ ] **Step 2** — Write `tests/resolve/test_phase4_integration.py`: a failing
  test that, after a full pipeline run, `publish` creates every view and
  `resolved_contributions` + `address_occupancy` return rows joined to canonical
  entities.
- [ ] **Step 3** — Fix any wiring gaps so the test passes. Commit.
- [ ] **Step 4** — Update `docs/DATA_RELATIONSHIPS.md` with the canonical layer.
  Commit.
- [ ] **Step 5** — Run the full `tests/resolve/` suite; confirm green. Run the
  whole pipeline + `publish` on Texas into a scratch DB and spot-check a
  resolved view and the occupancy view. Commit.

## Acceptance criteria

- [ ] `uv run pytest tests/resolve/` is fully green.
- [ ] `python -m app.resolve publish --state texas` builds the resolved and
  occupancy views.
- [ ] `resolved_contributions` and `address_occupancy` return correct rows.
- [ ] `docs/DATA_RELATIONSHIPS.md` shows the canonical layer.

## Collision protocol

Branch `resolve/phase-4/task-4z-integration`, cut after 4a–4d are merged. This
task is expected to edit shared files (`publish/__init__.py`, `cli.py`,
`docs/DATA_RELATIONSHIPS.md`). If a round-1 interface gap surfaces, fix it here
and note the deviation.
