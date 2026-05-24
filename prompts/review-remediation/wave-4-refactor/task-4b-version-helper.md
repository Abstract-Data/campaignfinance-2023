# Task 4b — Version-snapshot helper + analysis-loop N+1 fixes

> **Wave 4, parallel. Branch `remediation/wave-4/task-4b-version-helper`.**
> Requires Wave 3 merged. Read the pack README and the Refactoring Report
> (**RF-DRY-001**, **RF-CPLX-003** — the `unified_database.py` part).

## Context

`unified_database.py` repeats a ~25-line version-snapshot block across five
`update_*` methods (~85% similar). The duplication also hides a bug:
`update_person`/`update_committee`/`update_address` use a bare
`json.dumps({k: getattr(...)})` that raises `TypeError` on any `date`/`Decimal`
field, while `update_transaction`/`update_committee_person` handle those types.
Separately, `get_committee_officer_activities` (`:947-972`) and
`get_person_committee_financial_summary` (`:997-1054`) nest 4 levels deep with
per-iteration DB queries (N+1).

## Files

- **Modify:** `app/core/unified_database.py`
- **Create:** `tests/test_versioning.py`

## What to implement

- **RF-DRY-001** — Extract a generic `_record_version(session, entity,
  version_model, fk_field, ...)` helper and a `_to_json_safe(entity)` serializer
  that correctly handles `date`/`datetime`/`Decimal`. Rewrite the five
  `update_*` methods (`update_transaction`, `update_person`, `update_committee`,
  `update_address`, `update_committee_person`) as thin wrappers over it. This
  removes ~100 duplicated lines **and fixes the `json.dumps` bug uniformly**.
  Use a registry mapping entity type → version model.
- **RF-CPLX-003 (part)** — In `get_committee_officer_activities` and
  `get_person_committee_financial_summary`, apply guard clauses / early
  `continue` to flatten the 4-level nesting, and replace the per-iteration
  queries with a single `IN`-clause query or a join.
- Narrow any bare `except`; replace `ic()` with `Logger` in code you touch.

## Steps

- [ ] **1** — `tests/test_versioning.py`: failing tests that updating an entity
  with a `date` field **and** a `Decimal` field records a version without
  raising (this currently fails for person/committee/address), and that the
  version snapshot round-trips.
- [ ] **2** — Run; expect fail (TypeError on date/Decimal).
- [ ] **3** — Implement `_record_version` + `_to_json_safe`; rewrite the five
  `update_*`; flatten the two analysis loops. **4** — Run; pass. `ruff check
  --fix`. Commit.

## Acceptance criteria

- [ ] One `_record_version` helper + `_to_json_safe` serializer; the five
  `update_*` are thin wrappers.
- [ ] Versioning an entity with `date`/`Decimal` fields no longer raises.
- [ ] The two analysis methods are ≤ 3 nesting levels with no per-iteration query.

## Collision protocol

You own `app/core/unified_database.py` for Wave 4. Tasks 4a/4c/4e own other
files — no overlap.
