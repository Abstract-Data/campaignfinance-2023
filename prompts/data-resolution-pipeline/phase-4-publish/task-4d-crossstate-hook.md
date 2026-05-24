# Task 4d — Cross-state hook (verify + document, do not build)

> **Phase 4, round 1. Parallel-safe with 4a, 4b, 4c.** Blocks `task-4z`.
> Read the pack README, the Phase 4 README, and the spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). The design
deliberately scopes cross-state entity linking as **designed for, not built**.
`canonical_entity` already carries a `master_entity_id` self-reference reserved
for it. This task verifies that hook is sound and documents the extension point
— **without** implementing any cross-state resolution.

Reference: the spec's "Non-goals" (cross-state designed-but-not-built) and
"Schema design → Canonical layer" (`master_entity_id`).

## Dependencies

- **Depends on:** Phase 3 merged.
- **Blocks:** `task-4z`.
- **Parallel-safe with:** 4a, 4b, 4c.

## Scope discipline

This is intentionally a **small** task. Do not implement cross-state matching,
blocking, or scoring. Do not run resolution across states. The deliverable is a
verified, documented extension point — nothing more.

## Files

- **Create:** `app/resolve/publish/crossstate.py` — a thin, documented stub.
- **Create:** `docs/superpowers/specs/cross-state-extension.md` — the extension
  note.
- **Create:** `tests/resolve/test_crossstate_hook.py`.

New files only. Do **not** create `app/resolve/publish/__init__.py` — `task-4z`
owns it.

## Interface contract

`crossstate.py` exports:

- `get_master_entity(session, canonical_entity_id) -> CanonicalEntity | None` —
  follows `master_entity_id` to the master record, or `None` if unset.
- `entities_for_master(session, master_entity_id) -> list[CanonicalEntity]` —
  all per-state canonical entities grouped under one master.
- `link_to_master(session, canonical_entity_id, master_entity_id)` — sets the
  self-reference. This is a **manual** linking primitive only — it does no
  matching. Guard against cycles and self-links.

These functions make the existing `master_entity_id` column usable; they are the
seam a future cross-state phase would build on.

`docs/superpowers/specs/cross-state-extension.md` documents: how `master_entity_id`
works, what a future cross-state phase would add (a cross-state matching pass
over per-state canonical entities), and why it is out of scope now.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_crossstate_hook.py`: failing tests
  that `link_to_master` sets the reference; `get_master_entity` follows it;
  `entities_for_master` groups per-state entities; self-links and cycles are
  refused.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `crossstate.py`. Run; pass. Commit.
- [ ] **Step 4** — Write `docs/superpowers/specs/cross-state-extension.md`.
  Commit.

## Acceptance criteria

- [ ] `master_entity_id` is usable via the three primitives; cycles/self-links
  are refused.
- [ ] No cross-state matching/scoring logic exists (scope held).
- [ ] The extension note documents the seam and why it is deferred.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-4/task-4d-crossstate-hook`. New files only. Do not create
`publish/__init__.py`. Keep strictly to the verify-and-document scope.
