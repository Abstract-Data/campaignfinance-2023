# Task 4d — Value objects for name / address / officer data clumps

> **Wave 4, parallel. Branch `remediation/wave-4/task-4d-value-objects`.**
> Requires Wave 3 merged. Read the pack README and the Refactoring Report
> (**RF-SMELL-004**).

## Context

The same field groups recur as ad-hoc dicts and loose primitives across the
builders: first/last/middle/suffix/org name parts; street_1/2/city/state/zip
address parts; and the `name`/`role`/`committee_id` officer tuple. This is a
data-clump / primitive-obsession smell.

## Files

- **Create:** `app/core/value_objects.py`
- **Create:** `tests/test_value_objects.py`

New file only — no collision with any wave-4 peer.

## What to implement (RF-SMELL-004)

Define lightweight, frozen value objects (Pydantic models or
`@dataclass(frozen=True)`):

- `PersonName` — `first`, `middle`, `last`, `suffix`, `organization`; a
  `full_name` property; whitespace/case normalization built in.
- `AddressParts` — `street_1`, `street_2`, `city`, `state`, `zip_code`; a
  `normalized()` method consistent with the Wave-1 model normalization.
- `Officer` — `name: PersonName`, `role`, `committee_id`.

These are the types `builders.py` (task 4c) and the processor will adopt to
replace the ad-hoc `*_data` dicts. Keep them dependency-light and pure (no DB,
no I/O) so they are trivially testable and reusable by the planned
entity-resolution pipeline.

## Steps

- [ ] **1** — `tests/test_value_objects.py`: failing tests for construction,
  `PersonName.full_name`, `AddressParts.normalized()`, frozen-ness (mutation
  raises), and normalization (e.g. `AddressParts(state=" tx ").normalized()`).
- [ ] **2** — Run; expect fail. **3** — Implement `value_objects.py`. **4** —
  Run; pass. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] `PersonName`, `AddressParts`, `Officer` exist in
  `app/core/value_objects.py`, frozen, pure, normalized, fully tested.
- [ ] No DB or I/O dependency in the module.

## Collision protocol

New files only. Task 4c consumes these types in `builders.py` — keep the class
names and constructor signatures stable so 4c can build against this contract.
