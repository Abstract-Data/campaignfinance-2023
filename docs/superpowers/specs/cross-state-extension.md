# Cross-state entity linking — extension point

**Status:** Designed, not built (Phase 4, task 4d)  
**Spec:** [2026-05-23-data-resolution-pipeline-design.md](./2026-05-23-data-resolution-pipeline-design.md)

## What exists today

`canonical_entity` includes a nullable self-reference:

- `master_entity_id` → `canonical_entity.id`

Per-state resolution still produces one canonical row per state-specific entity cluster.
Cross-state identity is **not** inferred automatically.

`app/resolve/publish/crossstate.py` exposes manual primitives only:

| Function | Purpose |
|----------|---------|
| `get_master_entity` | Walk `master_entity_id` to the root master (cycle-safe) |
| `entities_for_master` | List the master and all entities linked under it |
| `link_to_master` | Set `master_entity_id` with self-link and cycle guards |

These functions do **not** match, score, block, or merge entities across states.

## What a future cross-state phase would add

A later pass would run **after** per-state canonicalization:

1. Candidate generation across states (name, address, employer, committee IDs where available).
2. Scoring and blocking (reuse resolution-stage patterns where appropriate).
3. Human review for ambiguous pairs.
4. `link_to_master` (or bulk equivalent) to attach per-state canonical rows to a shared master.

That phase would write `master_entity_id` based on evidence, not operator-only asserts.

## Why it is out of scope now

The pipeline design explicitly lists cross-state linking as a **non-goal** for the initial
resolution build:

- State filings use incompatible identifiers and disclosure rules.
- Premature cross-state merges would collapse distinct legal entities.
- Per-state canonical quality and publish views (Phase 4a–4c) must stabilize first.

Phase 4d only **verifies** the `master_entity_id` column and documents this seam so a
future phase can plug in without schema churn.
