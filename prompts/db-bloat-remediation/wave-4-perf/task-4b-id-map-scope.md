# Model: claude-sonnet-4-6

# Task 4b: State-Scope FK Resolution id-Map Reads

## Phase

Wave 4 — parallel with 4a (disjoint file ownership). Only after wave-3-complete.

## Branch

`db-bloat/wave-4/task-4b-id-map-scope`

## Objective

Per-family id-map reads in `filer.py` and `finalize.py` scan all states.
Add `WHERE state_id = ?` so FK resolution scans only the current state.

## File Ownership

This task owns:
- `app/core/ingest_vectorized/families/filer.py` (~L248 id-map read)
- `app/core/ingest_vectorized/finalize.py` (~L49/113 id-map reads)

Do NOT touch `common.py` (owned by 4a).

## Mandatory Pre-work

Run `gitnexus_impact` on:
- `filer.py` `run()` (or equivalent family `run()`)
- `finalize.py` `run()` and `finalize()`

**STOP and report if HIGH or CRITICAL.**

Use Context7 for current Polars + SQLModel docs.

## Implementation

### filer.py (~L248)

Find the id-map read for committee/filer FK resolution. It should be a query like:

```python
# Before (full-table scan):
committees = session.exec(select(UnifiedCommittee)).all()

# After (state-scoped):
committees = session.exec(
    select(UnifiedCommittee).where(UnifiedCommittee.state_id == state_id)
).all()
```

Add `state_id` parameter to the function if not already present, or use
the `FamilyContext` state_id.

Use parameterized queries only (SQLModel `.where()` — never string SQL).

### finalize.py (~L49/113)

Same pattern for the id-map reads in the finalize family. Add `WHERE state_id = ?`
to each FK-resolution query.

## Output Parity Test

Write or update a test confirming the same FK assignments on a sample state.
Place at `tests/test_wave4_fk_parity.py`.

## Commit

```
perf: state-scope id-map reads in filer + finalize families (4b)
```

## Checklist

- [ ] Wave 3 `db-bloat/wave-3-complete` tag confirmed
- [ ] `gitnexus_impact` run on filer `run()` and finalize `run()`/`finalize()`; no HIGH/CRITICAL
- [ ] Context7 consulted for SQLModel docs
- [ ] `filer.py` id-map read scoped by `state_id`
- [ ] `finalize.py` id-map reads scoped by `state_id`
- [ ] Parameterized queries only
- [ ] FK parity test written
- [ ] `gitnexus_detect_changes()` run before commit
