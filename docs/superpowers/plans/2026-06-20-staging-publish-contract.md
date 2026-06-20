# Resolve Staging/Publish Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Resolve the mismatch between documented atomic staging swap (`app/resolve/staging.py`) and actual survivorship publish (in-place delete + insert on live canonical tables), eliminating dead code and misleading `fail()` cleanup.

**Architecture (recommended: Option 1 — delete unused staging swap):** Production publish path already works via `_clear_live_canonical_snapshot()` in `survivorship.py` (delete live canonical rows, insert fresh golden records, preserve history in `entity_crosswalk` keyed by `run_id`). The `create_run_staging` / `swap_staging_to_live` helpers are tested in isolation but never called by any stage. `ResolutionRun.fail()` calls `drop_run_staging()`, which is a no-op in normal runs because no stage creates `staging_run_*` tables — yet tests imply failed runs clean up staging. **Option 1** deletes `staging.py`, removes the `fail()` hook, replaces Step 5 tests with documentation-aligned survivorship publish tests, and updates resolve docs to describe the actual delete-and-replace contract.

**Why not Option 2 (wire survivorship to staging swap):** Would require rewriting Stage 7 to write all four canonical tables into per-run staging copies, handle FK ordering (`canonical_campaign` → `canonical_entity`), change rollback semantics on partial publish, and re-validate `entity_crosswalk` timing — high blast radius (~750 LOC in survivorship + crosswalk writers) for no current production bug. Thermo review flagged **maintainability over aspirational design**; Option 1 aligns code with tested behavior.

**Tech Stack:** Python 3.12+, SQLModel, SQLAlchemy DDL (drop only in tests being removed), pytest (`tests/resolve/test_run.py`, `tests/resolve/test_survivorship.py`).

---

## Decision Record

| Option | Effort | Risk | Recommendation |
|--------|--------|------|----------------|
| **1: Delete staging swap + update docs/tests** | ~2–4 hours | Low — removes dead code | ✅ **Recommended** |
| **2: Wire survivorship to staging swap** | ~2–3 days | High — publish path rewrite | Defer to ADR if atomic swap becomes a hard requirement |

---

## File Map (Option 1)

| Path | Action | Responsibility |
|------|--------|----------------|
| `app/resolve/staging.py` | **Delete** | Unused atomic swap helpers |
| `app/resolve/run.py` | **Modify** | Remove `drop_run_staging` call from `fail()`; update docstring |
| `tests/resolve/test_run.py` | **Modify** | Remove Step 5 staging helper tests; add `fail()` does-not-require-staging test |
| `tests/resolve/test_survivorship_publish.py` | **Create** | Assert publish clears live canonical before write |
| `docs/ARCHITECTURE.md` or `app/resolve/README.md` | **Modify** | Document actual publish contract |
| `CHANGELOG.md` | **Modify** | Note removal of unused staging swap module |

---

### Task 1: Document current publish behavior with failing characterization test

**Files:**
- Create: `tests/resolve/test_survivorship_publish.py`

- [ ] **Step 1: Write test that live canonical is cleared on publish**

```python
# tests/resolve/test_survivorship_publish.py
from __future__ import annotations

from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.models.canonical import CanonicalEntity
from app.resolve.models.resolution import MatchRun, PassType, RunStatus
from app.resolve.stages.survivorship import run_survivorship_stage
from app.resolve.standardize.staging import ResolutionInput


def _engine():
    from app.resolve.models.canonical import CanonicalCampaign, CanonicalNameHistory
    from app.resolve.models.resolution import EntityCrosswalk
    from app.resolve.stages.cluster import ClusterAssignment
    from app.resolve.stages.fastpath import MergeEdge

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    tables = [
        MatchRun.__table__,
        ResolutionInput.__table__,
        MergeEdge.__table__,
        ClusterAssignment.__table__,
        CanonicalEntity.__table__,
        CanonicalNameHistory.__table__,
        CanonicalCampaign.__table__,
        EntityCrosswalk.__table__,
    ]
    SQLModel.metadata.create_all(engine, tables=tables)
    return engine


def test_survivorship_publish_replaces_live_canonical_snapshot():
    engine = _engine()
    with Session(engine) as session:
        session.add(CanonicalEntity(id=1, state_code="TX", entity_type="person", display_name="Stale"))
        session.add(
            MatchRun(id=1, state_code="TX", pass_type=PassType.entity, status=RunStatus.running)
        )
        session.add(
            ResolutionInput(
                run_id=1,
                source_type="unified_person",
                source_id="p1",
                entity_type="person",
                first_name="Ann",
                last_name="Adams",
                parse_status="parsed",
            )
        )
        session.commit()

        run_survivorship_stage(session, 1, {"state_code": "TX"})

        stale = session.exec(select(CanonicalEntity).where(CanonicalEntity.display_name == "Stale")).first()
        assert stale is None, "prior live canonical row must be deleted before publish"
        live = session.exec(select(CanonicalEntity)).all()
        assert len(live) >= 1
```

- [ ] **Step 2: Run test**

Run: `uv run pytest tests/resolve/test_survivorship_publish.py -v --tb=short`
Expected: **PASS** (documents existing behavior — this test should pass today)

- [ ] **Step 3: Commit**

```bash
git add tests/resolve/test_survivorship_publish.py
git commit -m "test: characterize survivorship live-canonical replace publish"
```

---

### Task 2: Remove unused `staging.py` module

**Files:**
- Delete: `app/resolve/staging.py`
- Modify: `app/resolve/run.py:261-292`

- [ ] **Step 1: Write test that staging module is gone**

```python
# tests/resolve/test_run.py (replace Step 5 class)
def test_staging_swap_module_removed():
    import importlib

    try:
        importlib.import_module("app.resolve.staging")
    except ModuleNotFoundError:
        return
    raise AssertionError("app.resolve.staging should be deleted (Option 1)")
```

Run: `uv run pytest tests/resolve/test_run.py::test_staging_swap_module_removed -v`
Expected: **FAIL** — module still importable

- [ ] **Step 2: Delete `app/resolve/staging.py`**

- [ ] **Step 3: Update `ResolutionRun.fail()` in `run.py`**

Remove block:

```python
        try:
            from app.resolve.staging import drop_run_staging

            drop_run_staging(session, run.id)
        except Exception:
            logger.exception("Failed to drop staging tables for run id=%d", run.id)
```

Replace docstring on `fail()`:

```python
    def fail(self, session: Session, error: str) -> None:
        """Mark the run ``failed`` and set ``finished_at``.

        Canonical publish uses delete-and-replace on the live tables inside
        Stage 7; a failed run before publish completes leaves prior canonical
        data intact (transaction rolled back before survivorship commit).
        """
```

- [ ] **Step 4: Remove Step 5 staging tests from `test_run.py`**

Delete classes/methods:
- `TestStagingHelpers` (lines ~309–420)
- `test_drop_run_staging_*`
- `test_resolution_run_fail_drops_staging` (lines ~448–466)

Keep `test_resolution_run_fail_drops_staging` replacement:

```python
def test_resolution_run_fail_without_staging_tables():
    """fail() must succeed even when no staging_run_* tables exist."""
    engine = _make_engine()
    run = ResolutionRun(state_code="TX", config={})
    with Session(engine) as session:
        run.start(session)
        run.fail(session, "simulated stage error")
    refreshed = Session(engine).get(MatchRun, run.run_id)
    assert refreshed.status == RunStatus.failed
```

- [ ] **Step 5: Run resolve unit tests**

Run: `uv run pytest tests/resolve/test_run.py tests/resolve/test_survivorship_publish.py -v --tb=short`
Expected: **PASS**

- [ ] **Step 6: Commit**

```bash
git rm app/resolve/staging.py
git add app/resolve/run.py tests/resolve/test_run.py
git commit -m "refactor: remove unused resolve canonical staging swap module"
```

---

### Task 3: Update documentation

**Files:**
- Modify: `docs/ARCHITECTURE.md` (resolve section) or create `docs/resolve/PUBLISH.md`
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add publish contract section**

```markdown
## Resolve canonical publish (Stage 7)

Survivorship publishes golden records by **delete-and-replace** on live canonical
tables (`_clear_live_canonical_snapshot` in `app/resolve/stages/survivorship.py`):

1. Delete `canonical_name_history`, `canonical_campaign`, `canonical_entity`.
2. Insert fresh rows for the completed run.
3. Preserve per-run provenance in `entity_crosswalk` keyed by `run_id`.

Failed runs roll back before Stage 7 commits; prior canonical data remains
serving. There is no per-run `staging_run_*` table swap.
```

- [ ] **Step 2: Add CHANGELOG entry**

```markdown
### Removed
- `app/resolve/staging.py` — atomic table swap helpers were tested but never wired
  to Stage 7 publish; survivorship uses delete-and-replace on live canonical tables.
```

- [ ] **Step 3: Grep for stale references**

Run: `rg "staging_run_|swap_staging|create_run_staging|drop_run_staging" app/ docs/ tests/`
Expected: No production references (only CHANGELOG/historical notes).

- [ ] **Step 4: Commit**

```bash
git add docs/ CHANGELOG.md
git commit -m "docs: document resolve survivorship delete-and-replace publish"
```

---

### Task 4: Full resolve regression

- [ ] **Step 1: Run resolve unit tier**

Run: `uv run pytest tests/resolve -m "not integration" -v --tb=short`
Expected: **PASS**

- [ ] **Step 2: Run ruff**

Run: `uv run ruff check app/resolve/run.py tests/resolve/`
Expected: **PASS**

- [ ] **Step 3: GitNexus detect_changes**

Run: `npx gitnexus detect_changes --scope all`
Expected: Blast radius limited to `run.py`, deleted `staging.py`, tests, docs.

---

## Option 2 Outline (if human overrides recommendation)

Only execute if explicitly approved — **do not implement in Option 1 pass:**

1. Stage 7 creates `staging_run_{id}_canonical_*` via `create_run_staging`.
2. Survivorship writes to staging tables instead of live.
3. On success: `swap_staging_to_live` for each table in FK order.
4. On `fail()`: `drop_run_staging` drops partial staging (now meaningful).
5. Rewrite `tests/resolve/test_run.py` Step 5 as integration tests against Stage 7.
6. Estimate: 8–12 tasks, `tests/resolve/test_survivorship.py` + phase2 integration full re-run.

---

## Self-Review

| Check | Status |
|-------|--------|
| Option chosen with rationale | ✅ Option 1 recommended |
| fail() staging cleanup addressed | ✅ Task 2 |
| Unused swap code removed | ✅ Task 2 |
| Publish contract documented | ✅ Task 3 |
| Survivorship behavior tested | ✅ Task 1 |
| Placeholder scan | ✅ None |
| Option 2 deferred with outline | ✅ Bottom section |

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-20-staging-publish-contract.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — Task 1 (characterization test) → Task 2 (delete module) → Task 3 (docs) → Task 4 (regression).

**2. Inline Execution** — @superpowers:executing-plans; human must confirm Option 1 before Task 2 (irreversible delete).

**Which approach?**
