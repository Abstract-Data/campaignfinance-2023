# score.py Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the 899-line `app/resolve/stages/score.py` monolith into focused modules (ScoredPair model, Splink harness, bulk Postgres COPY) with a thin `run_score_stage` orchestrator (~150 LOC) and **zero behavior change**.

**Architecture:** Extract by natural seams already present in the file: (1) `ScoredPair` SQLModel + column constants → `scored_pair.py`; (2) Postgres bulk load + index/unlogged helpers → `score_bulk.py`; (3) Splink/DuckDB streaming, pair iteration, explanation JSON → `score_splink.py`; (4) `score.py` retains only `run_score_stage`, `_score_unconfigured_type`, and re-exports for backward-compatible imports. All existing callers (`from app.resolve.stages.score import ScoredPair, run_score_stage`) keep working via re-exports in `score.py`.

**Tech Stack:** Python 3.12+, Splink, DuckDB, pandas (Splink interop), SQLModel, psycopg2 COPY, pytest (`tests/resolve/test_score.py`).

---

## File Map (create/modify before tasks)

| Path | Action | Responsibility | Approx LOC |
|------|--------|----------------|------------|
| `app/resolve/stages/scored_pair.py` | **Create** | `ScoredPair` model, `_SCORED_COLS`, `_SCORED_PAIR_BATCH_SIZE` | ~40 |
| `app/resolve/stages/score_bulk.py` | **Create** | `_COPY_SCORED_SQL`, `_copy_scored_postgres`, `_bulk_insert_scored`, index drop/create, `_ensure_scored_unlogged` | ~90 |
| `app/resolve/stages/score_splink.py` | **Create** | Splink training/scoring, DuckDB streaming, `_score_entity_type_streaming`, helper fns lines 104–730 + 733–932 of current file | ~650 |
| `app/resolve/stages/score.py` | **Modify** | Thin orchestrator: `run_score_stage`, `_score_unconfigured_type`; re-export public symbols | ~150 |
| `tests/resolve/test_score_imports.py` | **Create** | Import stability + module size guards |
| `tests/resolve/test_score.py` | **Verify unchanged** | Full Stage 4 acceptance suite |

**Seams in current `score.py` (line anchors):**

- Model: 78–97, constants 47–59
- Bulk: 315–389
- Splink/streaming: 104–314, 392–730, 733–954
- Orchestrator: 933–954, 961–1021

---

### Task 1: Extract `ScoredPair` model module

**Files:**
- Create: `app/resolve/stages/scored_pair.py`
- Create: `tests/resolve/test_score_imports.py`
- Modify: `app/resolve/stages/score.py`

- [ ] **Step 1: Write failing import test**

```python
# tests/resolve/test_score_imports.py
def test_scored_pair_importable_from_legacy_path():
    from app.resolve.stages.score import ScoredPair, run_score_stage

    assert ScoredPair.__tablename__ == "scored_pairs"
    assert callable(run_score_stage)


def test_scored_pair_importable_from_dedicated_module():
    from app.resolve.stages.scored_pair import ScoredPair

    assert ScoredPair.__tablename__ == "scored_pairs"
```

- [ ] **Step 2: Run test — expect FAIL on second assertion path**

Run: `uv run pytest tests/resolve/test_score_imports.py -v --tb=short`
Expected: **FAIL** with `ModuleNotFoundError: app.resolve.stages.scored_pair`

- [ ] **Step 3: Create `scored_pair.py`**

Move `ScoredPair` class (lines 78–97) and `_SCORED_COLS`, `_SCORED_PAIR_BATCH_SIZE` constants.

```python
# app/resolve/stages/scored_pair.py
from __future__ import annotations

from sqlalchemy import Column, Float, String, Text
from sqlmodel import Field, SQLModel

from app.resolve.models.resolution import SOURCE_ID_MAX_LENGTH

_SCORED_PAIR_BATCH_SIZE = 50_000
_SCORED_COLS = (
    "run_id",
    "source_a_type",
    "source_a_id",
    "source_b_type",
    "source_b_id",
    "entity_type",
    "score",
    "explanation_json",
)


class ScoredPair(SQLModel, table=True):
    __tablename__ = "scored_pairs"
    id: int | None = Field(default=None, primary_key=True)
    run_id: int = Field(index=True)
    source_a_type: str = Field(sa_column=Column(String(64), nullable=False))
    source_a_id: str = Field(sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False))
    source_b_type: str = Field(sa_column=Column(String(64), nullable=False))
    source_b_id: str = Field(sa_column=Column(String(SOURCE_ID_MAX_LENGTH), nullable=False))
    entity_type: str = Field(sa_column=Column(String(64), nullable=False, index=True))
    score: float = Field(sa_column=Column(Float, nullable=False))
    explanation_json: str = Field(sa_column=Column(Text, nullable=False))
```

- [ ] **Step 4: Re-export from `score.py`**

```python
from app.resolve.stages.scored_pair import ScoredPair, _SCORED_COLS, _SCORED_PAIR_BATCH_SIZE

__all__ = ["ScoredPair", "run_score_stage"]
```

Remove inline `ScoredPair` class from `score.py`.

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/resolve/test_score_imports.py tests/resolve/test_score.py -v --tb=short -x`
Expected: **PASS**

- [ ] **Step 6: Commit**

```bash
git add app/resolve/stages/scored_pair.py app/resolve/stages/score.py tests/resolve/test_score_imports.py
git commit -m "refactor: extract ScoredPair model from score stage"
```

---

### Task 2: Extract bulk Postgres COPY module

**Files:**
- Create: `app/resolve/stages/score_bulk.py`
- Modify: `app/resolve/stages/score.py`

- [ ] **Step 1: Write failing test for bulk module import**

```python
# tests/resolve/test_score_imports.py (append)
def test_score_bulk_exports_insert_helper():
    from app.resolve.stages.score_bulk import bulk_insert_scored

    assert callable(bulk_insert_scored)
```

Run: `uv run pytest tests/resolve/test_score_imports.py::test_score_bulk_exports_insert_helper -v`
Expected: **FAIL** — module missing

- [ ] **Step 2: Move bulk functions to `score_bulk.py`**

Move lines 309–389 (`_COPY_SCORED_SQL` through `_ensure_scored_unlogged`). Rename public API:

```python
# app/resolve/stages/score_bulk.py
def bulk_insert_scored(session: Session, rows: list[dict[str, Any]]) -> None:
    ...
```

Keep `_copy_scored_postgres` private. Import `ScoredPair`, `_SCORED_COLS` from `scored_pair`.

- [ ] **Step 3: Update `score.py` and `score_splink.py` callers**

Replace `_bulk_insert_scored(session, rows)` with `from app.resolve.stages.score_bulk import bulk_insert_scored` (still in monolithic file for now; splink extraction in Task 3).

- [ ] **Step 4: Run score tests**

Run: `uv run pytest tests/resolve/test_score.py -v --tb=short`
Expected: **PASS**

- [ ] **Step 5: Commit**

```bash
git add app/resolve/stages/score_bulk.py app/resolve/stages/score.py tests/resolve/test_score_imports.py
git commit -m "refactor: extract score bulk COPY helpers"
```

---

### Task 3: Extract Splink harness to `score_splink.py`

**Files:**
- Create: `app/resolve/stages/score_splink.py`
- Modify: `app/resolve/stages/score.py`

- [ ] **Step 1: Write LOC guard (fails until extraction complete)**

```python
# tests/resolve/test_score_imports.py (append)
from pathlib import Path

def test_score_orchestrator_under_200_loc():
    loc = len(Path("app/resolve/stages/score.py").read_text().splitlines())
    assert loc <= 200, f"score.py still {loc} lines; target ~150 orchestrator"
```

Run: `uv run pytest tests/resolve/test_score_imports.py::test_score_orchestrator_under_200_loc -v`
Expected: **FAIL** — `score.py` still ~899 lines

- [ ] **Step 2: Move Splink helpers to `score_splink.py`**

Move functions:
- `_build_uid`, `_row_to_dict`, `_load_entity_config`, `_linker_settings_obj`, `_extract_comp_meta`, `_build_explanation`
- `_train_and_score_pair`, `_train_linker`, `_scored_row`
- `_duckdb_tmp_root`, `_load_type_uids`, `_iter_type_pairs`, `_predict_exact_pairs`
- `_comp_meta_rows`, `_write_scored_via_pg`, `_score_entity_type_streaming`
- All module-level SQL string constants (`_PRED_NARROW_*`, `_PG_*`, `_CHUNK_CSV_PATH`, etc.)

Export public function:

```python
def score_entity_type_streaming(
    session: Session,
    run_id: int,
    entity_type: str,
    config: types.ModuleType,
    seed: int,
) -> int:
    """Public wrapper around former _score_entity_type_streaming."""
    ...
```

Import `bulk_insert_scored` from `score_bulk`; import `ScoredPair` only if needed for type hints.

- [ ] **Step 3: Slim `score.py` to orchestrator**

Final `score.py` structure:

```python
"""Stage 4: probabilistic record-linkage scoring with Splink."""

from __future__ import annotations

from sqlmodel import Session, delete, select

from app.resolve.standardize.staging import ResolutionInput
from app.resolve.stages.score_bulk import (
    bulk_insert_scored,
    create_scored_indexes,
    drop_scored_indexes,
    ensure_scored_unlogged,
)
from app.resolve.stages.score_splink import (
    load_entity_config,
    score_entity_type_streaming,
)
from app.resolve.stages.scored_pair import ScoredPair, _SCORED_PAIR_BATCH_SIZE

def _score_unconfigured_type(session, run_id, entity_type) -> int:
    ...  # move unchanged from lines 933-954

def run_score_stage(session: Session, run_id: int, config: dict) -> dict:
    ...  # lines 961-1021, calling extracted helpers
```

Re-export: `ScoredPair`, `run_score_stage`.

- [ ] **Step 4: Run full resolve unit tier**

Run: `uv run pytest tests/resolve/test_score.py tests/resolve/test_score_imports.py -v --tb=short`
Expected: **PASS** (including LOC guard)

- [ ] **Step 5: Run broader resolve tests (non-integration)**

Run: `uv run pytest tests/resolve -m "not integration" -v --tb=short -x`
Expected: **PASS**

- [ ] **Step 6: Run ruff**

Run: `uv run ruff check app/resolve/stages/score*.py app/resolve/stages/scored_pair.py && uv run ruff format --check app/resolve/stages/`
Expected: **PASS**

- [ ] **Step 7: Commit**

```bash
git add app/resolve/stages/score.py app/resolve/stages/score_splink.py tests/resolve/test_score_imports.py
git commit -m "refactor: split Splink harness from score stage orchestrator"
```

---

### Task 4: Update stage registry imports (if any break)

**Files:**
- Modify: `app/resolve/stages/__init__.py` (verify)
- Modify: `app/resolve/run.py` `ensure_resolution_schema` imports (verify `ScoredPair` table registration)

- [ ] **Step 1: Grep for direct imports**

Run: `rg "from app.resolve.stages.score import" app/ tests/`
Expected: All paths still resolve via re-exports.

- [ ] **Step 2: Verify schema registration**

Run: `uv run pytest tests/resolve/test_run.py::test_ensure_resolution_schema_creates_scored_pairs_and_clusters -v`
Expected: **PASS**

- [ ] **Step 3: Commit (if any import fixes needed)**

```bash
git commit -m "chore: align resolve stage imports after score split"
```

---

## Self-Review

| Check | Status |
|-------|--------|
| ScoredPair model extracted | ✅ Task 1 |
| Bulk COPY extracted | ✅ Task 2 |
| Splink harness extracted | ✅ Task 3 |
| Orchestrator ~150 LOC | ✅ LOC guard in Task 3 |
| Backward-compatible imports | ✅ Re-exports in score.py |
| Behavior unchanged | ✅ tests/resolve/test_score.py full suite |
| Placeholder scan | ✅ None |

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-20-score-decomposition.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — one subagent per task; run `tests/resolve/test_score.py` after every task; full `tests/resolve -m "not integration"` after Task 3.

**2. Inline Execution** — @superpowers:executing-plans; checkpoint after Task 2 (bulk) before Splink move.

**Which approach?**
