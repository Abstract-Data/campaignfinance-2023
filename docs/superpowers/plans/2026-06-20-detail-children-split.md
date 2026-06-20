# detail_children Split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract shared id-map helpers into `id_maps.py`, split the 1574-line `detail_children.py` into focused modules by concern, and fix the inverted import where `flat_txns_dims` imports private helpers from `detail_children`.

**Architecture:** Introduce `app/core/ingest_vectorized/id_maps.py` as the single source for SQLAlchemy-reflection id maps keyed identically to dedup indexes. Move `detail_children` into a subpackage (`families/detail_children/`) split by concern: specs/constants, Polars expressions, dim writes, transaction writes, per-type detail builders, and a thin worker orchestrator. `flat_txns_dims` imports from `id_maps`, not from `detail_children`. Keep `register(DetailChildrenWorker())` in the subpackage `__init__.py` so `families/__init__.py` import chain is unchanged.

**Tech Stack:** Python 3.12+, Polars LazyFrame/DataFrame, SQLAlchemy Core reflection, pytest equivalence harness, ruff.

---

## File Map (create/modify before tasks)

| Path | Action | Responsibility |
|------|--------|----------------|
| `app/core/ingest_vectorized/id_maps.py` | **Create** | `_reflect`, `_entity_id_map`, `_address_id_map`, `_person_id_map`, `_txn_id_map`, `_committee_entity_map`, `_loan_pk_map`, `_enum_name`, `_lower_or_none` |
| `app/core/ingest_vectorized/families/detail_children/specs.py` | **Create** | `TypeSpec`, `_SPECS`, `_GUARANTOR_COLS`, `_guarantor_source_cols`, per-type constants |
| `app/core/ingest_vectorized/families/detail_children/exprs.py` | **Create** | `_read`, `_ensure_cols`, `_spec_cols`, `_cs`, `_nullify`, `_opt_col`, `_person_type_expr`, `_full_name`, `_spec_party_frame`, `_address_has_anchor`, `_addr_key_cols`, `_norm_name`, `_get_unstripped`, `_guar`, `_pledge_date_expr` |
| `app/core/ingest_vectorized/families/detail_children/dims.py` | **Create** | `_write_committees`, `_write_dims` and dim-specific helpers from worker |
| `app/core/ingest_vectorized/families/detail_children/transactions.py` | **Create** | `_write_transactions` |
| `app/core/ingest_vectorized/families/detail_children/builders.py` | **Create** | `_build_loan`, `_build_debt`, `_build_credit`, `_build_travel`, `_build_asset`, `_build_pledge`, `_build_guarantors`, `_party_keys`, `_join_party_entity` |
| `app/core/ingest_vectorized/families/detail_children/worker.py` | **Create** | `DetailChildrenWorker` orchestrator (`run` only delegates) |
| `app/core/ingest_vectorized/families/detail_children/__init__.py` | **Create** | Re-export `DetailChildrenWorker`; call `register(DetailChildrenWorker())` |
| `app/core/ingest_vectorized/families/detail_children.py` | **Delete** | Replaced by subpackage |
| `app/core/ingest_vectorized/families/flat_txns_dims.py` | **Modify** | Import id maps from `id_maps`; remove `detail_children` import |
| `app/core/ingest_vectorized/families/__init__.py` | **Modify** | `from .detail_children import DetailChildrenWorker` (subpackage) |
| `tests/core/test_id_maps.py` | **Create** | Unit tests for id map key shapes |
| `tests/ingest_equivalence/test_detail_children_family.py` | **Verify unchanged** | Equivalence gate must still pass |

**Out of scope (follow-up):** `filer.py`, `cand.py`, `flat_txns_detail.py` still duplicate id-map helpers — do not touch in this plan.

---

### Task 1: Extract `id_maps.py` with characterization tests

**Files:**
- Create: `app/core/ingest_vectorized/id_maps.py`
- Create: `tests/core/test_id_maps.py`
- Modify: `app/core/ingest_vectorized/families/detail_children.py` (temporary — re-export from id_maps until subpackage lands)

- [ ] **Step 1: Write failing test for address id-map schema**

```python
# tests/core/test_id_maps.py
from __future__ import annotations

import polars as pl
from sqlmodel import Session, SQLModel, create_engine

from app.core.models import UnifiedAddress
from app.core.ingest_vectorized.id_maps import address_id_map


def test_address_id_map_empty_engine_returns_empty_frame():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=[UnifiedAddress.__table__])
    frame = address_id_map(engine)
    assert frame.height == 0
    assert set(frame.columns) == {"address_id", "_k_s1", "_k_city", "_k_state", "_k_zip"}


def test_address_id_map_lowercases_keys():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine, tables=[UnifiedAddress.__table__])
    with Session(engine) as session:
        session.add(
            UnifiedAddress(
                street_1="123 MAIN ST",
                city="Austin",
                state="TX",
                zip_code="78701",
            )
        )
        session.commit()
    frame = address_id_map(engine)
    row = frame.row(0, named=True)
    assert row["_k_s1"] == "123 main st"
    assert row["_k_city"] == "austin"
    assert isinstance(row["address_id"], int)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/core/test_id_maps.py -v --tb=short`
Expected: **FAIL** with `ModuleNotFoundError: No module named 'app.core.ingest_vectorized.id_maps'`

- [ ] **Step 3: Create `id_maps.py` by moving helpers from `detail_children.py`**

Copy lines 449–586 from `app/core/ingest_vectorized/families/detail_children.py` into `id_maps.py`. Rename public functions (drop leading underscore for module API):

```python
# app/core/ingest_vectorized/id_maps.py (excerpt)
from __future__ import annotations

from typing import Any

import polars as pl
from sqlalchemy import MetaData, Table, select

from app.core.ingest_vectorized import common


def reflect(engine: Any, name: str) -> Table:
    return Table(name, MetaData(), autoload_with=engine)


def entity_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """{entity_type, normalized_name} -> entity id for this state."""
    ...


def address_id_map(engine: Any) -> pl.DataFrame:
    """4-field lower-cased address key -> address surrogate id."""
    ...


def person_id_map(engine: Any, state_id: int) -> pl.DataFrame:
    """Person dedup key -> person id; org rows collapsed via common.collapse_org_person_key."""
    ...


def txn_id_map(
    engine: Any,
    state_id: int,
    transaction_types: frozenset[str],
) -> pl.DataFrame:
    """{transaction_id, transaction_type} -> txn surrogate id for this state.

    *transaction_types* are unified enum names (e.g. ``CREDIT``), NOT TEC record
    codes. Callers derive them from ``TypeSpec.transaction_type`` — do NOT import
    ``_SPECS`` here (avoids circular deps with detail_children).
    """
    ...
```

Keep `_enum_name` and `_lower_or_none` as private module functions.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/core/test_id_maps.py -v --tb=short`
Expected: **PASS** (2 tests)

- [ ] **Step 5: Wire `detail_children.py` to re-export from `id_maps` (compat shim)**

At top of `detail_children.py`, replace inline definitions with:

```python
from app.core.ingest_vectorized.id_maps import (
    address_id_map as _address_id_map,
    entity_id_map as _entity_id_map,
    person_id_map as _person_id_map,
    reflect as _reflect,
    committee_entity_map as _committee_entity_map,
    loan_pk_map as _loan_pk_map,
    txn_id_map as _txn_id_map,
)
```

Delete the moved function bodies from `detail_children.py`.

Update both `_txn_id_map` call sites to pass transaction types explicitly:

```python
want_types = frozenset(_SPECS[rt].transaction_type for rt in ordered if rt in _SPECS)
txn_map = _txn_id_map(engine, ctx.state_id, want_types)
```

- [ ] **Step 6: Run equivalence gate**

Run: `uv run pytest tests/ingest_equivalence/test_detail_children_family.py -v --tb=short`
Expected: **PASS**

- [ ] **Step 7: Commit**

```bash
git add app/core/ingest_vectorized/id_maps.py tests/core/test_id_maps.py app/core/ingest_vectorized/families/detail_children.py
git commit -m "refactor: extract vectorized id_maps module from detail_children"
```

---

### Task 2: Fix `flat_txns_dims` import direction

**Files:**
- Modify: `app/core/ingest_vectorized/families/flat_txns_dims.py:25-29`
- Test: `tests/ingest_equivalence/test_flat_txns_family.py`

- [ ] **Step 1: Write failing import-direction test**

```python
# tests/core/test_id_maps.py (append)
def test_flat_txns_dims_does_not_import_detail_children():
    import ast
    from pathlib import Path

    src = Path("app/core/ingest_vectorized/families/flat_txns_dims.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module and "detail_children" in node.module:
            raise AssertionError("flat_txns_dims must not import detail_children")
```

- [ ] **Step 2: Run test — expect FAIL**

Run: `uv run pytest tests/core/test_id_maps.py::test_flat_txns_dims_does_not_import_detail_children -v`
Expected: **FAIL** with `AssertionError: flat_txns_dims must not import detail_children`

- [ ] **Step 3: Update imports in `flat_txns_dims.py`**

```python
from app.core.ingest_vectorized.id_maps import (
    address_id_map as _address_id_map,
    entity_id_map as _entity_id_map,
    person_id_map as _person_id_map,
)
```

Update anti-join methods to call `_address_id_map(ctx.engine)` etc. (same call sites, new import).

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/core/test_id_maps.py tests/ingest_equivalence/test_flat_txns_family.py -v --tb=short`
Expected: **PASS**

- [ ] **Step 5: Commit**

```bash
git add app/core/ingest_vectorized/families/flat_txns_dims.py tests/core/test_id_maps.py
git commit -m "refactor: flat_txns_dims imports id_maps not detail_children"
```

---

### Task 3: Create `detail_children` subpackage — specs + exprs

**Files:**
- Create: `app/core/ingest_vectorized/families/detail_children/specs.py`
- Create: `app/core/ingest_vectorized/families/detail_children/exprs.py`

- [ ] **Step 1: Move specs block (lines 66–263) to `specs.py`**

Export: `TypeSpec`, `_SPECS`, `_PLACEHOLDER_NAMES_UPPER`, `_guarantor_source_cols`, `_spec_cols`.

- [ ] **Step 2: Move expression helpers (lines 265–447, 1543–1571) to `exprs.py`**

Import `TypeSpec` from `.specs`; import `common` from `app.core.ingest_vectorized`.

- [ ] **Step 3: Run equivalence gate (still using monolithic worker in old file)**

Run: `uv run pytest tests/ingest_equivalence/test_detail_children_family.py -v`
Expected: **PASS** (no behavior change yet if worker still in old file)

- [ ] **Step 4: Commit**

```bash
git add app/core/ingest_vectorized/families/detail_children/
git commit -m "refactor: extract detail_children specs and exprs modules"
```

---

### Task 4: Split worker into dims / transactions / builders / worker

**Files:**
- Create: `dims.py`, `transactions.py`, `builders.py`, `worker.py`, `__init__.py`
- Delete: `families/detail_children.py`

- [ ] **Step 1: Move `_write_committees` + `_write_dims` (+ helpers) to `dims.py`**

`DetailChildrenWorker._write_committees` and `_write_dims` become module-level functions taking `(worker, frames, ordered, ctx)` OR keep as methods on a mixin — prefer **methods on `DetailChildrenWorker` in `worker.py` delegating to functions in `dims.py`** to minimize signature churn.

- [ ] **Step 2: Move `_write_transactions` to `transactions.py`**

- [ ] **Step 3: Move `_build_loan` … `_build_guarantors` to `builders.py`**

- [ ] **Step 4: Implement thin `worker.py`**

```python
# app/core/ingest_vectorized/families/detail_children/worker.py
class DetailChildrenWorker:
    record_types = frozenset({"LOAN", "DEBT", "CRED", "TRVL", "ASSET", "PLDG"})
    priority = 11

    def run(self, files_by_type, ctx):
        from .dims import write_committees, write_dims
        from .transactions import write_transactions
        from .builders import write_details
        ...
```

Target: each file ≤ 400 LOC; `worker.py` ≤ 80 LOC.

- [ ] **Step 5: Create `__init__.py` with register**

```python
from app.core.ingest_vectorized.registry import register
from .worker import DetailChildrenWorker

register(DetailChildrenWorker())
__all__ = ["DetailChildrenWorker"]
```

- [ ] **Step 6: Delete `families/detail_children.py`; update `families/__init__.py`**

```python
from app.core.ingest_vectorized.families.detail_children import DetailChildrenWorker  # noqa: F401
```

- [ ] **Step 7: Run full ingest equivalence suite**

Run: `uv run pytest tests/ingest_equivalence/test_detail_children_family.py tests/ingest_equivalence/ -v --tb=short -x`
Expected: **PASS**

- [ ] **Step 8: Run ruff**

Run: `uv run ruff check app/core/ingest_vectorized/ tests/core/test_id_maps.py && uv run ruff format --check app/core/ingest_vectorized/`
Expected: **PASS** (fix any issues before commit)

- [ ] **Step 9: Commit**

```bash
git add app/core/ingest_vectorized/families/detail_children/ app/core/ingest_vectorized/families/__init__.py
git rm app/core/ingest_vectorized/families/detail_children.py
git commit -m "refactor: split detail_children into subpackage by concern"
```

---

### Task 5: Line-count guard + GitNexus detect

- [ ] **Step 1: Add LOC guard test**

```python
# tests/core/test_detail_children_structure.py
from pathlib import Path

MAX_LOC = 450
PKG = Path("app/core/ingest_vectorized/families/detail_children")

def test_detail_children_modules_under_loc_cap():
    for py in PKG.glob("*.py"):
        loc = len(py.read_text().splitlines())
        assert loc <= MAX_LOC, f"{py.name} has {loc} lines (cap {MAX_LOC})"
```

Run: `uv run pytest tests/core/test_detail_children_structure.py -v`
Expected: **PASS**

- [ ] **Step 2: Run GitNexus detect_changes**

Run: `npx gitnexus detect_changes --scope all` (or MCP equivalent)
Expected: Changes limited to ingest_vectorized families + new tests.

- [ ] **Step 3: Commit**

```bash
git add tests/core/test_detail_children_structure.py
git commit -m "test: enforce detail_children subpackage LOC cap"
```

---

## Self-Review

| Check | Status |
|-------|--------|
| Spec coverage: id_maps extraction | ✅ Task 1 |
| Spec coverage: flat_txns_dims import fix | ✅ Task 2 |
| Spec coverage: detail_children split by concern | ✅ Tasks 3–4 |
| Spec coverage: equivalence gate preserved | ✅ Multiple verification steps |
| Placeholder scan (TBD/TODO/add tests) | ✅ None |
| Type consistency (`engine: Any`, `pl.DataFrame`) | ✅ Matches existing family code |
| No behavior change | ✅ Equivalence harness is the gate |

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-20-detail-children-split.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task (Tasks 1→5 sequential); run equivalence gate after Task 4; two-stage review (`spec-reviewer` then `code-reviewer`) after Task 4.

**2. Inline Execution** — use @superpowers:executing-plans in one session; checkpoint after Task 2 (import fix) and Task 4 (subpackage split).

**Which approach?**
