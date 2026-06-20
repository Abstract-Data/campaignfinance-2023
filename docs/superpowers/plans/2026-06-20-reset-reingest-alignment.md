# reset_and_reingest Vectorized Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Align `scripts/reset_and_reingest.py` with the modern vectorized ingest path (`cf load` / `run_vectorized` / `discover_state_files`) while preserving truncate + bootstrap semantics documented in AGENTS.md and `docs/RUNBOOK.md`.

**Architecture:** Keep the script's truncate SQL and explicit `manager.bootstrap()` call unchanged. Replace the per-file `manager.load_and_save_file()` loop (legacy ORM `UnifiedDatabaseManager` API) with a single `run_vectorized(engine, parquet_dir, state="texas", ...)` call matching `app/entrypoint.py::_run_vectorized_load`. Add CLI flags `--engine orm` (legacy escape hatch) and `--preset` (passed through for ORM path only). Add unit tests mocking truncate/bootstrap/vectorized dispatch.

**Tech Stack:** Python 3.12+, SQLModel/SQLAlchemy, Rich console, `app/core/ingest_vectorized.run_vectorized`, pytest + monkeypatch.

---

## File Map

| Path | Action | Responsibility |
|------|--------|----------------|
| `scripts/reset_and_reingest.py` | **Modify** | Vectorized default ingest; preserve truncate/bootstrap |
| `tests/test_reset_and_reingest.py` | **Create** | Dry-run, skip-ingest, engine selection tests |
| `docs/RUNBOOK.md` | **Modify** | Confirm script uses vectorized path (one paragraph) |
| `AGENTS.md` | **Verify** | Already documents `cf load` as canonical — add cross-ref to script |

**Current problem (lines 150–158):**

```python
count = manager.load_and_save_file(f, "texas")  # legacy ORM API
```

**Target (mirrors `entrypoint._run_vectorized_load`):**

```python
from app.core.ingest_vectorized import run_vectorized
results = run_vectorized(engine, parquet_dir, state="texas", dry_run=dry_run)
```

---

### Task 1: Characterization tests for existing CLI surface

**Files:**
- Create: `tests/test_reset_and_reingest.py`

- [ ] **Step 1: Write failing tests for argparse and dry-run**

```python
# tests/test_reset_and_reingest.py
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def repo_root(tmp_path, monkeypatch):
    texas = tmp_path / "tmp" / "texas"
    texas.mkdir(parents=True)
    (texas / "sample.parquet").write_bytes(b"PAR1")
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_dry_run_truncates_without_vectorized_load(repo_root, monkeypatch):
    import scripts.reset_and_reingest as mod

    mock_engine = MagicMock()
    mock_manager = MagicMock()
    mock_manager.engine = mock_engine
    monkeypatch.setattr(mod, "get_db_manager", lambda bootstrap=False: mock_manager)

    vectorized = MagicMock()
    monkeypatch.setattr(mod, "run_vectorized", vectorized)

    monkeypatch.setattr(
        "sys.argv",
        ["reset_and_reingest.py", "--dry-run"],
    )
    mod.main()

    mock_engine.connect.assert_called()
    vectorized.assert_not_called()


def test_skip_ingest_does_not_call_vectorized(repo_root, monkeypatch):
    import scripts.reset_and_reingest as mod

    mock_manager = MagicMock()
    monkeypatch.setattr(mod, "get_db_manager", lambda bootstrap=False: mock_manager)
    vectorized = MagicMock()
    monkeypatch.setattr(mod, "run_vectorized", vectorized)

    monkeypatch.setattr("sys.argv", ["reset_and_reingest.py", "--skip-ingest"])
    mod.main()

    vectorized.assert_not_called()
    mock_manager.bootstrap.assert_called_once()
```

- [ ] **Step 2: Run tests — expect FAIL**

Run: `uv run pytest tests/test_reset_and_reingest.py -v --tb=short`
Expected: **FAIL** — `AttributeError: module has no attribute 'run_vectorized'` (or import errors)

- [ ] **Step 3: Commit test file only (red phase documented)**

```bash
git add tests/test_reset_and_reingest.py
git commit -m "test: add reset_and_reingest CLI characterization tests (red)"
```

---

### Task 2: Refactor ingest path to vectorized default

**Files:**
- Modify: `scripts/reset_and_reingest.py`

- [ ] **Step 1: Add imports and module-level hook for testability**

```python
from app.core.ingest_vectorized import run_vectorized
from app.core.unified_database import get_db_manager
```

Replace inline `from app.core.unified_database import get_db_manager` inside `main()`.

- [ ] **Step 2: Replace `_ingest` function**

```python
def _ingest(engine, *, dry_run: bool, state: str = "texas") -> None:
    console.rule("[bold green]Re-ingest Texas parquet files (vectorized)")
    parquet_dir = ROOT / "tmp" / state
    if not parquet_dir.exists():
        console.print(
            f"[red]Parquet directory not found:[/red] {parquet_dir}\n"
            "Run [bold]cf prepare texas --skip-download[/bold] first."
        )
        sys.exit(1)

    files = sorted(parquet_dir.glob("**/*.parquet"))
    if not files:
        console.print(f"[red]No .parquet files found under {parquet_dir}[/red]")
        sys.exit(1)

    console.print(f"Found {len(files)} parquet file(s) under {parquet_dir.relative_to(ROOT)}")

    if dry_run:
        console.print("[yellow]DRY RUN — would call run_vectorized()[/yellow]")
        return

    results = run_vectorized(engine, parquet_dir, state=state, dry_run=False)
    loaded = int(results.get("loaded", 0))
    console.print(
        f"\n[bold green]Re-ingest complete.[/bold green] "
        f"{loaded} rows loaded across {results.get('families_run', 0)} families."
    )
```

- [ ] **Step 3: Update `main()` to pass engine**

```python
def main() -> None:
    parser = argparse.ArgumentParser(...)
    parser.add_argument("--dry-run", ...)
    parser.add_argument("--skip-ingest", ...)
    parser.add_argument(
        "--engine",
        choices=("vectorized", "orm"),
        default="vectorized",
        help="Ingest engine (default: vectorized, matches cf load).",
    )
    args = parser.parse_args()

    manager = get_db_manager(bootstrap=False)
    _truncate(manager.engine, dry_run=args.dry_run)
    _bootstrap(manager, dry_run=args.dry_run)

    if args.skip_ingest:
        console.print("[dim]--skip-ingest set; stopping after bootstrap.[/dim]")
    elif args.engine == "orm":
        _ingest_legacy(manager, dry_run=args.dry_run)  # preserve old loop behind flag
    else:
        _ingest(manager.engine, dry_run=args.dry_run)
```

Extract existing per-file loop into `_ingest_legacy(manager, *, dry_run)` unchanged for `--engine orm`.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/test_reset_and_reingest.py -v --tb=short`
Expected: **PASS**

- [ ] **Step 5: Add vectorized-invocation test**

```python
def test_default_ingest_calls_run_vectorized(repo_root, monkeypatch):
    import scripts.reset_and_reingest as mod

    mock_manager = MagicMock()
    monkeypatch.setattr(mod, "get_db_manager", lambda bootstrap=False: mock_manager)
    vectorized = MagicMock(return_value={"loaded": 42, "families_run": 3})
    monkeypatch.setattr(mod, "run_vectorized", vectorized)

    monkeypatch.setattr("sys.argv", ["reset_and_reingest.py"])
    mod.main()

    vectorized.assert_called_once()
    kwargs = vectorized.call_args.kwargs
    assert kwargs.get("state") == "texas"
    assert kwargs.get("dry_run") is False
```

Run: `uv run pytest tests/test_reset_and_reingest.py -v`
Expected: **PASS**

- [ ] **Step 6: Commit**

```bash
git add scripts/reset_and_reingest.py tests/test_reset_and_reingest.py
git commit -m "feat: align reset_and_reingest with vectorized ingest default"
```

---

### Task 3: Update RUNBOOK cross-reference

**Files:**
- Modify: `docs/RUNBOOK.md`

- [ ] **Step 1: Locate reset section and add note**

Under the reset/re-ingest commands, add:

```markdown
`scripts/reset_and_reingest.py` truncates unified tables, runs `bootstrap()`, then
calls `run_vectorized()` (same engine as `cf load` default). Use `--engine orm` only
for debugging ORM parity; production reloads should use the vectorized default.
```

- [ ] **Step 2: Commit**

```bash
git add docs/RUNBOOK.md
git commit -m "docs: note reset_and_reingest uses vectorized ingest"
```

---

### Task 4: Smoke verification (manual, non-production)

- [ ] **Step 1: Dry-run CLI**

Run: `uv run python scripts/reset_and_reingest.py --dry-run`
Expected: Prints truncate SQL + "would call run_vectorized()" without DB writes.

- [ ] **Step 2: Skip-ingest against SQLite (if available in dev)**

Run: `uv run python scripts/reset_and_reingest.py --skip-ingest` (requires configured DB)
Expected: Truncate + bootstrap only; no vectorized call.

- [ ] **Step 3: Run ruff + entrypoint tests**

Run: `uv run ruff check scripts/reset_and_reingest.py tests/test_reset_and_reingest.py && uv run pytest tests/test_entrypoint.py -v --tb=short -k load`
Expected: **PASS**

---

## Self-Review

| Check | Status |
|-------|--------|
| Truncate semantics preserved | ✅ `_truncate` unchanged |
| Bootstrap semantics preserved | ✅ `_bootstrap` unchanged |
| Vectorized default ingest | ✅ Task 2 |
| Legacy ORM escape hatch | ✅ `--engine orm` |
| Tests with exact pytest commands | ✅ Tasks 1–2 |
| RUNBOOK updated | ✅ Task 3 |
| Placeholder scan | ✅ None |

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-06-20-reset-reingest-alignment.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — Task 1 (red tests) → Task 2 (implementation) → Task 3 (docs) → Task 4 (smoke).

**2. Inline Execution** — @superpowers:executing-plans; verify dry-run before any real DB truncate.

**Which approach?**
