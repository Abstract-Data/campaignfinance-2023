# Review Remediation — Completion Report

**Branch:** `remediation/review-fixes`
**Last integrated commit:** Wave 4 partial (see git log on branch)
**Date:** 2026-05-25

## Gate status

| Gate | Result |
|------|--------|
| `uv run pytest tests app/tests --ignore=tests/resolve` | **91 passed**, 1 skipped |
| `uv run pytest tests/resolve -m "not integration"` | **457 passed** |
| Baseline (pre-Wave 4 new tests) | 77 → 91 (+14 Wave 4 unit tests) |

## Wave summary

### Wave 0–2 — Done

- Phase branch, Wave 1 correctness/security (1a–1h + 1z)
- Wave 2 decouple: session injection, dead layer retired, absolute imports (2z @ `0fb8362`)

### Wave 3 — Done

| Task | Commit | Notes |
|------|--------|-------|
| 3a god-module split | `3ee6412` | `enums`, `constants`, `builders`, `processor`, `models/tables.py` (980 LOC) |
| 3b validator mixins | `518c5ee` | TX `AddressValidatedModel` + OK `_helpers.py` |
| 3z importer rewire | `7002d04` | All importers → split modules; shim deleted; `RECORD_TYPE_CODES` in loader |

### Wave 4 — Partial

| Task | Status | Notes |
|------|--------|-------|
| 4a processor refactor | **Done** | `DETAIL_BUILDERS` registry, helpers, `process_record_stream`, `tests/test_processor.py` |
| 4b version helper | **Done** | `_record_version` + `_to_json_safe`; fixed `.count()` bug; `tests/test_versioning.py` |
| 4c N+1 + excepts | **Not done** | `builders.py` / `unified_state_loader.py` per-row sessions remain |
| 4d value objects | **Done** | `app/core/value_objects.py` + tests |
| 4e Base/Table split | **Not done** | Validators still use `table=True` as parse surface |
| 4z integration | **Partial** | Detail reset block trimmed in loader; streaming API exists but loader not wired to Polars scan |

### Wave 5 — Partial

| Task | Status | Notes |
|------|--------|-------|
| 5a core tests | **Partial** | Added processor, versioning, value-object tests; no full characterization suite |
| 5b scraper hardening | **Not done** | |
| 5c orchestration | **Not done** | |
| 5d docs/ADR | **Partial** | `docs/ARCHITECTURE-DIAGRAM.md`, ADR 0002 added |
| 5z final audit | **This document** | Residue greps not fully clean (see blockers) |

## Backlog items cleared (high signal)

- RF-DRY-002, RF-CPLX-001, P2-PERF-002 (processor API), RF-SMELL-003 (field-inference map in `builders.py`)
- RF-DRY-001, RF-DRY-003, RF-DRY-004, RF-MAGIC-001, RF-SMELL-002 (split), RF-SMELL-004
- P1/P2 items from Waves 1–2 per prior integration commits

## Blockers / remaining work

1. **`models/tables.py` (980 LOC)** — exceeds ~600 LOC target; multi-file split broke SQLAlchemy forward refs; needs relationship registry fix before re-splitting.
2. **TASK-4c** — Officer-linking and builder lookups still open per-row sessions; broad `except Exception` handlers remain in loader path.
3. **TASK-4e** — Base/Create/Table validator split not started (large TX/OK surface).
4. **TASK-4z** — Wire `production_loader` to `process_record_stream` + Polars lazy/batched reads; adopt value objects in `builders.py`.
5. **Residue** — `datetime.utcnow()` still in `_record_version` and some `unified_database.py` paths; `ic()` / bare excepts may remain outside touched files.
6. **GitButler** — `but commit` fails on hunk overlap; commits landed via `git commit-tree` + `git update-ref`.
7. **Wave 5** — Scraper hardening (R2), production orchestration (R9), coverage gate, end-to-end smoke not verified in this session.

## Commands to verify

```bash
uv run pytest tests app/tests --ignore=tests/resolve
uv run pytest tests/resolve -m "not integration"
uv run ruff check app/core/processor.py app/core/value_objects.py tests/test_processor.py tests/test_value_objects.py tests/test_versioning.py
```

## Recommended next session

1. Complete 4c on `remediation/wave-4/task-4c-n-plus-1-and-excepts`
2. Complete 4e incrementally (one TX record type at a time)
3. 4z: streaming loader + value-object adoption in builders
4. 5b/5c parallel, then 5z full backlog audit + coverage gate
