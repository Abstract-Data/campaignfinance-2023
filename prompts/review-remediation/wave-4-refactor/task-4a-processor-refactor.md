# Task 4a — Detail-builder registry, `process_record` extraction, streaming

> **Wave 4, parallel. Branch `remediation/wave-4/task-4a-processor-refactor`.**
> Requires Wave 3 merged. Read the pack README, the Refactoring Report
> (**RF-DRY-002**, **RF-CPLX-001**, **RF-SMELL-003**) and the Code Review Report
> (**P2-PERF-002**).

## Context

`UnifiedSQLDataProcessor.process_record` (now in `app/core/processor.py` after
the Wave 3 split) is a ~221-line god function (cyclomatic complexity > 25). It
contains six near-identical `if transaction.transaction_type == X:` detail
blocks, a 10-branch type-inference chain, and processes whole files in memory.

## Files

- **Modify:** `app/core/processor.py`
- **Modify:** `scripts/loaders/production_loader.py` (the `transaction.X = None`
  reset block at `:148-156`, the shotgun-surgery counterpart)
- **Create:** `tests/test_processor.py`

## What to implement

- **RF-DRY-002** — Replace the six detail blocks (contribution/loan/debt/credit/
  travel/asset) with a `DETAIL_BUILDERS: dict[TransactionType, Callable]`
  registry; each entry a small builder function. `process_record` looks up
  `DETAIL_BUILDERS.get(txn.transaction_type)`. Remove the matching six-line
  reset block in `production_loader.py:148-156`.
- **RF-CPLX-001** — Extract `_build_participants(raw)`,
  `_attach_transaction_persons(txn, participants)`, and `_attach_detail_record`
  helpers so `process_record` becomes a ~30-line orchestrator.
- **RF-SMELL-003** — Replace the 10-branch `_determine_transaction_type`
  if/elif chain with an ordered list of `(keyword_tuple, TransactionType)` pairs
  (or a keyword→type map) — consistent with the dict maps the function already
  uses for its first two strategies.
- **P2-PERF-002 / R11** — Add a generator-based `process_record_stream` so the
  loader can iterate records without materializing the whole file; keep
  `process_records` as a thin wrapper. (The loader's Polars read stays as 4c's
  concern — here just provide the streaming processor API.)
- Narrow any bare `except` in the code you move; replace `ic()` with `Logger`.

## Steps

- [ ] **1** — `tests/test_processor.py`: failing per-builder tests — one record
  of each `TransactionType` produces the right detail record; a type-inference
  test; a streaming test (`process_record_stream` yields lazily).
- [ ] **2** — Run; expect fail. **3** — Implement registry + extraction + keyword
  map + streaming. **4** — Run; pass. Full suite green. `ruff check --fix`. Commit.

## Acceptance criteria

- [ ] `process_record` is < 50 lines; the six detail blocks are a registry.
- [ ] `_determine_transaction_type` uses a map, not a 10-branch chain.
- [ ] `process_record_stream` exists and is generator-based.
- [ ] The `production_loader.py:148-156` reset block is gone.
- [ ] Per-builder unit tests pass.

## Collision protocol

You own `app/core/processor.py` and the `:148-156` block of
`production_loader.py`. Task 4c owns `builders.py` + the rest of the loader's
session logic — coordinate only via the README contract, do not edit `builders.py`.
