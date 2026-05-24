# Task 0b — `UnifiedPledge` detail table

> **Phase 0, round 1. Parallel-safe with 0a, 0c–0f.** Blocks `task-0z`.
> Read the pack README and the source spec before starting.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Pledge
transactions (`PLDG`; files `pledges*.parquet`, `pldg_ss`, `pldg_t` in
`tmp/texas/`) are recognized as a `TransactionType.PLEDGE` but, unlike
contributions / loans / debts / credits / travel / assets, have **no detail
table**. This task closes that asymmetry with `UnifiedPledge`, mirroring
`UnifiedContribution`.

Authoritative field layout: `tmp/texas/CFS-ReadMe.txt`, record `PledgeData`
(`recordType = PLDG`). Reference `UnifiedContribution` in
`app/core/unified_sqlmodels.py` as the structural template.

## Dependencies

- **Depends on:** none — Phase 0 round 1.
- **Blocks:** `task-0z`.
- **Parallel-safe with:** 0a, 0c, 0d, 0e, 0f.

## Files

- **Create:** `app/core/source_models/pledges.py` — the `UnifiedPledge` SQLModel.
- **Create:** `app/core/source_models/pledges_ingest.py` — `build_pledge()`.
- **Create:** `tests/resolve/test_pledges.py`.

This task creates new files only. It does **not** edit any existing file.

## Interface contract

`UnifiedPledge` (table `unified_pledges`) — mirrors `UnifiedContribution`:

- `id: int` PK, `uuid: str`
- `transaction_id: int` — unique FK → `unified_transactions.id` (one-to-one)
- `pledgor_entity_id: int` FK → `unified_entities.id`
- `recipient_entity_id: int` FK → `unified_entities.id`
- `state_id: int | None` FK → `states.id`
- `amount: Decimal | None` (`Numeric(15,2)`)
- `pledge_date: date | None` (indexed)
- `is_fulfilled: bool` (default `False`)
- `description: str | None`, `metadata_json: str | None`
- `created_at`, `updated_at`

`build_pledge(transaction, pledgor_entity, recipient_entity, raw: dict, *, state_id: int) -> UnifiedPledge`
— constructs the detail row from an already-built `UnifiedTransaction` and the
contributor/recipient entities, matching how `UnifiedSQLDataProcessor` builds
`UnifiedContribution` for `TransactionType.CONTRIBUTION`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_pledges.py`: a failing test that
  `build_pledge()` produces a `UnifiedPledge` with `amount`, `pledge_date`, and
  both entity FKs populated from a sample PLDG transaction.
- [ ] **Step 2** — Run `uv run pytest tests/resolve/test_pledges.py -v`; expect
  failure.
- [ ] **Step 3** — Implement `app/core/source_models/pledges.py` per the
  interface contract (copy the shape of `UnifiedContribution`).
- [ ] **Step 4** — Implement `build_pledge()` in `pledges_ingest.py`.
- [ ] **Step 5** — Run the test; expect pass. Commit.
- [ ] **Step 6** — Add a test that `unified_pledges` creates via
  `SQLModel.metadata.create_all` and enforces the one-to-one `transaction_id`
  uniqueness. Run; pass; commit.

## Acceptance criteria

- [ ] `UnifiedPledge` matches the interface contract; table creates cleanly.
- [ ] `transaction_id` is unique (one-to-one with the transaction).
- [ ] `build_pledge()` is covered by passing tests and importable from
  `app.core.source_models.pledges_ingest`.
- [ ] No existing file is modified.

## Collision protocol

Branch `resolve/phase-0/task-0b-pledges`. New files only — do not create
`app/core/source_models/__init__.py`. Note: `task-0z` will register
`UnifiedPledge` into the loader dispatch so PLDG transactions get a detail row;
your job ends at the model + `build_pledge()` + tests.
