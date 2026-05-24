# Task 2b — Stage 5: classification into bands

> **Phase 2, round 1. Parallel-safe with 2a, 2c, 2e.** Blocks `task-2z`.
> Read the pack README, the Phase 2 README (staging contracts), and the spec.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Stage 5 turns
Splink's per-pair scores into decisions: high-confidence pairs auto-merge,
the medium band goes to a human review queue, low scores are rejected.

Reference: the spec's "The resolution pipeline → 5. Classify into bands" and the
`merge_review` policy (auto ≥ 0.99, review 0.80–0.99, reject below — starting
values, set in run config).

## Dependencies

- **Depends on:** Phase 1 merged; builds against `task-2a`'s `scored_pairs`
  contract (Phase 2 README).
- **Blocks:** `task-2z`.
- **Parallel-safe with:** 2a, 2c, 2e.

## Files

- **Create:** `app/resolve/stages/classify.py` — the stage-5 runner.
- **Create:** `tests/resolve/test_classify.py`.

New files only. Do **not** create any `__init__.py` — `task-2z` owns those.

## Interface contract

`classify.py` exports `run_classify_stage(session, run_id, config) -> dict`
(`Stage` protocol from Phase 1 `task-1d`). It:

1. Reads `scored_pairs` for `run_id` (the `task-2a` contract).
2. Reads `auto_threshold` and `review_threshold` from `config` (defaults 0.99
   and 0.80; per-entity-type overrides allowed).
3. Bands each pair: `score ≥ auto_threshold` → `auto`; `review_threshold ≤ score
   < auto_threshold` → `review`; below → `reject`.
4. Writes a `match_decision` row for **every** pair (`band`, `outcome`,
   `score`, `explanation_json` carried through).
5. `auto` pairs → append to the `merge_edges` staging table with
   `edge_source="probabilistic"` (per the Phase 2 README contract).
6. `review` pairs → insert `merge_review` rows with `status="pending"`.
7. Applies prior human decisions: a pair previously **approved** in
   `merge_review` is emitted as an `auto` edge (`edge_source="approved_review"`)
   regardless of score; a pair previously **rejected** is dropped and never
   re-queued.
8. Returns `{"auto_merges": <n>, "queued": <n>, "rejected": <n>}`.

## Steps (TDD)

- [ ] **Step 1** — Write `tests/resolve/test_classify.py`: failing tests that a
  score of 0.999 bands `auto`, 0.90 bands `review`, 0.50 bands `reject`; that
  each produces the right downstream row (`merge_edges` / `merge_review` /
  decision-only); and that every pair gets a `match_decision`.
- [ ] **Step 2** — Run; expect failure.
- [ ] **Step 3** — Implement `run_classify_stage()`. Run; pass. Commit.
- [ ] **Step 4** — Add tests for the prior-decision rules: an approved pair
  becomes an `auto` edge even at a low score; a rejected pair is not re-queued.
  Implement; run; pass; commit.
- [ ] **Step 5** — Add a test that per-entity-type threshold overrides in
  `config` are honored. Run; pass; commit.

## Acceptance criteria

- [ ] Pairs band correctly against configurable thresholds.
- [ ] Every pair writes a `match_decision`; auto/review pairs also write the
  correct downstream row.
- [ ] Approved reviews override score; rejected pairs never re-queue.
- [ ] `run_classify_stage()` conforms to the `Stage` protocol.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-2/task-2b-classification`. New files only. You append to
the `merge_edges` staging table (defined by Phase 1 `task-1f`) — that is a
cross-phase table, not a concurrently-edited file, so it is safe. Do not import
`task-2a`/`2c`. Do not create any `__init__.py`.
