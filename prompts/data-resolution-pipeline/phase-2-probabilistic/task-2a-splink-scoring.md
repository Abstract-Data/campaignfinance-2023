# Task 2a — Stage 4: Splink probabilistic scoring

> **Phase 2, round 1. Parallel-safe with 2b, 2c, 2e.** Blocks `task-2z`.
> Read the pack README, the Phase 2 README (staging contracts), and the spec.

## Context

`campaignfinance` project (Python 3.12, `uv`, SQLModel, Postgres). Phase 1
resolves only exact matches. This task adds stage 4 — probabilistic record
linkage with [Splink](https://moj-analytical-services.github.io/splink/), which
scores each blocked candidate pair with a calibrated 0–1 match probability and
an explainable per-comparison breakdown.

Reference: the spec's "Matching engine" and "The resolution pipeline → 4.
Probabilistic score (Splink)".

## Dependencies

- **Depends on:** Phase 1 merged.
- **Blocks:** `task-2b`, `task-2z`.
- **Parallel-safe with:** 2b, 2c, 2e.

## Files

- **Create:** `app/resolve/stages/score.py` — the stage-4 runner.
- **Create:** `app/resolve/splink_config/person.py`, `organization.py`,
  `committee.py` — per-entity-type Splink comparison settings.
- **Create:** `tests/resolve/test_score.py`.
- **Modify (declared shared-file exception):** `pyproject.toml` / `uv.lock` —
  via `uv add splink duckdb`. This is the one shared file this task edits; no
  other Phase 2 task touches it.

Do **not** create any `__init__.py` — `task-2z` owns those.

## Interface contract

`score.py` exports `run_score_stage(session, run_id, config) -> dict`
(conforming to the `Stage` protocol from Phase 1 `task-1d`). It:

1. Reads the `candidate_pairs` staging table for `run_id` (Phase 1 `task-1e`
   output) and the `resolution_input` features (`task-1c`).
2. For each `entity_type`, runs Splink on the **DuckDB** backend using that
   type's comparison config. m/u probabilities are estimated from the data
   (EM); seed any randomness from `config` for determinism.
3. **Address comparisons must use Splink's term-frequency adjustment** so a
   common address (registered-agent, large building) contributes little weight
   — see the spec's address-as-shared-hub section.
4. Writes one `scored_pairs` row per candidate pair (`score` 0–1,
   `explanation_json` = Splink's per-comparison contribution breakdown), per the
   Phase 2 README contract.
5. Returns `{"pairs_compared": <n>}`.

Each `splink_config/<type>.py` exports a `comparison_settings` object naming the
standardized fields compared (name parts, org name, address parts) and the
similarity levels per field. Persons compare name + address; organizations
compare normalized org name + address; committees lean on `filer_id` plus name.

## Steps (TDD)

- [ ] **Step 1** — `uv add splink duckdb`; commit the dependency change alone.
- [ ] **Step 2** — Write `tests/resolve/test_score.py`: a failing test that
  `run_score_stage()` on a small seeded `candidate_pairs` + `resolution_input`
  fixture produces a `scored_pairs` row per pair with a `score` in `[0, 1]` and
  a non-empty `explanation_json`.
- [ ] **Step 3** — Run; expect failure.
- [ ] **Step 4** — Implement the three `splink_config/<type>.py` files.
- [ ] **Step 5** — Implement `run_score_stage()` in `score.py`. Run the test;
  expect pass. Commit.
- [ ] **Step 6** — Add a determinism test: scoring the same fixture twice yields
  identical scores. Add a test that a high-frequency address value contributes
  near-zero weight (term-frequency adjustment is active). Run; pass; commit.

## Acceptance criteria

- [ ] `run_score_stage()` scores every candidate pair and writes `scored_pairs`
  per the contract; conforms to the `Stage` protocol.
- [ ] Per-entity-type comparison configs exist and are used.
- [ ] Address comparison uses term-frequency adjustment.
- [ ] Scoring is deterministic (seeded); `explanation_json` is populated.
- [ ] `splink` and `duckdb` are added to `pyproject.toml`.
- [ ] No file outside the Files list is touched.

## Collision protocol

Branch `resolve/phase-2/task-2a-splink-scoring`. New files plus the one declared
`uv add` edit. Communicate downstream only via the `scored_pairs` staging table
— do not import `task-2b`/`2c`. Do not create any `__init__.py`.
