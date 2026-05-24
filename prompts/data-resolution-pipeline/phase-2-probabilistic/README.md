# Phase 2 — Probabilistic matching

**Goal:** Add the probabilistic matching layer — Splink scores the fuzzy
candidate pairs, pairs are classified into bands, and connected-components
clustering replaces Phase 1's trivial clustering. This is the "true fuzzy
matching" the project is really after.

**Outcome when done:** A full run scores every blocked candidate pair, auto-merges
the high-confidence matches, queues the medium band for review, clusters the
merge edges, and rebuilds canonical records — all measured against a labeled
golden set with a precision floor enforced in CI.

**Prerequisite:** Phase 1 merged (the deterministic pipeline runs end-to-end).

Reference: the spec's "Matching engine", "The resolution pipeline" stages 4–6,
the `merge_review` band policy, and the mega-cluster guard under "Error handling".

## Rounds

**Round 1 — stages + test harness (run concurrently):**

| Task | Delivers | Owns |
|------|----------|------|
| `task-2a` | Stage 4 — Splink scoring, per-entity-type comparison config | `app/resolve/stages/score.py`, `app/resolve/splink_config/` |
| `task-2b` | Stage 5 — classification into `auto`/`review`/`reject` bands | `app/resolve/stages/classify.py` |
| `task-2c` | Stage 6 — connected-components clustering + mega-cluster guard | `app/resolve/stages/cluster.py` |
| `task-2e` | Golden-set fixtures + precision/recall regression harness | `tests/resolve/golden/`, `tests/resolve/test_match_quality.py` |

**Round 2 (after round 1 merges):**

| Task | Delivers | Owns |
|------|----------|------|
| `task-2d` | Stage 7 update — survivorship consumes probabilistic clusters; field-level provenance | edits `app/resolve/stages/survivorship.py` |

**Then — `task-2z` integration:** wires stages 4→5→6 into the pipeline, sets
starting thresholds, runs a full Texas pass, gates on the golden-set precision.

## Inter-stage staging-table contracts

Round-1 tasks are parallel because they communicate only through staging
tables, never by importing each other. Build to these contracts:

- **`candidate_pairs`** (input, from Phase 1 `task-1e`) — `run_id`,
  `source_a_type`, `source_a_id`, `source_b_type`, `source_b_id`, `rule_name`.
- **`scored_pairs`** — written by `2a`, read by `2b`. `run_id`, the four
  `source_*` columns, `entity_type`, `score` (float 0–1), `explanation_json`.
- **`merge_edges`** — created by Phase 1 `task-1f`; `2b` appends probabilistic
  `auto` edges to it. `run_id`, `source_a_*`, `source_b_*`, `edge_source`
  (`deterministic`/`probabilistic`/`approved_review`).
- **`clusters`** — written by `2c`, read by `2d`. `run_id`, `cluster_id`,
  `source_type`, `source_id`, `entity_type`, `held_for_review` (bool — set by
  the mega-cluster guard).

## Collision-freedom

- Round 1: four disjoint paths (`score.py`+`splink_config/`, `classify.py`,
  `cluster.py`, the golden-set test dir). No overlap.
- `task-2d` is the only Phase 2 task that edits `survivorship.py`, and it runs
  alone in round 2.
- Splink + DuckDB are new dependencies — `task-2a` adds them via `uv add` and
  declares `pyproject.toml`/`uv.lock` as its one shared-file exception.
- Thresholds live in run config (snapshotted to `match_run.config_json`);
  `task-2z` sets the starting values.

## Verifying the phase

`task-2z` is done when `uv run pytest tests/resolve/` is green (including the
golden-set precision gate), a full Texas run completes through stages 1–7, the
`merge_review` queue is populated for the medium band, and no auto-published
cluster exceeds the mega-cluster size cap.
