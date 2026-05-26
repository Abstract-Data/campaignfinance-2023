# ADR 0003: AI governance for entity resolution

**Date:** 2026-05-25
**Status:** accepted

## Context

The resolution pipeline (`app/resolve/`) links donor, filer, and vendor records
across filings using a mix of deterministic rules and **Splink** probabilistic
record linkage. Splink scores candidate pairs with an EM-trained Fellegi–Sunter
model per entity type (person, organization, committee). Matches above
configurable thresholds become merge edges; human reviewers can approve or reject
borderline pairs via the `merge_review` queue.

There was no recorded policy for how probabilistic matches are explained,
audited, overridden, or monitored for drift — a gap flagged in the developer
assessment risk register (R3).

## Decision

### Why Splink alongside deterministic matching

- **Deterministic fast-path** (`app/resolve/stages/fastpath.py`) handles exact
  keys and high-confidence rules first — no model training required.
- **Splink** handles fuzzy name/address variation where rules would either miss
  true duplicates or over-merge. Splink is open source, runs locally on run data
  (DuckDB backend), and stores per-pair `explanation_json` in `scored_pairs`.
- We do **not** use external LLM or cloud ML APIs for entity resolution; all
  linkage runs in-process against PostgreSQL/SQLite staging tables.

### Training data and drift detection

- Splink models are **fit per run** on that run's `resolution_input` rows via EM
  (`app/resolve/stages/score.py`). There is no long-lived global model file.
- **Drift signal:** compare distribution of `scored_pairs.score` and auto-merge
  rates run-over-run for the same `entity_type` and `state_code`. Material shifts
  trigger manual review before promoting survivorship output.
- **Minimum volume gate:** scoring skips EM when fewer than four records exist
  for an entity type (`_MIN_RECORDS_FOR_EM` in `score.py`).

### Transparency and human review

- Every scored pair persists `explanation_json` (Splink match weights per
  comparison column).
- The reviewer CLI (`python -m app.resolve.review.cli`) renders explanations via
  `app/resolve/review/explain.py` — side-by-side source records plus a
  human-readable waterfall.
- Pairs in the **review band** (between auto-merge and auto-reject thresholds)
  land in `merge_review` with status `pending` until a named reviewer approves
  or rejects.

### Bias and fairness

- Campaign-finance PII is predominantly **names and addresses**, not protected
  demographic attributes. Splink comparisons use normalized name tokens,
  phonetic keys, and address parts — not race, gender, or age.
- **Shared-address down-weighting:** TF adjustment on address comparisons reduces
  false merges at registered-agent hubs and large PO Box facilities.
- **Monitoring:** periodic audits sample approved merges stratified by
  `entity_type` and `state_code`; false-positive reports feed back into blocking
  key and threshold tuning — not into silent auto-retrain.

### Override and reversibility

- Reviewers **approve** or **reject** via CLI; decisions write to `match_decision`
  and update `merge_review.status`.
- `EntityCrosswalk.match_method` records the path: `exact`, `deterministic_rule`,
  `probabilistic`, or `approved_review`.
- **`unmerge` / reverse run** (`app/resolve/reverse.py`) removes crosswalk rows
  and canonical records produced by a specific `match_run`, preserving audit
  history in `resolution_audit_log`.

## Consequences

- Probabilistic linkage is auditable: every merge has a score, explanation JSON,
  and recorded method — but operators must run the review CLI for borderline pairs.
- Per-run EM means reproducibility depends on input row order and volume; golden-set
  tests (`tests/resolve/golden/`) are the regression gate.
- No automated erasure of merged canonical rows; see ADR 0002 for retention policy.
- Future use of external ML or LLM-assisted matching requires a new ADR and human
  approval before production deployment.
