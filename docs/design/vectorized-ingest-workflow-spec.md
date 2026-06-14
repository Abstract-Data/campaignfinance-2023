# P1–P4 Vectorized Ingest — Multi-Agent Workflow Spec

Status: DRAFT (2026-06-14)
Companion to: `docs/design/vectorized-ingest-plan.md` (the phased plan)
Runnable script: `.claude/workflows/vectorized-ingest.js`

This spec describes how to **batch** the vectorized-ingest implementation (plan
phases P1–P4) as a deterministic multi-agent workflow, where every unit of work is
gated by the P0 equivalence harness (`app/core/ingest_equivalence.py`).

## When to launch
Prerequisites (do NOT run before these are true):
1. **P0 merged** — the equivalence harness + golden fixtures are on `main`.
2. **Scaffold exists** — an `app/core/ingest_vectorized/` package with the engine
   skeleton (read fixtures → write to a target engine) and a comparison entrypoint
   `run_vectorized(engine, fixtures_dir)` the verifier can call. (One bootstrap agent
   can create this skeleton; see the script's Phase 0.)
3. The team has agreed the row-for-row diff is the merge criterion.

Until then this is a **spec**, not a run. Launch with
`Workflow({ name: "vectorized-ingest" })` when ready.

## The gate (non-negotiable)
For each family, the build is "done" only when, on the golden fixtures:

```
orm_snap = snapshot_unified(orm_engine)
vec_snap = snapshot_unified(vectorized_engine)
diff_snapshots(orm_snap, vec_snap)  # restricted to the family's tables == []
```

A verifier agent runs this and returns a structured verdict. No self-attestation —
the verdict is the diff result.

## Work units (record-type families)
Independent behind the harness, so they fan out in parallel:

| Family | Record types | Target tables | Notes |
|---|---|---|---|
| `refs` | FILER, CVR1, FINL, SPAC, CVR2, CVR3, EXCAT | unified_committees, unified_reports, committee_persons, lookups | FK roots; build first |
| `flat_txns` | RCPT, EXPN | unified_transactions, _contributions, _expenditures, _transaction_persons, persons/entities/addresses dims | the volume; proves dims+joins |
| `detail_children` | LOAN, DEBT, CRED, TRVL, ASSET, PLDG | _loans/_debts/_credits/_travel/_assets/_pledges + loan_guarantors | struct/explode for children |
| `cand` | CAND | _transaction_persons (role=CANDIDATE) | enrichment join to expenditure id-map |

## Orchestration shape
`pipeline(FAMILIES, implement, verify)` — each family flows implement → verify with
no barrier, so a fast family verifies while a slow one is still being implemented:

- **implement(family)** — implementer agent writes the vectorized transform for that
  family in `app/core/ingest_vectorized/<family>.py` as pure Polars expressions
  (zero `map_elements` in the hot path), reusing `unified_field_library` mappings and
  matching the ORM builder semantics exactly. Writes/extends unit tests.
- **verify(family)** — verifier agent runs the family-restricted equivalence diff on
  the golden fixtures and returns `{passed, diff_lines, map_elements_found}`. Fails if
  the diff is non-empty OR any `map_elements`/`apply` appears in the family module.

After all families pass, an **integrate** phase runs the full-slice equivalence
(all tables) + the throughput benchmark, and reports a go/no-go for P5 (flip default).

Loop-until-green: a family whose verifier fails is re-implemented (bounded retries)
with the diff lines fed back to the implementer.

## Agent prompts (essence)
- Implementer: "Vectorize ingest for {types}. Replace the ORM builder logic for these
  record types with Polars column expressions in `ingest_vectorized/<family>.py`.
  Match `app/core/builders.py` + `processor.py` semantics for these types exactly.
  Hard rule: no `map_elements`/`apply`. Make `diff_snapshots` empty for {tables} on the
  golden fixtures. Return the files changed + how you verified."
- Verifier: "Run the equivalence harness on the golden fixtures restricted to {tables}:
  load via ORM and via `run_vectorized`, `diff_snapshots`, and grep the family module
  for `map_elements`/`apply`. Return {passed, diff_lines[], map_elements_found}. Do not
  trust the implementer's report — run it yourself."
- Integrator: "Run full-slice equivalence (all tables) + benchmark vectorized vs ORM
  throughput on a larger sample. Return {all_equal, throughput_x, blockers[]}."

## Cost / scale
~6–10 agents per pass (4 families × implement+verify, + retries, + integrate). This is
real implementation work — only launch with explicit opt-in. Expect multiple rounds as
edge cases surface; the harness keeps every round honest.
