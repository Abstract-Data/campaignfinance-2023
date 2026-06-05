# 0004 — ELT (dbt-on-Postgres) Unify Spike

- Status: Proposed (proof-of-concept complete)
- Date: 2026-06-05
- Deciders: (pending review)
- Scope: Texas contributions (RCPT) + expenditures (EXPN) only. `app/resolve/` and the
  per-state validators untouched; `scripts/loaders/production_loader.py` untouched.

## Context

The imperative unify/load layer (`app/core/processor.py`, `app/core/builders.py`,
`app/core/load_cache.py::BuilderCache`) builds canonical rows one at a time with
Python-side dedup and per-row DB lookups. The seven bugs in `PIPELINE_REVIEW.md`
(role-blind person extraction, backwards contribution entity assignment, NULL-address
dedup explosion, missing state scoping, no DB-level uniqueness) are all symptoms of
that row-based pattern — patched, but the pattern keeps inviting them.

This spike (`prompts/elt-unify-refactor/current.md`) tests whether a **set-based,
SQL-first ELT layer using dbt on Postgres** makes those bug classes *unexpressible*,
moves dedup ownership to the database, and turns every unification rule into a tested,
declarative model — built **beside** the existing loader so the two can be reconciled
and benchmarked. It is a decision-informing spike, not a migration.

## What was built (`transform/`)

- **Silver EL** — `transform/silver_load.py` discovers TX bronze parquet, validates each
  row with the **existing** `TECContribution` / `TECExpense` Pydantic validators (no
  validation logic rewritten), and lands clean rows into a `silver` schema. Dirty rows
  are rejected and counted. The validators expect CSV semantics (empties as `""`); the
  loader maps parquet nulls to `""` accordingly and lower-cases columns so dbt can
  reference TEC fields unquoted.
- **Gold** — a `dbt-postgres` project (`transform/dbt/`): staging → `int_role_people`
  (the role union) + `int_committees` → `dim_*` + `*_keys` surrogate registries →
  `unified_*` models. Dedup grains, `normalize_entity_name`, and the role mapping are
  reproduced in SQL.
- **Publish** — `publish_to_unified` macro loads `gold.*` into the ORM-owned
  `public.unified_*` tables (uuid/timestamps/enums supplied), the single seam to the
  world `app/resolve` reads.
- **Dedicated spike DB** — everything targets a separate `campaignfinance_elt_spike`
  database so the real `campaign_finance` data is never touched. (This differs from the
  plan's "docker-compose db" only in *which* local Postgres; both pipelines still write
  the same physical `public.unified_*` tables, keeping reconciliation honest.)

## The role-mapping bug is now unexpressible (Fix 1, locked)

Each staging model names **only its own role's** source columns: `stg_tx_contributions`
reads `contributor*` and emits `role = 'CONTRIBUTOR'`; `stg_tx_expenditures` reads
`payee*` and emits `role = 'PAYEE'`. `int_role_people` is their UNION, so a contribution
row *cannot* produce a PAYEE/CANDIDATE and an expenditure row *cannot* produce a
CONTRIBUTOR. The committee (filerIdent) is captured only on
`unified_transactions.committee_id`, never as a `transaction_persons` row.

Locked by:
- dbt **unit tests** `expenditure_row_yields_only_payee`, `contribution_row_yields_only_contributor`.
- singular tests `assert_no_cross_role_rows`, `assert_one_person_per_transaction`.

`dbt build` is green: **PASS=78, 0 errors** (21 models, 55 generic tests, 2 unit tests,
2 singular tests). On a 5,000-row-per-file sample the Gold tables are exactly:

| table | rows |
|---|---|
| unified_transactions | 10,000 |
| unified_transaction_persons | 10,000 (**exactly one per transaction**) |
| unified_contributions | 5,000 |
| unified_expenditures | 5,000 |
| unified_persons | 5,045 |
| unified_entities | 5,041 |
| unified_addresses | 4,068 |
| unified_committees | 7 |

`transaction_persons == transactions` with max one person per transaction — **zero
phantom rows**, which the row-based loader could only achieve after the Fix-1 patch.

## Reconciliation vs production_loader

Both paths write the same physical `public.unified_*` tables; we compare on **natural
keys** (surrogate ints differ by construction). `transform/reconcile.py` runs the dbt
path, then the loader (FILER committees + the same capped RCPT/EXPN), and diffs.

Sample: first 5,000 rows of `contribs_01` + first 5,000 of `expend_01`; loader also
loads the full FILER committee master so its transactions can link.

Numbers below are **after** the post-spike hardening (filer-id padding + FILER master,
see "Fixes applied"):

| table | dbt ELT | loader | delta |
|---|--:|--:|--:|
| unified_transactions | 10,000 | 10,000 | **0** |
| unified_transaction_persons | 10,000 | 10,000 | **0** |
| unified_committees | 20,085 | 20,085 | **0** |
| unified_contributions | 5,000 | 4,999 | +1 |
| unified_expenditures | 5,000 | 5,000 | 0 |
| unified_entities | 24,902 | 21,546 | +3,356 |
| unified_persons | 5,045 | 21,567 | −16,522 |
| unified_addresses | 4,068 | 17,635 | −13,567 |

Natural-key sets: **committees `shared=20,085 / dbt-only=0 / loader-only=0`** (exact
match after the fix); persons `shared=4,851 / dbt-only=194 / loader-only=16,716`.
Phantom invariant: both max **1** person/transaction.

**Explained differences (all expected, none regressions):**
- **Committees now match exactly** — folding the FILER master into Silver + zero-padding
  the filer id (fixes #3/#7) brought committees to `20,085 = 20,085, shared=20,085`.
- **persons / addresses (loader higher)** — the loader extracts committee **officers**
  from the FILER file as persons/addresses/entities; the dbt PoC models committees but
  not officers (officer extraction is a follow-up). This is the bulk of the −16,522
  persons and the 16,716 loader-only person keys.
- **entities (dbt higher, +3,356)** — the dbt path eagerly materializes one COMMITTEE
  entity per FILER committee; the loader materializes committee/officer entities more
  lazily. Different derivations, each internally consistent (the unique index rebuild on
  publish confirms no dedup violation).
- **persons `dbt-only=194`** — the EXPN validator parses payee names via
  `person_name_parser`, while the loader uses the raw payee fields (the builder does
  **not** re-parse). A migration should pick one name-handling path for both.
- **contributions +1** — the loader rejected one RCPT row whose committee was absent
  from FILER; the dbt path (which also derives committees from the rows) kept it.
- **transaction_persons cardinality** — dbt produces exactly one row per transaction by
  construction; the post-Fix-1 loader matches (max 1).

## Benchmark (acceptance #4)

Same sample, same machine, wall-clock end-to-end:

| path | wall-clock |
|---|--:|
| **dbt ELT** (Silver validate+load → `dbt build` → publish) | **~19.5 s** |
| production_loader (FILER master + capped RCPT/EXPN, row-by-row ORM) | ~308 s |

~16× faster. Not purely like-for-like — the loader's time is dominated by the row-by-row
FILER load (~20k committees + officers through the ORM) — but that asymmetry *is* the
finding: the ELT path issues set-based `CREATE TABLE AS` + bulk inserts and lets the DB
own dedup, while the loader pays a per-row lookup-or-insert against the Python
`BuilderCache`. The publish (#2: drop indexes → bulk insert → rebuild indexes) keeps the
DB-load step cheap even though committees jumped 7 → 20,085.

At full volume the **Silver Python validation is the bottleneck**, measured at
~1,800 rows/s/core → ~6 h single-threaded for 40.3M rows. Fanning validation across
cores (8 workers, `--all-files`) brings that to ~1 h; the dbt transform + publish over
40M rows is minutes. The EL, not the transform, is what a production migration must
parallelize.

## Resolve consumability (acceptance #5)

`app/resolve/standardize/stage1.build_resolution_input()` runs **unchanged** against the
published `public.unified_*` and produced **50,032 resolution_input rows** from the dbt
Gold output (5,045 persons + 20,085 committees + ~24,902 entities standardized). The only
adapter required is the dbt→public publish step (because resolve binds SQLModel classes
to `public.unified_*` with native PG enum types and a string `committee_id` FK — it
cannot read a bare `gold` schema). No `app/resolve` change.

## Fixes applied (post-spike hardening)

Run at multi-file volume, the spike surfaced and resolved several issues:

1. **Org-vs-individual person key** — `person_nk` keyed on org+first+last, but the DB's
   `uix_persons_org_state` keys on org alone; a dirty ENTITY row carrying both an org
   name and a stray first/last split into two rows that collided at publish. Fixed by
   branching the key on `organization IS NULL`, matching the partial indexes exactly.
2. **Memory + throughput (#1)** — the loader now streams per-file and validates across
   CPU cores (`--all-files`, N workers); Silver lands as uniform TEXT so parallel
   appends don't race on inferred types. (The 40M-row run is ~1 h, EL-bound.)
3. **Publish performance (#2)** — `publish_to_unified` drops the dedup indexes, bulk-
   inserts, then rebuilds them (the rebuild also re-validates the dedup invariants);
   bulk-load into the indexed ORM tables is no longer per-row index maintenance.
4. **Filer-id fidelity (#3) + FILER master (#7)** — Silver re-pads the validator's int
   `filerIdent` to the canonical 8-char id and folds in the FILER committee master
   (name/type/status), so committees match the loader exactly. `committee_type` (a FK to
   `committee_types`) is seeded in publish.
5. **Name-handling (#4)** — corrected understanding: the loader does **not** parse names;
   the EXPN validator does (payee). The ~194-key divergence is the validator-vs-raw
   split; a migration should standardize one path. No code change in the PoC.
6. **Rejects (#5)** — the validator None-join is worked around (parquet nulls → `""`,
   CSV-faithful); residual EXPN rejects (~1%) are legitimate missing-required-field
   rows — the intended "validate at the boundary" behavior, not a bug.
7. **Registry statefulness (#6)** — the `*_keys` ids are append-stable; a production run
   must **not** `--full-refresh` a registry once resolve has consumed Gold (it renumbers
   ids). Decision: persist the registries; full-refresh only on a coordinated reload.

## What got simpler

- **Role mapping** — a literal per-staging-model column list replaces the role-blind
  `build_person` loop; the phantom-role bug is structurally impossible.
- **Dedup** — owned by the database. Grains are declared once (`generate_surrogate_key`
  over the same tuples as `_DEDUP_INDEXES`) and verified by `unique` tests, instead of
  a Python cache shadowing partial indexes.
- **NULL-address dedup (Fix 5)** — a `has_street` branch in the address nk handles the
  street-less contribution rows set-based; no per-row `NULL = NULL` trap.
- **State scoping (Fix 4)** — `state_id` is in every nk by construction.
- **Testability** — unit tests assert transformation logic on mock rows with no DB.

## What got harder

- **Integer surrogate keys** — resolve's ORM needs int PK/FKs, but set-based dedup wants
  hash keys. The `*_keys` registries (incremental `delete+insert`) bridge this at the
  cost of statefulness and a strict build order; `--full-refresh` on a registry
  renumbers ids and invalidates any persisted resolve output.
- **Entity↔person 1:1** — keeping `unified_entities.person_id` unique required deriving
  each person's entity from a single representative `dim_persons` row (suffix/middle
  vary across occurrences). A subtlety the row-based builder side-stepped by creating the
  entity only on a person's first occurrence.
- **The publish seam** — enum casts must use the exact UPPERCASE member names
  (`'CONTRIBUTOR'::personrole`), and uuid/created_at/updated_at/amended must be supplied
  because SQLModel sets them Python-side. A real migration would generate these in-model.
- **Procedural logic resists SQL** — travel parent-transaction linkage, candidate
  backfill into campaigns, and survivorship-style precedence don't translate cleanly;
  they would stay thin Python or carefully-scoped SQL. (Out of PoC scope.)

## Decision — go / no-go

**Recommendation: GO**, staged, for the Tier-1 record types and beyond (see
`prompts/elt-unify-refactor/current.md` Part B.5 / the plan's migration surface).

Rationale: the spike proves the bug classes become unexpressible, dedup is DB-owned and
test-locked, the output is a drop-in source for `app/resolve`, and per-type cost is low
(each new Tier-1 type is one more union branch + one detail model). The hard parts
(surrogate-key registry, publish seam) are solved once and reused. Gate the rollout on:
(1) folding the FILER committee master into Silver, (2) generating uuid/timestamps
in-model so publish is a thin insert, (3) deciding registry persistence vs full-refresh
policy before any resolve output is persisted from Gold.

Tier-2 (`ASSET`, loan/debt guarantor 1–5 explosion) and Tier-4 (multi-state, where the
state-scoped dedup payoff multiplies) are the highest-value follow-ups.
