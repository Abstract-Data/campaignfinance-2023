# Prompt: Re-architect the Unify/Load Slice as ELT (dbt-on-Postgres) — Proof-of-Concept First
# Version: 1.0.0
# Model: claude-sonnet-4-6
# Last Updated: 2026-06-06
# Maintainer: John Eakin / Abstract Data

> Hand this to an implementation agent (or use it yourself). It is scoped as a
> **strangler-fig proof of concept**, not a big-bang rewrite. Do **not** touch
> `app/resolve/` or the per-state validators. Build the new path beside the old
> one, benchmark it, and only then decide whether to migrate the rest.

---

## Role & context

You are working in the `campaignfinance` repo: a multi-state campaign-finance
data pipeline (Texas is the only state that currently loads end to end). Stack:
Python 3.12, uv, SQLModel/Pydantic v2, Polars, Splink, PostgreSQL (prod) /
SQLite (dev).

Today the **unify/load** layer (`app/core/processor.py`, the builders, and
`app/core/load_cache.py::BuilderCache`) constructs canonical rows **imperatively,
one row at a time**, with Python-side dedup and per-row DB lookups. The project's
own `PIPELINE_REVIEW.md` documents seven bugs that are all symptoms of this
row-based pattern (role-blind person extraction, FK-flush ordering, dedup that
lived only in Python until partial-unique indexes were backfilled). They are
patched but the pattern still invites them.

The goal of this work is to prove out a **set-based, SQL-first (ELT) unify layer**
using **dbt on Postgres**, in which those bug classes become impossible to
express, dedup is owned by the database, and every unification rule is a tested,
declarative model.

## What stays exactly as-is (hard constraints)

- **Do not modify `app/resolve/`.** Entity resolution (Splink, blocking,
  survivorship, crosswalks, review queue) is well-designed and out of scope. The
  new gold tables must remain a drop-in source for it — same table/column shape
  that `app/resolve/standardize/staging.py::ResolutionInput` reads today.
- **Keep Pydantic/SQLModel validation at the edge (Silver).** Per-row validation
  and clean rejects of dirty government CSVs stay in Python. Do **not** port
  validation into SQL. The rule is *validate at the boundary, transform in SQL*.
- **Keep the existing `production_loader` path working and untouched** so the two
  approaches can be benchmarked side by side and the old path remains the
  fallback.

## Architecture target (medallion)

- **Bronze** — raw state files landed as-is. Already done: Texas converts to
  all-string parquet. No change.
- **Silver** — validate + type + normalize per state using the existing Pydantic
  validators, then land the clean, typed rows into Postgres tables (one per
  state record type, e.g. `silver_tx_contributions`, `silver_tx_expenditures`,
  `silver_tx_filers`). Loading is plain `COPY` / Polars `write_database` — this
  is the "EL"; dbt does **not** do extract/load.
- **Gold** — `dbt` models that transform Silver into the unified schema
  (`unified_persons`, `unified_entities`, `unified_addresses`, `unified_committees`,
  `unified_transactions`, `unified_transaction_persons`, `unified_campaigns`) with
  **dedup owned by the database**.

## Proof-of-concept scope (do only this first)

Pick the two record types that exercise the role-mapping bug most directly:

1. **Texas contributions (RCPT)** → unified contribution + CONTRIBUTOR person +
   committee (RECIPIENT) link.
2. **Texas expenditures (EXPN)** → unified expenditure + PAYEE person + committee
   link.

Deliver the full Bronze→Silver→Gold path for **just these two**, enough to load a
representative Texas sample and compare output to the current loader. Do not
attempt loans, pledges, assets, travel, or the other states yet.

## dbt specifics (verified against current dbt docs — use these, don't improvise)

Adapter: **`dbt-postgres`**. Run Postgres in dev too (Testcontainers or a local
container) — do **not** target SQLite. SQLite/Postgres parity has already caused
schema-fragility bugs here, and partial indexes + raw DDL behave differently.

**Sources** — declare the Silver tables as dbt sources so Gold models `ref`/
`source` them and you get lineage:

```yaml
sources:
  - name: silver
    schema: silver
    tables:
      - name: tx_contributions
      - name: tx_expenditures
      - name: tx_filers
```

**Dedup via incremental models.** `dbt-postgres` supports these
`incremental_strategy` values: `append` (default when no `unique_key`), `merge`,
`delete+insert` (the default when `unique_key` IS set), and `microbatch`.

- Use **`delete+insert`** as the default strategy (works on all supported
  Postgres versions). Use `merge` only if the target is Postgres 15+.
- Set `unique_key` to the SAME key tuples enforced by the existing partial-unique
  indexes in `app/core/unified_database.py::_DEDUP_INDEXES`, so a dbt dedup and a
  DB row stay consistent:
  - persons (non-org): `(lower(first_name), lower(last_name), state_id)`
  - persons (org): `(lower(organization), state_id)`
  - addresses (full): `(lower(street_1), lower(city), lower(state), zip_code)`
  - entities: `(entity_type, normalized_name, state_id)`

Person dedup model sketch (replaces `BuilderCache` for persons):

```sql
{{ config(materialized='incremental', incremental_strategy='delete+insert',
          unique_key=['first_name_lc','last_name_lc','state_id']) }}

with contributor_people as (
    -- the external party on a contribution row is the CONTRIBUTOR, named explicitly
    select
        lower(contributor_name_first) as first_name_lc,
        lower(contributor_name_last)  as last_name_lc,
        {{ var('state_id') }}         as state_id,
        contributor_name_first, contributor_name_last
    from {{ source('silver','tx_contributions') }}
    where contributor_name_last is not null
),
payee_people as (
    -- the external party on an expenditure row is the PAYEE, named explicitly
    select
        lower(payee_name_first) as first_name_lc,
        lower(payee_name_last)  as last_name_lc,
        {{ var('state_id') }}   as state_id,
        payee_name_first, payee_name_last
    from {{ source('silver','tx_expenditures') }}
    where payee_name_last is not null
)
select distinct * from (
    select * from contributor_people
    union all
    select * from payee_people
) all_people
{% if is_incremental() %}
where (first_name_lc, last_name_lc, state_id) not in (select first_name_lc, last_name_lc, state_id from {{ this }})
{% endif %}
```

The point: each role's source columns are named in the SQL. There is no
"first field that matches" loop, so a contribution row can never emit a PAYEE/
CANDIDATE person and an expenditure row can never emit a phantom CONTRIBUTOR.

**Tests — implement all three kinds:**

1. **Generic data tests** in schema YAML — `unique`, `not_null`, `relationships`
   (FK integrity: every `unified_transactions.contributor_entity_id` resolves to
   an `unified_entities.id`), `accepted_values` (roles, transaction types).
2. **Unit tests** (dbt 1.8+) — assert the role-mapping logic with mock rows. This
   is the regression lock for `PIPELINE_REVIEW` Fix 1. Example:

```yaml
unit_tests:
  - name: expenditure_row_yields_only_payee
    description: An EXPN row produces exactly one PAYEE person and zero CONTRIBUTOR rows.
    model: unified_transaction_persons
    given:
      - input: source('silver','tx_expenditures')
        rows:
          - {expend_info_id: 'E1', payee_name_first: 'Acme', payee_name_last: 'Signs', filer_ident: 'C1'}
      - input: source('silver','tx_contributions')
        rows: []
    expect:
      rows:
        - {transaction_id: 'E1', role: 'PAYEE',        person_last: 'Signs'}
        # NO CONTRIBUTOR / CANDIDATE rows expected
```

3. A **reconciliation test** comparing Gold output against the current
   `production_loader` on the same Texas sample (see acceptance criteria).

**Campaigns** stay deterministic and structural — mirror
`app/resolve/publish/campaigns.py`: identity is `(committee_entity, office,
election_cycle)`, deduped by that tuple. If you model campaigns at all in the
PoC, do it as a plain dbt model keyed on that tuple, not probabilistic matching.

## Acceptance criteria

1. `dbt build` runs green (models + generic tests + unit tests) against a
   dockerized Postgres loaded with a Texas sample.
2. **Reconciliation**: for the sample, the dbt Gold tables and the current
   `production_loader` output agree on row counts and key dedup invariants for
   persons, committees, and transaction-person links — OR every difference is
   explained and is a *fix* (e.g. fewer phantom rows), documented in the writeup.
3. **No phantom-role rows**: a contribution sample produces zero PAYEE/CANDIDATE
   person rows; an expenditure sample produces zero CONTRIBUTOR rows. Locked by
   the unit test above.
4. **Benchmark**: wall-clock + DB round-trip count for the dbt path vs the
   current loader on the same sample, reported in a short table.
5. The Gold tables are consumable by `app/resolve` unchanged (confirm
   `ResolutionInput` still reads them, or document the exact adapter needed).

## Deliverables

- A `dbt/` project (or `transform/dbt/`) with `sources.yml`, the Silver→Gold
  models for the two record types, schema tests, and unit tests.
- A small Python shim that loads the Pydantic-validated Silver rows into Postgres
  (`COPY` / `write_database`), reusing existing validators — no validation logic
  rewritten.
- `docs/adr/NNNN-elt-unify-spike.md`: the benchmark numbers, the reconciliation
  result, what got simpler, what got harder (procedural survivorship-style logic
  that resists SQL), and a clear **go / no-go recommendation** on migrating the
  remaining record types and states.

## Process notes

- Follow the repo's GitNexus guardrails: run `gitnexus_impact` before editing any
  existing symbol, and `gitnexus_detect_changes()` before committing. The new dbt
  project is additive; the only existing code you should touch is a thin Silver
  loader, and the old loader must keep working.
- Do not delete or rewrite `production_loader.py`. This is a spike to inform a
  decision, not a migration.
