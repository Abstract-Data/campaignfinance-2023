# Data Resolution Pipeline — Design Spec

- **Status:** Draft for review
- **Date:** 2026-05-23
- **Topic:** Deduplication, entity resolution, and relational linking of campaign finance data
- **Applies to:** `campaignfinance` (Abstract-Data/campaignfinance-2023)

## Summary

This spec defines a pipeline that cleans the campaign finance data, resolves
duplicate records, and links every record into a coherent relational graph.

The core idea: the existing `unified_*` tables become an immutable **source
layer** (data as ingested, never mutated). On top of it we add a **resolution
layer** — canonical entity tables, a crosswalk that maps every source record to
its resolved entity, dated name history, a human review queue, and a full match
audit trail. A re-runnable, seven-stage pipeline performs deterministic and
probabilistic (Splink) matching to populate that layer.

A prerequisite phase (**Phase 0**) closes gaps in the source layer itself:
several Texas record types present in `tmp/texas` have no model and no link to
the reports they belong to.

## Problem statement

The repository already intends entity resolution — there is a `UnifiedEntity`
"deduplication layer", a `normalized_name` column, and version tables — but the
implementation has the following defects:

1. **Deduplication is exact-match only.** `_normalize_name()` lowercases and
   strips punctuation; "John Smith", "Jon Smith", and "Smith, John" all produce
   different keys. Addresses dedupe on exact `street_1/city/state/zip`, so "123
   Main St" and "123 Main Street" become separate rows. No value-level fuzzy
   matching exists anywhere.
2. **Three competing dedup mechanisms.** Inline per-record `SELECT`s during
   record-building (`build_committee`, `_find_entity`, `_find_address_by_fields`),
   in-memory caches in `production_loader.py`, and post-hoc SQL sweeps
   (`_dedupe_addresses`, `_dedupe_persons_and_entities`) each use slightly
   different key definitions and disagree.
3. **No survivorship logic.** Merging two persons keeps `MIN(id)` and repoints
   foreign keys; it does not merge attributes into a best "golden record".
4. **Campaign identity is fragile.** Campaigns key on `normalized_name +
   committee + candidate + election_year`, where `election_year` is derived from
   the transaction date — so a campaign with transactions spanning a year
   boundary fragments into multiple campaign rows.
5. **No match audit.** Merges record no reason, rule, score, or confidence.
6. **`_find_address_by_fields` bug.** It rebuilds `UnifiedAddress(**dict(result))`
   as a detached object from a raw SQL row, so the "found" address frequently
   re-inserts as a duplicate.
7. **Address is mistakenly part of identity.** `_person_key` requires an equal
   `address_id` to match two persons — this both over-fragments a donor who
   moved and conflates a shared address with shared identity.
8. **Source-layer coverage gaps.** Several Texas record types in `tmp/texas`
   have no model, and transactions are not linked to the reports (cover sheets)
   they belong to (see Appendix A).

## Goals

- One canonical record per real-world person, organization, committee, campaign,
  and address — with every source record linked to it.
- Fuzzy matching that survives name variants, typos, and address inconsistency.
- Name changes preserved as dated history under one stable canonical ID.
- High-confidence matches merged automatically; ambiguous matches routed to a
  human review queue.
- Every merge explainable (rule or score) and reversible.
- Non-destructive: the source layer is never mutated; the pipeline is
  re-runnable and idempotent.
- All `tmp/texas` data loaded, modeled, and linked to its report.

## Non-goals

- Cross-state entity linking is **designed for but not built** in this effort.
  Resolution runs one state at a time; a `master_entity_id` self-reference is
  reserved on `canonical_entity` for a later additive phase.
- No changes to the existing ingestion of already-modeled record types beyond
  the Phase 0 additions.
- No geocoding to lat/long. Address resolution standardizes and dedupes
  textual addresses only.
- No public-facing UI. The review queue is worked through a CLI (a UI may
  follow later).

## Architecture overview

Three layers:

1. **Source layer (immutable).** The existing `unified_*` tables plus the Phase
   0 additions. Written only by ingestion; never mutated by resolution.
2. **Resolution layer (new).** `match_run`, the crosswalk tables, `match_decision`,
   and `merge_review`. Records every run and every pairwise decision.
3. **Canonical layer (new).** `canonical_entity`, `canonical_campaign`,
   `canonical_address`, and `canonical_name_history` — one row per real-world
   thing, plus its dated history.

Data flows source → resolution → canonical. The source layer is the record of
truth as filed; the canonical layer is the cleaned, deduplicated graph; the
crosswalk connects them. Because the source layer is never mutated, the whole
resolution can be re-run with an improved model by rebuilding the resolution and
canonical layers, and any merge can be undone by deleting a run's crosswalk and
decision rows.

New code lives in a new `app/resolve/` package, driven by a `resolve` CLI in the
spirit of the existing `scripts/loaders/`.

## Schema design

### Source layer — existing tables (frozen)

`unified_transactions`, `unified_contributions`, `unified_loans`, `unified_debts`,
`unified_credits`, `unified_travel`, `unified_assets`, `unified_committees`,
`unified_persons`, `unified_addresses`, `unified_entities`, `unified_campaigns`,
and their junction tables remain as-is. The existing `*Version` tables continue
to version the source layer; they are not used by the canonical layer.

### Source layer — Phase 0 additions

| Table | Source record | Purpose |
|-------|---------------|---------|
| `unified_reports` | CVR1 (`cover`, `cover_ss`, `cover_t`) | One row per filed report: filer, reporting period, filed date, form type, declared totals. |
| `unified_pledges` | PLDG detail | Detail table for pledge transactions, mirroring `unified_contributions`. |
| `expenditure_categories` | EXCAT (`expn_catg`) | Lookup table of expenditure category codes. |
| `committee_purposes` | CVR3 (`purpose`) | A committee's stated purpose, per report. |
| `spac_links` | SPAC (`spacs`) | Links a specific-purpose committee to the candidate/measure it supports or opposes. |
| `unified_notices` | CVR2 (`notices`) | Notices received by candidates/officeholders. |

`unified_reports` key columns: `id` (PK), `uuid`, `state_id` (FK), `committee_id`
(a **string** FK → `unified_committees.filer_id`, matching the existing
`unified_transactions.committee_id` convention — the column is named `committee_id`
but holds the string `filer_id`), `report_ident` (the state `reportInfoIdent`),
`form_type`, `period_start`, `period_end`, `filed_date`, `is_final` (FINL records
flag a report final), and declared totals (`total_contributions`,
`total_unitemized_contributions`, `total_expenditures`,
`total_unitemized_expenditures`, `loan_balance`, `contributions_maintained`,
`cash_on_hand`), `file_origin_id` (FK), `raw_data`.

`unified_transactions` gains a `report_id` column (FK → `unified_reports.id`),
populated from the source `reportInfoIdent` carried on every TEC transaction
record. This is the link that ties transactions to their filing.

FINL (`finals`) is represented as the `is_final` flag on `unified_reports` rather
than a separate table; a `TECFinalReport` validator already exists state-side.

### Canonical layer

**`canonical_entity`** — one row per resolved person, organization, or committee.

- `id` (PK), `uuid`
- `entity_type` (enum: `person`, `organization`, `committee`)
- `canonical_name`, `normalized_name`
- `canonical_address_id` (FK → `canonical_address`, nullable; many entities → one address)
- `state_code`
- `master_entity_id` (FK → `canonical_entity`, nullable; reserved for future cross-state linking, unused now)
- `first_seen_date`, `last_seen_date`, `source_record_count`
- `last_run_id` (FK → `match_run`), `created_at`, `updated_at`

`entity_type` parameterizes the matching logic (different comparison columns and
weights for a person vs. a business) but all three types share one table so that
contributions/expenditures point at a single FK target. A "vendor" is an
`organization` observed in a payee role, not a separate type.

**`canonical_campaign`** — one row per campaign.

- `id` (PK), `uuid`
- `committee_entity_id` (FK → `canonical_entity` where `entity_type = committee`) — identity anchor
- `office_normalized`, `district`
- `election_cycle` (int) — derived from the **report period**, never from individual transaction dates
- `candidate_entity_id` (FK → `canonical_entity`, nullable)
- `canonical_name`, `state_code`
- `last_run_id`, `created_at`, `updated_at`

Campaign identity is the tuple `(committee_entity_id, office_normalized,
election_cycle)`. Name variants are recorded in `canonical_name_history`.

**`canonical_address`** — one row per resolved physical location.

- `id` (PK), `uuid`
- `standardized_line_1`, `standardized_line_2` (unit/suite preserved)
- `city`, `state`, `zip5`, `zip4`
- `parse_status` (from `usaddress`: `parsed`, `partial`, `unparsed`)
- `frequency` (a **derived** count of distinct entities at this address — a display/query attribute only, e.g. for the `address_occupancy` view. Splink computes its own term-frequency adjustment internally during scoring and does not read this column.)
- `last_run_id`, `created_at`, `updated_at`

**`canonical_name_history`** — every name a canonical entity or campaign has filed under.

- `id` (PK)
- `subject_type` (enum: `entity`, `campaign`), `subject_id`
- `name`, `normalized_name`
- `first_seen_date`, `last_seen_date`, `occurrence_count`
- `source` (file origin / report reference)

### Resolution layer

**`entity_crosswalk`** — maps each source record to its canonical entity.

- `id` (PK)
- `source_type` (enum: `unified_person`, `unified_committee`, `unified_entity`)
- `source_id` (string; accommodates the string `filer_id` PK of committees)
- `canonical_entity_id` (FK → `canonical_entity`)
- `match_method` (enum: `exact`, `deterministic_rule`, `probabilistic`, `manual`)
- `match_score` (float, nullable for non-probabilistic methods)
- `confidence_band` (enum: `auto`, `review`, `manual`)
- `run_id` (FK → `match_run`), `decided_at`, `decided_by`

`address_crosswalk` and `campaign_crosswalk` follow the same shape, mapping
source addresses and source campaign records to `canonical_address` /
`canonical_campaign` respectively.

**`match_run`** — one row per pipeline execution.

- `id` (PK), `state_code`, `pass_type` (enum: `entity`, `address`, `campaign`)
- `engine_version`, `config_json` (snapshot of thresholds, blocking rules, comparison config)
- `started_at`, `finished_at`, `status` (enum: `running`, `completed`, `failed`)
- counts: `records_in`, `pairs_compared`, `auto_merges`, `queued`, `rejected`, `canonical_out`

**`match_decision`** — every pairwise decision in a run.

- `id` (PK), `run_id` (FK)
- `source_a_type`, `source_a_id`, `source_b_type`, `source_b_id`
- `score`, `method`, `band` (`auto`/`review`/`reject`), `outcome` (`merged`/`queued`/`rejected`)
- `explanation_json` — the Splink per-comparison contribution breakdown (the audit trail)

**`merge_review`** — the human review queue.

- `id` (PK), `run_id` (FK that surfaced the pair)
- `source_a_type`, `source_a_id`, `source_b_type`, `source_b_id`
- `score`, `explanation_json`
- `status` (enum: `pending`, `approved`, `rejected`), `reviewer`, `decided_at`, `notes`

Approved/rejected `merge_review` rows are durable: a rejected pair is never
re-queued, and an approved pair is treated as a confirmed merge edge on
subsequent runs.

### Address-as-shared-hub model

Address resolution and entity resolution are **separate passes**.
`canonical_address` dedupes locations; `canonical_entity` dedupes people and
organizations.

A `canonical_address` is a many-to-one target: many `canonical_entity` rows may
share one `canonical_address_id`, which is normal (households, office buildings,
registered-agent addresses). Sharing an address is therefore a **match feature**,
never an identity. Two records merge only with name evidence; address alone never
merges entities.

Splink's term-frequency adjustment is required for the address comparison: an
address shared by few records is strong evidence, while an address shared by
many records (a registered agent on thousands of LLC filings) contributes almost
nothing. Blocking must never use address alone, and high-frequency address
values are capped in blocking to prevent oversized candidate blocks.

A published `address_occupancy` view (`canonical_address` × `canonical_entity` ×
role × transaction counts) answers "who is active at this address" as a join. If
a household or shared-office relationship needs to be asserted explicitly, it is
recorded as a `co_located_with` association edge using the existing
`UnifiedEntityAssociation` pattern — entities stay linked but distinct, never
merged.

## The resolution pipeline

A new `app/resolve/` package and a `resolve` CLI run the pipeline one state at a
time. There are seven idempotent stages; each reads the previous stage's output
from a staging table tagged with the `run_id`, so any stage can be re-run alone.
Canonical tables are written to staging tables and atomically swapped into place
only on successful completion of the run.

1. **Standardize / feature-prep.** For every source record, compute matching
   features into a `resolution_input` staging table: parsed name parts
   (`probablepeople` / `nameparser`, which also classifies person vs. business),
   standardized addresses (`usaddress` + `usaddress-scourgify`, already
   dependencies), phonetic codes (metaphone), and normalized organization names
   (strip `LLC`/`INC`/`CO`/`CORP`, `&`/`and`, punctuation). Implemented with
   Polars.
2. **Block.** Generate candidate pairs only within blocks (e.g. same phonetic
   last name + ZIP3; same organization-name prefix). Blocking rules are config,
   snapshotted into `match_run`.
3. **Deterministic fast-path.** Auto-resolve certainties before scoring:
   identical committee `filer_id`; identical standardized name + standardized
   address. These write to the crosswalk with `match_method = exact`.
4. **Probabilistic score (Splink).** Score remaining candidate pairs per entity
   type with Splink's Fellegi-Sunter model; output a calibrated 0–1 probability
   and a per-comparison contribution breakdown per pair. Model weights are
   estimated from the data (EM); minimal manual training.
5. **Classify into bands.** Apply per-entity-type thresholds: `auto` (≥ 0.99),
   `review` (0.80–0.99), `reject` (< 0.80). Starting values; tuned against the
   labeled golden set; stored in `config_json`.
6. **Cluster.** Run connected-components over all `auto` edges plus previously
   `approved` review edges. Each component is one canonical entity — so A↔B and
   B↔C strong matches collapse A, B, C together even if A↔C was never compared.
   A mega-cluster guard holds back any cluster exceeding a configurable size cap
   and routes it to review instead of auto-publishing.
7. **Survivorship / publish.** For each cluster, build or refresh the
   `canonical_entity` (or campaign/address) row via survivorship rules, populate
   `canonical_name_history`, and write the crosswalk rows.

Medium-band pairs from stage 5 are loaded into `merge_review`. A reviewer works
the queue via the CLI; approvals and rejections are durable and feed stage 6 on
the next run.

### Survivorship rules

The canonical record's displayed attributes are computed from all linked source
records, recomputed each run:

- **Name:** most complete (most non-empty parts), ties broken by most recent.
- **Address:** most recent fully-parsed address.
- **Dates:** `first_seen_date` = min, `last_seen_date` = max across linked records.
- **Provenance:** each populated canonical field records the source record it
  came from.

### Campaign cycle handling

`election_cycle` is taken from the report's `period_end` (via the new
`unified_reports`), not from individual transaction dates. This prevents a
campaign whose transactions span a calendar boundary from fragmenting.

## Matching engine

The engine is **Splink** (probabilistic record linkage, MIT-licensed, runs
locally) wrapped by a deterministic fast-path and rule layer:

- Stages 1–3 are deterministic and remove the bulk of duplicates cheaply.
- Stage 4 is Splink, on its DuckDB backend.
- The comparison configuration per entity type defines which standardized fields
  are compared and at what similarity levels; address comparisons use Splink's
  term-frequency adjustment.

Splink is chosen because its calibrated probabilities map directly onto the
auto/review/reject bands, and its per-comparison breakdown satisfies the audit
requirement. The engine is swappable: all stages write to the same staging and
crosswalk schema, so an alternative scorer could replace stage 4 without schema
change.

## Error handling and resilience

- Every stage is transactional and idempotent. Each run gets a fresh `run_id`.
  Canonical tables are built in staging tables and atomically swapped only on
  success, so a failed run leaves the last-good canonical data intact.
- The pipeline is deterministic: identical input + model + config produce a
  byte-identical crosswalk. Random seeds are fixed; `config_json` is snapshotted.
- Records that cannot be parsed (empty name, unparseable address) do not crash a
  run. They are flagged `resolution_status = unresolved`, pass through as their
  own singleton canonical entity, and are counted in the run report.
- The mega-cluster guard (stage 6) routes any oversized cluster to review rather
  than auto-publishing it.
- Splink failures, empty blocks, or schema drift fail the stage loudly; the run
  is marked `failed` and no partial canonical write occurs.
- Because matching is set-based over staging tables, the existing
  per-record-`SELECT` churn and the `_find_address_by_fields` detached-object
  bug do not carry into the new pipeline.

## Testing strategy

- **Unit (property-based, Hypothesis):** the standardizers — name, address, and
  organization normalization. Invariants such as idempotency (standardizing
  twice equals standardizing once).
- **Golden set:** a hand-labeled fixture of known same/different pairs per entity
  type drives a precision/recall regression test. CI fails if precision drops
  below a configured floor.
- **Clustering invariants:** transitivity holds; no cluster exceeds the guard
  cap; singletons are preserved.
- **Idempotency:** running the pipeline twice on the same input produces an
  identical crosswalk.
- **Reversibility:** merging then unmerging a run restores the prior graph
  (this test is built in Phase 3, alongside the unmerge tooling it exercises).
- **Integration:** the full pipeline on a small real state extract, asserting an
  expected duplicate-reduction range.

## Rollout phases

| Phase | Scope | Outcome |
|-------|-------|---------|
| **0 — Source-layer completion** | `unified_reports` + `unified_transactions.report_id`; `unified_pledges`; `expenditure_categories`, `committee_purposes`, `spac_links`, `unified_notices`; loader manifest fix (directory glob over `tmp/<state>/`, including `_ss`/`_t` variants). | All `tmp/texas` data loaded and linked to reports. |
| **1 — Foundation + deterministic wins** | Canonical/crosswalk/audit schema + migration; `resolve` CLI skeleton; pipeline stages 1–3 and 7. | Exact-ish duplicates collapse, no ML. |
| **2 — Probabilistic matching** | Splink integration; stages 4–6; clustering; survivorship; mega-cluster guard. | True fuzzy matching. |
| **3 — Review queue + audit** | `merge_review` CLI workflow; match-explanation reports; reversibility tooling. | Every merge defensible and reversible. |
| **4 — Publish + cross-state hook** | Resolved views / fact table (`address_occupancy` and resolved transaction views); `master_entity_id` left in place, unbuilt. | Analysis-ready data; cross-state linking as a later add-on. |

Each phase ends with the pipeline runnable end-to-end on the scope built so far.

Two sequencing notes for the plan: (1) In Phase 1, stage 7 (survivorship/publish)
runs before stage 6 (clustering) exists, so Phase 1 includes a trivial clustering
path — each deterministic edge group is treated as a one-record-or-exact-group
cluster — which stage 6 replaces with real connected-components clustering in
Phase 2. (2) The reversibility test (see Testing strategy) lands with Phase 3,
when the unmerge tooling it exercises is built.

## Key files touched

- `app/core/unified_sqlmodels.py` — Phase 0 source-layer models; `report_id` on transactions.
- `app/core/unified_field_library.py` — field mappings for new record types.
- `app/ingest/file_reader.py` — schema entries for CVR1/CVR2/CVR3/EXCAT/SPAC.
- `scripts/loaders/loader_config.py` / `production_loader.py` — directory-glob ingestion.
- `app/resolve/` — new package: standardizers, blocking, Splink config, clustering, survivorship, CLI.
- `docs/DATA_RELATIONSHIPS.md` — updated ERD once the canonical layer exists.

## Open questions

- Exact band thresholds per entity type — to be tuned against the golden set in
  Phase 2; starting values (0.99 / 0.80) are placeholders.
- Mega-cluster size cap value — to be set from observed cluster-size
  distributions in Phase 2.
- Whether the review-queue CLI is sufficient long-term or a small UI is wanted
  (Phase 3 decision, out of scope here).

## Appendix A — `tmp/texas` coverage matrix

23 parquet files were checked against `CFS-ReadMe.txt` and the codebase.

| TEC record (files) | Texas model | Unified model | Status |
|--------------------|-------------|---------------|--------|
| RCPT — `contribs`, `cont_ss`, `cont_t` | TECContribution | UnifiedContribution | Models OK; `_ss`/`_t` not in load manifest |
| EXPN — `expend`, `expn_t` | TECExpenditure | UnifiedTransaction | Models OK; `expn_t` not in load manifest |
| LOAN — `loans` | TECLoan | UnifiedLoan | OK |
| DEBT — `debts` | TECDebt | UnifiedDebt | OK |
| CRED — `credits` | TECCredit | UnifiedCredit | OK |
| TRVL — `travel` | TECTravel | UnifiedTravel | OK |
| ASSET — `assets` | TECAsset | UnifiedAsset | Model OK; not in load manifest |
| FILER — `filers` | TECFiler | UnifiedCommittee | OK |
| PLDG — `pledges`, `pldg_ss`, `pldg_t` | TECPledge | (none) | Partial — no detail table |
| CAND — `cand` | TECCandidate | (folded into EXPN) | Partial — office detail lost |
| CVR1 — `cover`, `cover_ss`, `cover_t` | TECCoverSheet1 | (none) | Missing — the report layer |
| FINL — `finals` | TECFinalReport | (none) | Missing unified representation |
| CVR2 — `notices` | (none) | (none) | Missing entirely |
| CVR3 — `purpose` | (none) | (none) | Missing entirely |
| EXCAT — `expn_catg` | (none) | (none) | Missing — lookup table |
| SPAC — `spacs` | (none) | (none) | Missing — linkage data |

The `_ss` (special session) and `_t` files reuse their parent record's schema,
so they need no new model — only inclusion in ingestion. `loader_config.py`
currently enumerates 10 Texas files; 14 files in `tmp/texas` are unreferenced,
including `assets` and `cand` (which have models). Phase 0 switches ingestion to
a directory glob to fix this.
