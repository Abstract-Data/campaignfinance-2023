# Response to Code Review 2026-06-07 — Verified Findings & Implementation Plan

Every claim in the review was checked against the code **and the actual TEC CSV headers in `tmp/texas/`**. Four of the eleven gaps are false positives, one is mis-diagnosed, and one is more severe than reported. The corrected picture changes the implementation order significantly.

---

## 1. Verification scorecard

| # | Review claim | Verdict | Reality |
|---|---|---|---|
| 3 | `TECTravelData` missing `table=True` | ✅ Confirmed (critical) | Wired into `CategoryTypes["travel"]` but can never persist — no table mapping. Also uses `pydantic.Field`, not `sqlmodel.Field`, so `primary_key=True` is inert. |
| 5 | `DebtData` snake_case naming | ✅ Confirmed — **worse than reported** | CSV headers are camelCase (`reportInfoIdent`, `loanInfoId`); `DebtData` requires snake_case fields with no alias generator. **Every DEBT row fails validation today — 100% data loss**, not a style issue. |
| 4 | `DebtData` missing amount/date/rate | ❌ Mis-diagnosed | `debts_*.csv` has **no** amount, date, or interest-rate columns (verified against the live header: 89 cols = header + lender + guarantors 1–5). Those fields live in `loans_*.csv` (146 cols), already handled. The **real** gap: all 65 guarantor columns are uncaptured — guarantors are exactly the kind of entity the dedup pipeline wants. |
| 8 | Contribution validator missing `contributorStreetAddr1/2` | ❌ False positive | TEC's public extract **does not ship street lines for contributors** (verified header: city/state/county/country/zip/region only). Nothing to capture. Implication is for Splink, not the validator — see §3. |
| 1 | `committee_name_at_filing` not on `UnifiedReport` | ✅ Confirmed (minor nuance) | Not "silently discarded" — it's inside `raw_data` JSON, so existing rows are backfillable without re-ingest. |
| 2 | No treasurer→report link | ✅ Confirmed | `cover_*.csv` carries `treasPersentTypeCd`, `treasNameOrganization`, `treasNameLast/First` etc.; all end up only in `raw_data`. |
| 6 | EXCAT commented out | ❌ False positive (mostly) | `ExpenditureCategory` in `app/core/source_models/lookups.py` handles EXCAT. The commented block in `texas_expenses.py` is dead code — delete it. |
| 7 | CVR3/SPAC have no unified models | ❌ False positive | `CommitteePurpose` (lookups.py, `build_committee_purpose` registered for CVR3) and `spac.py` (table=True) both exist. |
| 9 | Employer not used in dedup | ✅ Confirmed — bigger than reported | `employer` is captured on `UnifiedPerson` but **never reaches `ResolutionInput`** (no column). Fix spans stage1 extraction + staging schema + Splink config, not just `person.py`. |
| 10 | Travel dedup blocked by #3 | ✅ Confirmed | Processor already maps `TRVL → PersonRole.PAYEE ("traveller")`, so once staged, travel flows through existing machinery. |
| 11 | Campaigns dropped when `election_year IS NULL` | ✅ Confirmed | `campaigns.py:76` filters them out; officeholder/multi-cycle committees get no `CanonicalCampaign`. |
| D-person | Cross-role pairs missed by zip3 blocking | ❌ False positive for persons | `PREDICTION_BLOCKING_RULES` already includes `block_on("first_name_phonetic", "last_name_phonetic")` with **no geographic constraint** — cross-role person pairs are generated. |
| D-org | Cross-role org pairs missed | ✅ Confirmed | Orgs only block on `(normalized_org, zip3)`; a vendor/donor org with two addresses resolves as two entities. |
| D-employer | Don't block on employer; keep history | ✅ Sound advice | Survivorship picks employer implicitly via the best-name row; no history kept. |

Process note: the review proposed schema "fixes" for source columns that don't exist (Gaps 4, 8). Any future field-coverage review should diff validators against `head -1` of the actual CSVs (or `CFS-ReadMe.txt`) first — a 10-line script, see §4.

---

## 2. Implementation plan (corrected priority order)

### Wave 1 — Data-loss bugs (do first; both are ingest-blocking)

**1a. `DebtData` → camelCase rebuild** (`texas_debtdata.py`)
Rename every field to match the CSV header exactly (`recordType`, `formTypeCd`, `reportInfoIdent`, `loanInfoId`, `lenderPersentTypeCd`, …) and add the five guarantor blocks (`guarantorPersentTypeCd1..5`, name + address fields each). Generate the guarantor fields programmatically or accept the verbosity — but they must exist, because guarantor identities should be extracted as entities later. Update the model validators (`lender_persent_type_cd` → `lenderPersentTypeCd` keys). Migration: the `texas.tx_debt_data` table columns must be renamed or the table dropped and recreated — it's empty today (all rows failed validation), so drop/recreate is safe.

**1b. `TECTravelData` → real table** (`texas_traveldata.py`)
```python
from sqlmodel import Field          # not pydantic.Field — primary_key needs sqlmodel
class TECTravelData(TECSettings, table=True):
```
Create `texas.tx_travel_data` (metadata create_all or migration). Then verify travel rows land, and that the processor's `TRVL` role mapping produces traveller entities in `unified_entities` (Gap 10 resolves automatically; add one integration test asserting a traveller appears in `resolution_input`).

Run `gitnexus_impact` on both classes before editing, per CLAUDE.md; re-run `npx gitnexus analyze` after committing.

### Wave 2 — Point-in-time columns on `UnifiedReport`

Add to `reports.py`:
```python
committee_name_at_filing: Optional[str]
treasurer_name_at_filing: Optional[str]
```
Populate in `build_report()` from `raw.get("filerName")` and the `treas*` name parts (handle INDIVIDUAL vs ENTITY via `treasPersentTypeCd`). **Backfill existing rows from `raw_data` JSON** — one SQL UPDATE with `json_extract`, no re-ingest needed. Skip a treasurer FK; the date-range join against `TECTreasurer` is correct relational design — wrap it in a query helper (e.g. `treasurer_for_report(report)`) and document it.

### Wave 3 — Resolution-quality improvements

**3a. Employer as comparison signal (never blocking).**
Add `employer: str | None` to `ResolutionInput` (staging.py), populate in stage1 from `UnifiedPerson.employer` (normalized: upper, strip punctuation — reuse org-normalization helpers in `standardize/orgs.py`), then in `splink_config/person.py`:
```python
cl.JaroWinklerAtThresholds("employer", [0.88]),
```
Re-run EM training after adding the comparison — m/u probabilities must be re-estimated. Expect employer to be null-heavy (only contribution-sourced persons have it); Splink handles nulls natively.

**3b. Org cross-role blocking.**
Add to `organization.py` `PREDICTION_BLOCKING_RULES`: `block_on("org_name_phonetic", "state")` (the staging column is `state`, not `state_code` — the review's suggested column doesn't exist). Heed the lock-step warning in the config comments: mirror the rule in `app.resolve.blocking` default rules and **measure pair counts before/after** — orgs are far fewer than persons, so explosion risk is low, but verify.

**3c. Campaigns with NULL election year.**
In `campaigns.py`, drop `AND election_year IS NOT NULL`; key the identity tuple as `(committee_entity_id, office, election_year | None)` and make `CanonicalCampaign.election_cycle` nullable with the unique constraint adjusted (Postgres treats NULLs as distinct in unique constraints — use `NULLS NOT DISTINCT` or a sentinel cycle of 0 to avoid duplicate "no-cycle" rows on re-runs; sentinel is simpler and the builder is delete-and-rebuild anyway).

**3d. Employer history.**
Cheapest correct version: in survivorship, aggregate `(employer, first_activity_date, last_activity_date)` per cluster into `provenance_json["employer_history"]`, and set the scalar employer from the **most recent** record, not the best-name row. Promote to a `CanonicalEmployerHistory` table only when something needs to query it relationally (the `CanonicalNameHistory` pattern is there to copy).

### Wave 4 — Hygiene

Delete the commented `TECExpenseCategory` block in `texas_expenses.py`. Add the review's cross-role consolidation SQL as a QA check in `publish/views.py` or the review CLI. Document that contributor street lines don't exist in TEC public data, so person `line_1` comparisons only fire for entities sourced from filer/treasurer/lender records.

---

## 3. Is there a better way? (whole-codebase re-review)

### Verdict: the architecture is right; the execution layer between staging and unified is the one part worth rethinking.

The shape — state validators → state staging schema → unified hub-and-spoke (`UnifiedTransaction` ← `report_id` → `UnifiedReport`) → Splink/DuckDB resolution → canonical layer with crosswalks and survivorship provenance — is the textbook design for multi-state campaign-finance ETL. CanonicalEntity without a role field, roles on transactions, TF-adjusted address comparisons, append-only crosswalks: all correct decisions. Don't replace Splink; don't restructure the schema.

**The weak point is throughput, not correctness.** The pipeline is vectorized at both ends (Polars for CSV→parquet; DuckDB for Splink) but the middle — validation and unified-building — is row-by-row Pydantic + ORM:

1. `FileReader` streams rows through Pydantic validators one at a time, then `session.add_all()` in batches.
2. `processor.py`/`builders.py` build `UnifiedPerson`/`UnifiedEntity`/`UnifiedTransaction` per row, with per-row dedup lookups (`_find_person_by_name_state`) softened by in-memory caches.

With ~100+ contribution files (tens of millions of rows), the ORM round-trips in the middle become days-vs-minutes. Notably, `link_transactions_to_reports()` already proves the better pattern in this same codebase — one set-based SQL UPDATE.

**Recommended evolution (incremental, not a rewrite):**

1. **Vectorize validation with a quarantine pattern.** You already have parquet. Express the per-field checks (blank→null, INDIVIDUAL/ENTITY name requirements, date parsing) as Polars expressions; split each frame into `valid` → bulk `COPY`/`INSERT` into the staging table, and `rejects` → a quarantine table with the failure reason. Keep the Pydantic models as the schema source of truth and for API/single-record paths; the Polars rules can be generated from them. This also makes the DebtData failure mode impossible to miss — 100% quarantine on day one is loud, silent row-by-row exceptions are not.
2. **Make texas→unified set-based.** The `unified_field_library` mappings are already declarative — they can compile to `INSERT INTO unified_x SELECT … FROM texas.tx_y` statements (or Polars transforms) instead of driving per-row builder calls. Entity get-or-create becomes: insert distinct `(entity_type, normalized_name, state_id)` with `ON CONFLICT DO NOTHING`, then join back for FKs. The DB unique indexes this requires are exactly PIPELINE_REVIEW's outstanding "Fix 6."
3. **Don't add a third engine.** DuckDB is already in the stack for Splink; if Postgres set-based SQL ever isn't enough, do the staging→unified transform in DuckDB over the parquet directly and `COPY` results in. But Postgres `INSERT…SELECT` will likely suffice.
4. **Resist consolidating away the staging schema.** Three copies of the data (parquet, `texas.*`, `unified_*`) looks redundant but buys auditability and per-state isolation — keep it.

What I would *not* adopt from common alternatives: dlt/dbt would replace working bespoke code for marginal gain at this stage; FEC-style "just load raw + views" forfeits the validation layer that's already catching real TEC data defects; an off-the-shelf MDM tool loses the tuned blocking rules and review queue already built around Splink.

### Sequencing with the gap fixes
Wave 1–2 are small and should land on the current row-by-row path now. If full-history Texas loads are on the near-term roadmap, do the set-based refactor (items 1–2 above) **before** investing further in per-row builder features — every gap fix added to the ORM path is code you'll port later.
