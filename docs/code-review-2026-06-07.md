# Campaign Finance — Code Review
**Date:** 2026-06-07  
**Scope:** DB schema, Texas validators, field coverage, deduplication pipeline

---

## A) Filing ID Linkage

### What's working

**Reports → filing ID**: `UnifiedReport.report_ident` maps directly from TEC's `reportInfoIdent` (the canonical filing identifier). The `link_transactions_to_reports()` function in `reports_ingest.py` runs a SQL UPDATE that joins `unified_transactions` to `unified_reports` on `(state_id, report_ident)` — so every transaction gets a typed FK (`report_id`) once the CVR1 row exists. Re-runnable and idempotent.

**All financial record types carry `report_ident`**: contributions, expenditures, loans, debts, and travel all include `reportInfoIdent` in their source validators. The hub-and-spoke design (`UnifiedTransaction` ← `report_id` → `UnifiedReport`) is sound.

**Campaign name changes** are tracked via two complementary mechanisms:
- `UnifiedCommitteeVersion` stores JSON snapshots of the full committee record on each change (append-only audit trail).
- `CanonicalNameHistory` stores every name a canonical entity has ever filed under, with `first_seen_date`/`last_seen_date` and occurrence counts.

**Committee officers and persons**: `UnifiedCommitteePerson` is a proper junction table with `role`, `start_date`, `end_date`, covering treasurers, chairs, and assistants. `TECTreasurer` fields include `treasEffStartDt`/`treasEffStopDt` for tenure tracking.

**Notices (CVR2)**: `UnifiedNotice` carries `report_ident` + `committee_id` FK. Correctly handled.

**FINL records**: `build_final_report()` correctly mutates the existing `UnifiedReport.is_final = True` rather than creating a duplicate row.

### Gaps

**Gap 1 — Committee name at filing time not stored on `UnifiedReport`**

`UnifiedReport` has no `committee_name_at_filing` column. The raw CVR1 row provides `filerName`, and `build_report()` already receives it via `raw.get("filerName")` — but it's silently discarded. If the committee name changes after filing, you can only reconstruct the at-filing name by correlating `filed_date` against `UnifiedCommitteeVersion` timestamps, which is fragile.

**Fix**: Add `committee_name_at_filing: Optional[str]` to `UnifiedReport` and populate it from `raw.get("filerName")` in `build_report()`.

**Gap 2 — No direct treasurer → specific report linkage**

Treasurers are linked to committees (filers), not to reports. To know who signed a specific filing, you must query `UnifiedCommitteePerson` / `TECTreasurer` and filter by `treasEffStartDt ≤ report.filed_date ≤ treasEffStopDt`. This works but requires a non-obvious join. TEC CVR1 data includes a `treasNameLast`/`treasNameFirst` at the top of each filing — this is currently in `raw_data` JSON but not surfaced as a column.

**Fix** (minor): Add `treasurer_name_at_filing: Optional[str]` to `UnifiedReport`, or at minimum document the date-range join in a query helper.

---

## B) Report → Transaction Type Linkage

### What's working

| Record Type | Validator | `reportInfoIdent` | Staged Table | Unified Table |
|---|---|---|---|---|
| CVR1 (cover) | `reports_ingest.py` | ✅ | `unified_reports` | ✅ |
| RCPT (contributions) | `TECContribution` | ✅ | `tx_contributions` | `unified_contributions` |
| EXPN (expenditures) | `TECExpense` | ✅ | `tx_expenses` | `unified_expenditures` |
| LOAN | validator exists | ✅ | `tx_loan_data` | `unified_loans` |
| PLDG (pledges) | validator exists | ✅ | — | `unified_pledges` |
| CAND (direct expend) | `CandidateData` | ✅ | `tx_candidate_data` | — |
| CVR2 (notices) | `notices.py` | ✅ | — | `unified_notices` |
| FINL | `build_final_report()` | mutates CVR1 | — | — |
| ASSET | validator exists | ✅ | — | `unified_assets` |

### Gaps

**Gap 3 — Travel (TRVL) records are NOT staged to the `texas` schema**

`TECTravelData` defines `__tablename__` and `__table_args__` but is missing `table=True`. In SQLModel, `table=True` on the class definition is required for a row to be created in the DB. Without it, `TECTravelData` is a Pydantic validation model only — validated records are never written to `texas.tx_travel_data`.

This means travel records:
- Won't appear in the texas staging table
- Can't be fed to the deduplication pipeline
- Won't link to `unified_transactions` / `UnifiedTravel` records

**Fix**: Add `table=True` to `TECTravelData`:
```python
class TECTravelData(TECSettings, table=True):
```

**Gap 4 — `DebtData` is missing financial fields the unified schema expects**

`DebtData` (DEBT records) captures lender identity and address but is missing these fields that the `UnifiedLoan`/debt schema expects:
- `loan_amount` / debt amount
- `loan_date` / incurred date  
- `loan_interest_rate`
- `collateral_desc`
- `loan_status` / `isGuaranteed` (partial — has `loan_guaranteed_flag`)
- `repayment_dt`

TEC's `debts_*.csv` does contain these columns (debt amount, incurred date, interest rate). They're in the raw data but not captured in the validator.

**Fix**: Add the missing financial fields to `DebtData`.

**Gap 5 — `DebtData` uses inconsistent snake_case naming**

All other Texas validators use camelCase field names (`reportInfoIdent`, `filerIdent`). `DebtData` uses snake_case (`report_info_ident`, `filer_ident`, `loan_info_id`). If your CSV loader or field mapping code expects camelCase column names, debt records will fail to map.

**Fix**: Rename `DebtData` fields to camelCase and update any mappings.

**Gap 6 — `EXCAT` (expenditure categories) validator is commented out**

The `TECExpenseCategory` class in `texas_expenses.py` is commented out. Expenditure category code lookups (`expendCatCd` → `expendCatDescr`) won't be loaded. `expn_catg_*.csv` files ship a reference table you'd want for enrichment. Low priority but a data gap.

**Gap 7 — CVR3 (purpose) and SPAC have no visible unified models**

Both are tracked in `texas_coverage.py`'s `PREFIX_MAP` but no dedicated `UnifiedPurpose` or `UnifiedSpac` tables are visible. If these are intentionally in `raw_data` JSON only, document that decision. If they need structured tables, they're missing.

---

## C) Contributor Deduplication (Fuzzy Matching)

### What's working

The Splink-based entity resolution pipeline is well-designed:

**Per-entity-type configs**: Separate comparison and blocking configs for `person`, `organization`, and `committee` — appropriate since name similarity logic differs by entity type.

**Person comparisons** (`splink_config/person.py`):
- Jaro-Winkler on `first_name` and `last_name` at two thresholds (0.92 + 0.7) — handles nicknames, typos, initials
- Blocking rules: `(last_name_phonetic, zip3)` and `(first_name_phonetic, last_name_phonetic)` — effective at keeping pair counts manageable

**Organization comparisons** (`splink_config/organization.py`):
- Jaro-Winkler on `normalized_org` at three thresholds (0.92, 0.8, 0.7) — handles "Corp" vs "Corporation", punctuation differences
- Blocking: `(normalized_org, zip3)`

**Address deduplication**:
- `CanonicalAddress` as a shared hub — many entities → one address row
- TF-adjustment on street address line 1 — registered-agent buildings, PO boxes, shared addresses get near-zero Bayes weight ✅
- Phonetic blocking (`last_name_phonetic`, `first_name_phonetic`) prevents exact-spelling dependency

**EM training**: Random sampling for m-probability estimation. Fallback to `compare_two_records` for pairs Splink's bulk predict misses.

**Canonical layer**: `CanonicalEntity` gives one row per real-world entity with UUID, `canonical_name`, `normalized_name`, `canonical_address_id`, provenance JSON for survivorship auditing. `CanonicalNameHistory` tracks all names ever filed under.

### Gaps

**Gap 8 — `TECContribution` is missing `contributorStreetAddr1` and `contributorStreetAddr2`**

The contribution validator has city, state, county, country, postal code, and region — but not the actual street address lines. TEC's `contribs_*.csv` includes `contributorStreetAddr1` and `contributorStreetAddr2`. The unified field library correctly maps `contributorStreetAddr1` → `contributor_street_1` with confidence 1.0, but the source validator never captures these fields.

This is a significant deduplication gap: blocking on `last_name_phonetic + zip3` works, but address-based comparisons will have no street line to match on for contribution donors. You can still block and score, but the address Bayes factor will be weaker.

**Fix**: Add to `TECContributionBase`:
```python
contributorStreetAddr1: Optional[str] = Field(
    default=None, description="Contributor street address line 1"
)
contributorStreetAddr2: Optional[str] = Field(
    default=None, description="Contributor street address line 2"
)
```

**Gap 9 — Employer/occupation signals not used in deduplication**

`TECContribution` captures `contributorEmployer`, `contributorOccupation`, and `contributorJobTitle` — these are high-quality auxiliary signals for disambiguation. Two donors with the same common name (e.g., "John Smith, Austin TX") who share employer/occupation are almost certainly the same person.

These fields exist in the validator but are not included in Splink's comparison set.

**Fix**: Add an auxiliary comparison in `person.py`:
```python
cl.JaroWinklerAtThresholds("employer", [0.88]),
```
Or treat employer as a blocking aid: donors with identical employer + zip3 who clear the name threshold get a strong match boost.

**Gap 10 — Travel entity deduplication is blocked by Gap 3**

Because `TECTravelData` doesn't write to the staging table (Gap 3), traveler identities never enter the entity resolution pipeline. Once Gap 3 is fixed, traveler identities (INDIVIDUAL/ENTITY) should be extracted to `unified_entities` and run through the same Splink pipeline.

**Gap 11 — `UnifiedCampaign` only built when `election_year IS NOT NULL`**

In `campaigns.py`, the canonical campaign builder filters `primary_committee_id IS NOT NULL AND election_year IS NOT NULL`. Campaigns without a mapped election year are silently excluded from the canonical layer. For multi-cycle or officeholder committees this means no canonical campaign row, which in turn means no `CanonicalCampaign` FK on their transactions.

**Fix**: Either derive a default `election_cycle` from the latest report's `period_end` year, or build a "no election year" canonical campaign record with `election_cycle = NULL` and adjust the unique constraint accordingly.

---

## Summary: Priority Fixes

| # | Severity | Issue | File |
|---|---|---|---|
| 3 | 🔴 Critical | `TECTravelData` missing `table=True` — travel never staged | `texas_traveldata.py` |
| 8 | 🔴 Critical | `TECContribution` missing street address lines | `texas_contributions.py` |
| 4 | 🟠 High | `DebtData` missing financial fields (amount, date, rate, status) | `texas_debtdata.py` |
| 5 | 🟠 High | `DebtData` snake_case vs camelCase naming inconsistency | `texas_debtdata.py` |
| 1 | 🟡 Medium | `UnifiedReport` doesn't capture `committee_name_at_filing` | `reports_ingest.py` + `reports.py` |
| 11 | 🟡 Medium | Campaigns excluded when `election_year IS NULL` | `campaigns.py` |
| 9 | 🟡 Medium | Employer/occupation not used in person deduplication comparisons | `splink_config/person.py` |
| 2 | 🟢 Low | Treasurer-to-report link requires date-range join (no direct FK) | architecture |
| 6 | 🟢 Low | `EXCAT` validator commented out | `texas_expenses.py` |
| 7 | 🟢 Low | CVR3/SPAC have no visible unified tables | coverage |

---

## Field Coverage Checklist (Texas CSV → DB)

| File type | Validator | All TEC fields captured? | Notable missing |
|---|---|---|---|
| `contribs_*.csv` (RCPT) | `TECContribution` | ⚠️ Mostly | `contributorStreetAddr1`, `Addr2` |
| `expend_*.csv` (EXPN) | `TECExpense` | ✅ Yes | — |
| `loans_*.csv` (LOAN) | loan validator | ✅ Likely | — |
| `debts_*.csv` (DEBT) | `DebtData` | ⚠️ Partial | amount, date, interest rate, status |
| `travel_*.csv` (TRVL) | `TECTravelData` | ✅ Fields present, ⚠️ not staged | `table=True` missing |
| `cover_*.csv` (CVR1) | `build_report()` | ⚠️ Mostly | `filerName` at filing |
| `filers_*.csv` (FILER) | `TECFiler` | ✅ Yes | — |
| `pledges_*.csv` (PLDG) | pledge validator | ✅ Yes | — |
| `cand_*.csv` (CAND) | `CandidateData` | ⚠️ Partial | name validation disabled |
| `assets_*.csv` (ASSET) | asset validator | ⚠️ By design | TEC provides description only |
| `expn_catg_*.csv` (EXCAT) | commented out | ❌ Not captured | entire category table |
| `finals_*.csv` (FINL) | `build_final_report()` | ✅ Yes (mutates CVR1) | — |
| `notices_*.csv` (CVR2) | `UnifiedNotice` | ✅ Yes | — |
| `purpose_*.csv` (CVR3) | unknown | ❓ | no unified table visible |
| `spacs_*.csv` (SPAC) | unknown | ❓ | no unified table visible |

---

## D) Cross-Role Entity Matching & Employer Changes

### Employer changes over time

Employer is a disambiguator, not an identity anchor. People change employers; using it as a blocking key would cause the same person to not be blocked together across filing years.

**What to change:**
- Never use employer in a blocking rule.
- Keep employer as a **comparison feature only** — when two records share employer + name, that adds strong Bayes weight. When they don't match, it adds near-zero negative weight (since it legitimately changes).
- For survivorship on `CanonicalEntity`, employer should come from the most recent record by date, not the most common.

**What to add — employer history:**

Store employer history analogously to `CanonicalNameHistory`, either as a dedicated `CanonicalEmployerHistory` table or as a JSON array in `provenance_json`:

```json
{"employer_history": [
  {"value": "Acme Corp", "first_seen": "2016-01-01", "last_seen": "2019-12-31"},
  {"value": "Beta LLC",  "first_seen": "2021-03-15", "last_seen": null}
]}
```

This lets you answer "who was this person's employer at the time of this filing" with a date-range join — the same pattern used for treasurers.

---

### Cross-role matching (contributor ↔ payee)

The architecture is correctly designed: `CanonicalEntity` has no role field, roles are captured on transactions (`transaction_type = CONTRIBUTION / EXPENDITURE`). The same `CanonicalEntity.id` can appear on both a contribution and an expenditure. The pipeline just needs to find those matches.

**The gap: address-based blocking misses cross-role pairs**

A political consultant who also donates will have:
- As a **contributor**: home address (Houston, TX 77002)
- As a **payee on expenditures**: business address (Austin, TX 78701)

Current blocking rules `(last_name_phonetic, zip3)` won't put these two records in the same candidate pair because zip3 differs. They resolve as two separate canonical entities.

**Fix: add a name-only fallback blocking rule**

In `splink_config/person.py` `PREDICTION_BLOCKING_RULES`, add:

```python
block_on("first_name_phonetic", "last_name_phonetic", "state_code"),
```

This blocks by name + state without requiring zip match. The TF-adjusted address comparison then provides the Bayes evidence to distinguish true matches from false positives (e.g., two unrelated "John Smith"s in Texas).

For organizations, add to `splink_config/organization.py`:

```python
block_on("normalized_org", "state_code"),
```

**The role-field normalization dependency**

Before Splink runs, contributor records carry `contributorNameFirst`/`contributorNameLast` and expenditure payees carry `payeeNameFirst`/`payeeNameLast`. Both need to normalize to the same columns (`first_name`, `last_name`) in `unified_entities`. The unified field library maps both correctly — confirm the entity extraction step uses those unified names consistently for both roles, otherwise Splink's comparisons are comparing mismatched columns.

**Post-resolution: role consolidation diagnostic query**

After Splink clusters, run this as a data-quality check:

```sql
SELECT ce.id, ce.canonical_name,
       SUM(CASE WHEN ut.transaction_type = 'CONTRIBUTION' THEN 1 ELSE 0 END) AS contrib_count,
       SUM(CASE WHEN ut.transaction_type = 'EXPENDITURE' THEN 1 ELSE 0 END) AS expend_count
FROM canonical_entity ce
JOIN unified_entities ue ON ue.canonical_entity_id = ce.id
JOIN unified_transactions ut ON ut.entity_id = ue.id
GROUP BY ce.id, ce.canonical_name
HAVING contrib_count > 0 AND expend_count > 0
ORDER BY contrib_count + expend_count DESC;
```

This surfaces high-activity cross-role entities for spot-checking and gives you a quality signal on how well cross-role dedup is working.

---

### Summary

| Problem | Don't | Do |
|---|---|---|
| Employer changes | Block on employer | Compare only; store employer history by date |
| Cross-role person match | Require zip match | Add `(first_name_phonetic, last_name_phonetic, state_code)` blocking rule |
| Cross-role org match | Require zip match | Add `(normalized_org, state_code)` blocking rule |
| Verifying it works | Assume Splink caught them | Run role-consolidation query post-resolution |

The net result is one `CanonicalEntity` row per real-world person or organization regardless of whether they appear as a donor, a vendor, or both — with transaction type on each linked record indicating the role.
