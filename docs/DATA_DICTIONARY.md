# Data Dictionary
# Campaign Finance — Field Definitions & Data Classification
# Last Updated: 2026-05-25

This document is the authoritative reference for all fields in the unified data model.
Agents should consult this file when:
- Adding or renaming a field in `UnifiedFieldLibrary`
- Writing validators for new state data
- Deciding what to log at DEBUG level (see PII policy below)
- Reviewing the `raw_data` column access policy

---

## Unified Field Library — Field Catalogue

All fields are defined in `app/core/unified_field_library.py`. The table below is the
human-readable reference. FieldCategory drives validation routing and PII classification.

### Transaction Core Fields

| Field | Category | PII | Description |
|-------|----------|-----|-------------|
| `transaction_id` | `TRANSACTION_ID` | No | Unique identifier for a financial transaction. Not always present in source data. |
| `amount` | `AMOUNT` | No | Monetary amount (USD). Negative values indicate refunds or corrections. Stored as `NUMERIC(15,2)`. |
| `transaction_date` | `DATE` | No | Date when the transaction occurred. Not always present. |
| `description` | `DESCRIPTION` | No | Free-text description or purpose of the transaction. |
| `transaction_type` | `TYPE` | No | Type: `contribution`, `expenditure`, `loan`, `pledge`. |
| `parent_type` | `TYPE` | No | Parent transaction type code (e.g., `RCPT`, `EXPN`). Used for sub-transaction linking. |
| `parent_id` | `TRANSACTION_ID` | No | Parent transaction ID for sub-transaction linkage. |
| `parent_amount` | `AMOUNT` | No | Parent transaction amount. |

### Person / Contributor Fields (PII)

| Field | Category | PII | Description |
|-------|----------|-----|-------------|
| `person_first_name` | `PERSON_NAME` | **Yes** | First name of contributing person. |
| `person_last_name` | `PERSON_NAME` | **Yes** | Last name of contributing person. |
| `parent_full_name` | `PERSON_NAME` | **Yes** | Full name associated with parent transaction. |
| `person_organization` | `PERSON_ORGANIZATION` | **Yes** | Organization name for entity contributors. |
| `person_employer` | `PERSON_EMPLOYMENT` | **Yes** | Employer of the contributing person. |
| `person_occupation` | `PERSON_EMPLOYMENT` | **Yes** | Occupation of the contributing person. |

### Address Fields (PII)

| Field | Category | PII | Description |
|-------|----------|-----|-------------|
| `address_line1` | `PERSON_ADDRESS` | **Yes** | Street address line 1. |
| `address_line2` | `PERSON_ADDRESS` | **Yes** | Street address line 2 (suite, apt, etc.). |
| `address_city` | `PERSON_ADDRESS` | Partial | City name. Used in address deduplication. |
| `address_state` | `PERSON_ADDRESS` | No | Two-letter state/province code. |
| `address_zip` | `PERSON_ADDRESS` | Partial | ZIP/postal code (5-digit or 9+4 format). |
| `departure_city` | `PERSON_ADDRESS` | No | Departure city for travel expenditures. |
| `arrival_city` | `PERSON_ADDRESS` | No | Arrival city for travel expenditures. |

### Committee Fields

| Field | Category | PII | Description |
|-------|----------|-----|-------------|
| `committee_name` | `COMMITTEE_NAME` | No | Name of the political committee. |
| `committee_type` | `COMMITTEE_TYPE` | No | Committee type: `candidate`, `pac`, `party`, `other`. |

### Filing / Metadata Fields

| Field | Category | PII | Description |
|-------|----------|-----|-------------|
| `filed_date` | `FILING_INFO` | No | Date the report was filed with the regulator. |
| `amended` | `AMENDMENT_INFO` | No | Whether this is an amended filing. Default `false`. |
| `loan_guaranteed_flag` | `FILING_INFO` | No | Whether the loan has a guarantor. Default `false`. |
| `loan_guarantee_amount` | `AMOUNT` | No | Amount guaranteed by a third party. |

### Travel Expenditure Fields

| Field | Category | PII | Description |
|-------|----------|-----|-------------|
| `transportation_type_cd` | `TYPE` | No | Transportation type code (airline, car, etc.). |
| `transportation_type_descr` | `DESCRIPTION` | No | Human-readable transportation description. |
| `departure_dt` | `DATE` | No | Departure datetime for travel. |
| `arrival_dt` | `DATE` | No | Arrival datetime for travel. |
| `travel_purpose` | `DESCRIPTION` | No | Purpose of the travel expenditure. |
| `asset_descr` | `DESCRIPTION` | No | Description of a campaign asset. |

---

## `UnifiedTransaction.raw_data` — Access Policy

**Definition:** `raw_data: str | None` — JSON column (`Text`) on the `UnifiedTransaction`
table (`app/core/models/tables.py:342`). Stores the original source CSV row exactly as
received from the state portal before normalization.

**Why it exists:** Preserves the source record for audit trails and debugging when the
normalized fields lose information.

**What it contains:** The raw CSV row in JSON form, including all source column values.
For Texas and Oklahoma data, this includes PII fields: contributor name, address,
employer, and occupation in their original un-normalized form.

**Access restrictions:**

| Context | Policy |
|---------|--------|
| DEBUG logging | **PROHIBITED.** Never log `raw_data` or the full record dict at any log level. Log `id`, `state`, and `transaction_type` only. |
| API responses | **PROHIBITED.** `raw_data` must never appear in any public or internal API response. |
| Analytics queries | **PROHIBITED.** Exclude from `SELECT *`; reference explicitly by field. |
| Data engineering / pipeline debugging | **PERMITTED.** Direct DB access for authorized data engineering work only. |
| Test fixtures | **PERMITTED with caution.** Use anonymized/synthetic data in fixtures — never real contributor PII. |

**Correct logging pattern:**
```python
# BAD — logs PII
logger.debug("Processing record: %s", record)
logger.debug("raw_data: %s", transaction.raw_data)

# CORRECT — safe identifiers only
logger.debug("Processing record id=%s state=%s type=%s",
             record.get("id"), state, record.get("transaction_type"))
```

**ADR reference:** `docs/adr/0002-data-classification-and-retention.md` governs the
retention and access policy for all PII fields.

---

## FieldCategory Reference

| Category | PII Classification | Fields |
|----------|--------------------|--------|
| `TRANSACTION_ID` | No | `transaction_id`, `parent_id` |
| `AMOUNT` | No | `amount`, `loan_guarantee_amount`, `parent_amount` |
| `DATE` | No | `transaction_date`, `filed_date`, `departure_dt`, `arrival_dt` |
| `DESCRIPTION` | No | `description`, `transportation_type_descr`, `travel_purpose`, `asset_descr` |
| `TYPE` | No | `transaction_type`, `parent_type`, `transportation_type_cd` |
| `PERSON_NAME` | **Yes** | `person_first_name`, `person_last_name`, `parent_full_name` |
| `PERSON_ORGANIZATION` | **Yes** | `person_organization` |
| `PERSON_EMPLOYMENT` | **Yes** | `person_employer`, `person_occupation` |
| `PERSON_ADDRESS` | **Yes** (line1/2); Partial (city/zip) | All `address_*` fields |
| `COMMITTEE_NAME` | No | `committee_name` |
| `COMMITTEE_TYPE` | No | `committee_type` |
| `FILING_INFO` | No | `filed_date`, `loan_guaranteed_flag` |
| `AMENDMENT_INFO` | No | `amended` |

---

## Adding a New Field

1. Add the `FieldDefinition` entry to `UnifiedFieldLibrary.__init__()` in
   `app/core/unified_field_library.py` with the correct `FieldCategory`.
2. Add the state-specific source column mapping in the appropriate
   `StateFieldMapping` entry.
3. Update this file with the new field's row, PII classification, and description.
4. If the field carries PII, update `docs/adr/0002-data-classification-and-retention.md`.
5. Run `uv run pytest tests/ -q -k "field_library or unified_field"` to confirm no
   existing mappings broke.
