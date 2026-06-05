# Campaign Finance Pipeline — Data Quality Review
*Generated 2026-05-27. Covers the full ingest path: field_library → builder → processor → state_loader → DB.*

---

## Executive Summary

Seven bugs collectively prevent clean, accurate, fully-linked records from reaching the database. They fall into three clusters:

1. **Role-blind person extraction** — the builder resolves all contributor/payee/candidate/recipient fields to the same unified name, so every role gets the same person, and expenditures/loans produce phantom contributor rows.
2. **Entity relationships not persisted** — `UnifiedContribution`, `UnifiedLoan`, etc. require `contributor_entity_id`/`recipient_entity_id` foreign keys, but those entities are created in-memory and are not reliably flushed before the FK is written.
3. **Missing dedup constraints at the DB layer** — person, address, and transaction-person deduplication is done only in Python; no unique constraints back it up, so concurrent runs or edge cases silently create duplicates.

---

## Issue 1 — CRITICAL: All Four Person Roles Resolve to the Same Fields

**File:** `app/core/processor.py`, `_build_participants()` +
`app/core/unified_field_library.py`, Texas mappings

### What happens
`_build_participants` calls `builder.build_person(raw_data, role)` for all four roles (CONTRIBUTOR, RECIPIENT, PAYEE, CANDIDATE) from the **same raw dict**. Inside `build_person`, the only thing looked up is `person_first_name`, `person_last_name`, `person_organization` — three unified field names. The field library maps every role-specific TEC column to those same three names:

```
contributorNameFirst  → person_first_name   ← contribution rows
payeeNameFirst        → person_first_name   ← expenditure rows
lenderNameFirst       → person_first_name   ← loan rows
pledgerNameFirst      → person_first_name   ← pledge rows
candidateNameFirst    → person_first_name   ← expenditure rows (secondary person)
```

`_get_field_value` loops through the mappings and returns **the first one found in raw_data**, so on a contribution row it finds `contributorNameFirst` and returns that value for ALL four roles. Result:

- **Contribution rows**: CONTRIBUTOR, RECIPIENT, PAYEE, CANDIDATE all become "John Smith" (the contributor). Three extra phantom `UnifiedTransactionPerson` rows per transaction.
- **Expenditure rows**: `payeeNameFirst` wins → CONTRIBUTOR, RECIPIENT, PAYEE, CANDIDATE all become the payee. The actual filer/committee that wrote the check appears nowhere as the payer.
- **Loan rows**: lender fields win for all roles.

### What it should look like

Each TEC file type has exactly **one external party**. The committee is the implicit other party and is already captured via `filerIdent → committee_filer_id`. The correct role map is:

| File type      | External party role | Relevant TEC field prefix |
|----------------|---------------------|---------------------------|
| contributions  | CONTRIBUTOR         | `contributor*`            |
| expenditures   | PAYEE               | `payee*`                  |
| loans          | CONTRIBUTOR (lender)| `lender*`                 |
| pledges        | CONTRIBUTOR         | `pledger*`                |
| credits        | CONTRIBUTOR (payor) | `payor*`                  |
| travel         | PAYEE (traveler)    | `traveller*`              |

Only ONE role should produce a person from a given row. The other three should return `None`.

### Fix

**Option A (simpler):** Add role-scoped unified field names to the library — `contributor_first_name`, `payee_first_name`, etc. — and pass `role` into `build_person` to select the right prefix. This is the lowest-risk change.

**Option B (cleaner):** In `_build_participants`, use `record_type` (already present in raw_data for TEC) to dispatch to a role-specific extraction function that only builds the relevant person.

---

## Issue 2 — CRITICAL: `UnifiedContribution.contributor_entity_id` Is Never Reliably Set

**Files:** `app/core/processor.py`, `_build_contribution_detail()` + `_entity_context()`

### What happens

`_build_contribution_detail` gets `contributor_entity` from `ctx["contributor_entity"]`, which is `contributor.entity` (the `UnifiedEntity` object built inline by `build_person` → `_get_or_create_entity`). That entity is a **pending SQLAlchemy object** with no `id` yet (it hasn't been flushed). When `UnifiedContribution` is constructed with `contributor=contributor_entity`, SQLModel sets `contributor_entity_id` from the relationship — but only after flush. In practice, the entity flush path works for the happy path. However:

1. The fallback logic is **backwards**: `if not contributor_entity and committee and committee.entity: contributor_entity = committee.entity`. If the contributor is missing, this makes the **committee donate to itself**. Contributions where `contributorNameFirst` is blank (e.g., self-funding entries, anonymous PAC transfers) silently become self-contributions.

2. `recipient_entity` is set correctly in `_entity_context` (to the committee's entity), but then in `_build_contribution_detail`, `if not recipient_entity and recipient and recipient.entity` uses `PersonRole.RECIPIENT` as a fallback. Because of Issue 1, `recipient.entity` is the **contributor's** entity (same person as the contributor), so the contribution ends up with contributor == recipient.

### Fix

- Remove the "committee as fallback contributor" line — if `contributor_entity` is None, skip creating the `UnifiedContribution` or mark it as anonymous.
- Remove the `PersonRole.RECIPIENT` fallback — the committee entity from `_entity_context` is the correct recipient.
- Add a `session.flush()` before constructing detail records so that entity IDs are populated.

---

## Issue 3 — HIGH: Expenditure Type Has No Detail Record

**File:** `app/core/processor.py`, `DETAIL_BUILDERS` dict

```python
DETAIL_BUILDERS: dict[TransactionType, DetailBuilder] = {
    TransactionType.CONTRIBUTION: _build_contribution_detail,
    TransactionType.LOAN:         _build_loan_detail,
    TransactionType.DEBT:         _build_debt_detail,
    TransactionType.CREDIT:       _build_credit_detail,
    TransactionType.TRAVEL:       _build_travel_detail,
    TransactionType.ASSET:        _build_asset_detail,
    # TransactionType.EXPENDITURE is MISSING
}
```

Expenditure transactions (the second most common type in TEC data) are stored in `unified_transactions` only — with no linked `UnifiedContribution` equivalent. You have no structured way to query "who did committee X pay and how much" without parsing the `persons` junction table by role.

### Fix

Add an `UnifiedExpenditure` table (mirroring `UnifiedContribution`) and a `_build_expenditure_detail` builder that sets `payer_entity_id = committee.entity.id` and `payee_entity_id = contributor_entity.id`. Alternatively, reuse `UnifiedContribution` with a `direction` enum field. Either way, `DETAIL_BUILDERS[TransactionType.EXPENDITURE]` must be wired up.

---

## Issue 4 — HIGH: Entity Dedup Is Not State-Scoped

**File:** `app/core/builders.py`, `_find_entity()`

```python
query = select(UnifiedEntity).where(
    UnifiedEntity.entity_type == entity_type,
    UnifiedEntity.normalized_name == normalized_name,
)
```

No `state_id` filter. "John Smith" as a person entity in Texas will match "John Smith" in Oklahoma, collapsing two unrelated people into one entity. For multi-state ingest this will corrupt entity links across all states.

### Fix

```python
query = select(UnifiedEntity).where(
    UnifiedEntity.entity_type == entity_type,
    UnifiedEntity.normalized_name == normalized_name,
    UnifiedEntity.state_id == self.state_id,  # add this
)
```

---

## Issue 5 — HIGH: NULL Address Columns Break Dedup Queries

**File:** `app/core/unified_state_loader.py`, `_persist_transaction_from_record()`

```python
existing_address = session.exec(
    select(UnifiedAddress).where(
        UnifiedAddress.street_1 == person.address.street_1,  # ← NULL = NULL is FALSE in SQL
        UnifiedAddress.city     == person.address.city,
        ...
    )
).first()
```

In SQL, `NULL = NULL` evaluates to `FALSE` (not `TRUE`). Since TEC contribution files have no street data, `street_1` is always `NULL`. This WHERE clause never matches an existing address, so every transaction creates a new `UnifiedAddress` row for the same city/state/zip. You end up with tens of thousands of duplicate address rows.

The same problem exists in `builders.py`, `_find_address_by_fields()` — it skips adding the `street_1` filter when the value is falsy, which is correct, but the city/state/zip query alone is too broad for dedup (e.g., "Austin, TX 78701" could match any contributor in that ZIP code, collapsing thousands of distinct people to one address — though this may actually be the desired behavior for city-only data).

### Fix

For the loader's address dedup, use SQLAlchemy's null-safe comparisons:

```python
from sqlalchemy import or_, null

street_cond = (
    UnifiedAddress.street_1.is_(None)
    if person.address.street_1 is None
    else UnifiedAddress.street_1 == person.address.street_1
)
existing_address = session.exec(
    select(UnifiedAddress).where(
        street_cond,
        UnifiedAddress.city     == person.address.city,
        UnifiedAddress.state    == person.address.state,
        UnifiedAddress.zip_code == person.address.zip_code,
    )
).first()
```

---

## Issue 6 — HIGH: No DB-Level Unique Constraints Backing the Python Dedup

**File:** `app/core/models/tables.py`

The Python-side dedup in `_persist_transaction_from_record` is the only guard against duplicate rows. There are no database unique constraints on:

- `UnifiedPerson (first_name, last_name, state_id)` — a concurrent run or a session flush at the wrong time can insert two rows for "John Smith" before either is committed.
- `UnifiedAddress (city, state, zip_code)` — same race condition.
- `UnifiedTransactionPerson (transaction_id, person_id, role)` — if person dedup resolves to the same person for multiple roles, multiple rows with identical semantics are inserted.
- `UnifiedEntity (entity_type, normalized_name, state_id)` — entities can be created concurrently.

The only existing uniqueness on persons is the `uuid` column (a random UUID), which is not a dedup key.

### Fix

Add partial unique indexes in a new Alembic migration:

```sql
-- Persons: unique natural key per state
CREATE UNIQUE INDEX uix_persons_name_state
  ON unified_persons (lower(first_name), lower(last_name), state_id)
  WHERE organization IS NULL AND first_name IS NOT NULL AND last_name IS NOT NULL;

CREATE UNIQUE INDEX uix_persons_org_state
  ON unified_persons (lower(organization), state_id)
  WHERE organization IS NOT NULL;

-- Addresses: unique city/state/zip when no street
CREATE UNIQUE INDEX uix_addresses_city_state_zip
  ON unified_addresses (lower(city), lower(state), zip_code)
  WHERE street_1 IS NULL;

-- Transaction-person junction: one role per person per transaction
CREATE UNIQUE INDEX uix_txperson_txid_personid_role
  ON unified_transaction_persons (transaction_id, person_id, role);
```

---

## Issue 7 — MEDIUM: `UnifiedTransaction.transaction_id` Has No Unique Constraint

**File:** `app/core/models/tables.py`, `UnifiedTransaction.transaction_id`

```python
transaction_id: str | None = Field(default=None, sa_column=Column(String(500)))
```

No unique constraint. TEC assigns a unique `contributionInfoId` / `expendInfoId` to every row. Without a constraint, re-running ingest after the file-origin dedup guard fails (e.g., the FileOrigin row exists but no transactions were committed) will insert duplicate transaction rows.

### Fix

```sql
CREATE UNIQUE INDEX uix_transactions_source_id
  ON unified_transactions (transaction_id, committee_id)
  WHERE transaction_id IS NOT NULL;
```

Using `(transaction_id, committee_id)` because TEC IDs are unique only within a filer, not globally.

---

## Issue 8 — MEDIUM: Person Dedup Ignores Middle Name and Suffix

**File:** `app/core/unified_state_loader.py`, `_persist_transaction_from_record()`

The dedup query matches on `(first_name, last_name, state_id)` only. "John A. Smith Jr." and "John B. Smith Sr." would be collapsed to the same person. For political figures who share common names (multiple "Robert Johnson" state representatives) this produces incorrect entity merges.

### Fix

Include `middle_name` in the natural key with NULL-safe handling, or use a fuzzy match score rather than equality.

---

## Issue 9 — MEDIUM: `PersonRole.RECIPIENT` Is Semantically Undefined for TEC Data

**File:** `app/core/enums.py`, `app/core/processor.py`

In TEC contribution data, the "recipient" is always the filing committee — which is already captured as `UnifiedCommittee` via `filerIdent`. Having a separate `PersonRole.RECIPIENT` for a person attached to a contribution transaction is redundant and creates noise. No TEC file has a `recipientNameFirst` column.

The current code always builds a `PersonRole.RECIPIENT` from the same name fields as the contributor (Issue 1), producing a spurious `UnifiedTransactionPerson` row with role=RECIPIENT pointing to the contributor.

### Fix

Either remove `PersonRole.RECIPIENT` from `_build_participants` for TEC records, or only attach it when the raw data has an explicit recipient person field (distinct from contributor).

---

## Issue 10 — MEDIUM: `_find_or_create_person` in `_create_committee_relationships` Uses a Different Dedup Path

**File:** `app/core/unified_state_loader.py`, `_find_or_create_person()`

This method (used to build committee officer relationships) uses a case-insensitive name match (`ilike`) without state scoping, and splits the name on spaces (treating everything after the first word as a last name). "Pam Thornton Smith" becomes `first="Pam"`, `last="Thornton Smith"` — which won't match the person record from transaction ingest where she's stored as first="Pam", last="Smith". Officers are silently duplicated.

### Fix

Standardize on the same dedup key used in `_persist_transaction_from_record` and add `state_id` to the `ilike` query.

---

## Issue 11 — LOW: Fuzzy Field Match Creates Unpredictable Results

**File:** `app/core/builders.py`, `_fuzzy_field_match()`

When no explicit mapping exists for a unified field, the builder falls back to a word-overlap heuristic. A field like `contributionAmount` has 2 overlapping words with `amount` (after normalization: `contribution_amount` vs `amount` — actually only 1 word, `amount`). The heuristic requires ≥2 word overlap, so single-word unified fields like `amount` never match via fuzzy — that's fine. But multi-word unified fields like `transaction_date` will fuzzy-match the first source field containing both "transaction" and "date" words, which could pull in unrelated fields.

The fuzzy fallback also skips fields that start with `person_`, `address_`, `committee_`, `transaction_` — but this check is on the **source** field name. TEC source fields don't use these prefixes so the guard doesn't help.

### Fix

Set `strict_field_resolution=True` after adding all required explicit mappings. The fuzzy fallback should be a development tool, not production behavior. Add a warning log that fires if fuzzy is used in production, to surface unmapped fields.

---

## Structural Recommendation: Role-Specific Field Routing Table

The cleanest long-term fix for Issues 1, 2, and 9 together is to replace the flat `person_first_name` → all-roles mapping with a **role × record_type dispatch table** in the field library:

```python
# Conceptual structure
ROLE_FIELD_PREFIXES = {
    "texas": {
        "CONTRIBUTION": {
            PersonRole.CONTRIBUTOR: ("contributorNameFirst", "contributorNameLast", "contributorNameOrganization", "contributorStreet*"),
        },
        "EXPENDITURE": {
            PersonRole.PAYEE: ("payeeNameFirst", "payeeNameLast", "payeeNameOrganization", "payeeStreet*"),
        },
        "LOAN": {
            PersonRole.CONTRIBUTOR: ("lenderNameFirst", "lenderNameLast", "lenderNameOrganization", "lenderStreet*"),
        },
        "PLEDGE": {
            PersonRole.CONTRIBUTOR: ("pledgerNameFirst", "pledgerNameLast", "pledgerNameOrganization", "pledgerStreet*"),
        },
    }
}
```

`_build_participants` would receive `record_type` from raw_data and only build the roles listed for that type. All other roles return `None`.

---

## Priority Matrix

| # | Issue | Impact | Effort | Do First? |
|---|-------|--------|--------|-----------|
| 1 | Role-blind person extraction | All 4 roles get wrong person | Medium | **Yes** |
| 2 | Contribution entity assignment backwards | contributor/recipient swapped | Low | **Yes** |
| 3 | Expenditure has no detail record | Expenditures are unqueryable | Medium | Yes |
| 4 | Entity dedup not state-scoped | Cross-state entity collisions | Low | **Yes** |
| 5 | NULL address dedup fails | Thousands of duplicate addresses | Low | **Yes** |
| 6 | No DB-level unique constraints | Race conditions, silent dupes | Low | Yes |
| 7 | transaction_id not unique | Dupes on re-ingest edge cases | Low | Yes |
| 8 | Person dedup ignores middle/suffix | False merges on common names | Low | No |
| 9 | RECIPIENT role semantics undefined | Phantom transaction-person rows | Low | **Yes** |
| 10 | Officer dedup uses different path | Officers not linked to tx persons | Medium | No |
| 11 | Fuzzy match in production | Unpredictable field resolution | Low | No |

---

## Recommended Fix Order

1. **Fix role routing (Issue 1)** — everything downstream depends on getting the right person per role. Add role-scoped field names or a record_type dispatch table.
2. **Fix NULL address dedup (Issue 5)** — quick one-line fix with big impact on address table cleanliness.
3. **Fix entity state scoping (Issue 4)** — one line in `_find_entity`.
4. **Fix contribution entity direction (Issue 2)** — remove the backwards fallback.
5. **Fix RECIPIENT role semantics (Issue 9)** — stop building PersonRole.RECIPIENT for record types where the recipient is the committee.
6. **Add EXPENDITURE detail builder (Issue 3)** — add `UnifiedExpenditure` table and wire up the builder.
7. **Add DB unique constraints (Issue 6 + 7)** — Alembic migration to lock in dedup guarantees.

After these seven changes, re-ingest from scratch (truncate + reload). The existing 170K rows carry the corrupted values from before the earlier fixes and cannot be patched in place for Issues 1–5.
