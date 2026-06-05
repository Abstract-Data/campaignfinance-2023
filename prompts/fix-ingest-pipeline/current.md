# Prompt: Fix Ingest Pipeline Data Quality Issues

## Context

You are working on a Python campaign finance data pipeline built with FastAPI + SQLModel + Pydantic v2 + async SQLAlchemy + Alembic. The project root is the working directory. Key files:

- `app/core/unified_field_library.py` — `UnifiedFieldLibrary`: maps state-specific column names to unified field names
- `app/core/builders.py` — `UnifiedSQLModelBuilder`: constructs SQLModel objects from raw dicts
- `app/core/processor.py` — `UnifiedSQLDataProcessor` + `_build_participants()`: orchestrates record → transaction conversion
- `app/core/unified_state_loader.py` — `UnifiedStateLoader`: batch ingest, session management, dedup guards
- `app/core/models/tables.py` — all SQLModel `table=True` classes
- `app/core/enums.py` — `PersonRole`, `TransactionType`, etc.

A detailed analysis of all known issues is in `PIPELINE_REVIEW.md`. Read it before starting.

---

## Objective

Implement all fixes from `PIPELINE_REVIEW.md` in priority order. The goal is that after re-ingesting Texas TEC parquet files every transaction has:

- Correct `transaction_date`, `amount`, and `transaction_type`
- `committee_id` FK populated and pointing to the correct `UnifiedCommittee`
- Exactly one `UnifiedTransactionPerson` row per external party (contributor, payee, lender, etc.) — no phantom RECIPIENT/CANDIDATE duplicates
- `UnifiedContribution` with correct `contributor_entity_id` (the person/org who gave) and `recipient_entity_id` (the committee that received)
- `UnifiedExpenditure` detail records for expenditure transactions
- No duplicate `UnifiedPerson` or `UnifiedAddress` rows
- All `UnifiedEntity` records scoped to the correct state

---

## Fix 1 — Role-Scoped Field Routing (CRITICAL)

**Problem:** `_build_participants()` calls `build_person(raw_data, role)` for all four roles. The field library maps every role-specific TEC column (`contributorNameFirst`, `payeeNameFirst`, `lenderNameFirst`, `pledgerNameFirst`, etc.) to the same unified field name `person_first_name`. So all four roles resolve to the same person from the same raw dict.

**Required changes:**

### 1a. Add a `RECORD_TYPE_ROLE_MAP` in `processor.py`

Create a dispatch table that maps each TEC `record_type` to exactly which `PersonRole` values should be built, and which field-prefix group to use for each role:

```python
# Maps TEC record_type → {PersonRole: field_prefix}
# Only roles present in the dict get built for that record type.
RECORD_TYPE_ROLE_MAP: dict[str, dict[PersonRole, str]] = {
    "RCPT":   {PersonRole.CONTRIBUTOR: "contributor"},   # contributions
    "EXPN":   {PersonRole.PAYEE: "payee"},               # expenditures
    "LOAN":   {PersonRole.CONTRIBUTOR: "lender"},        # loans (lender = contributor)
    "PLEDGE": {PersonRole.CONTRIBUTOR: "pledger"},       # pledges
    "CREDIT": {PersonRole.CONTRIBUTOR: "payor"},         # credits/refunds
    "TRAVEL": {PersonRole.PAYEE: "traveller"},           # travel (traveller = payee)
    "ASSET":  {},                                        # assets have no external person
}
```

For record types not in this map, fall back to building only `PersonRole.CONTRIBUTOR` using the `contributor` prefix.

### 1b. Add role-scoped unified field names to `UnifiedFieldLibrary`

Add the following unified field name set (these are the target names the builder will look up for each prefix):

```
{prefix}_first_name        → person_first_name  (role-scoped)
{prefix}_last_name         → person_last_name   (role-scoped)
{prefix}_organization      → person_organization (role-scoped)
{prefix}_street_1          → address_street_1   (role-scoped)
{prefix}_street_2          → address_street_2
{prefix}_city              → address_city
{prefix}_state             → address_state
{prefix}_zip               → address_zip
```

Add Texas mappings for each prefix:

| Prefix       | TEC source columns                                                                 |
|--------------|------------------------------------------------------------------------------------|
| `contributor`| `contributorNameFirst/Last/Organization`, `contributorStreetAddr1/2/City/StateCd/PostalCode` |
| `payee`      | `payeeNameFirst/Last/Organization`, `payeeStreetAddr1/2/City/StateCd/PostalCode`  |
| `lender`     | `lenderNameFirst/Last/Organization`, `lenderStreetAddr1/2/City/StateCd/PostalCode`|
| `pledger`    | `pledgerNameFirst/Last/Organization`, `pledgerStreetAddr1/2/City/StateCd/PostalCode` |
| `payor`      | `payorNameFirst/Last/Organization`                                                 |
| `traveller`  | `travellerNameFirst`, `travellerNameLast`                                          |

### 1c. Update `build_person()` to accept a `field_prefix` parameter

Change the signature to:
```python
def build_person(self, raw_data: dict[str, Any], role: PersonRole, field_prefix: str = "contributor") -> UnifiedPerson | None:
```

Inside, replace all calls to `_get_field_value(raw_data, "person_first_name")` with `_get_field_value(raw_data, f"{field_prefix}_first_name")`, and similarly for all other person/address fields.

### 1d. Update `_build_participants()` to use the dispatch table

```python
def _build_participants(
    builder: UnifiedSQLModelBuilder, raw_data: dict[str, Any]
) -> dict[PersonRole, UnifiedPerson | None]:
    record_type = raw_data.get("record_type", "").upper()
    role_map = RECORD_TYPE_ROLE_MAP.get(record_type, {PersonRole.CONTRIBUTOR: "contributor"})
    result: dict[PersonRole, UnifiedPerson | None] = {role: None for role in PersonRole}
    for role, prefix in role_map.items():
        result[role] = builder.build_person(raw_data, role, field_prefix=prefix)
    return result
```

---

## Fix 2 — Correct Contribution Entity Direction

**Problem:** In `_build_contribution_detail()`, if `contributor_entity` is None, the code falls back to `committee.entity` as the contributor. This makes the committee appear to donate to itself.

**Required change in `processor.py`, `_build_contribution_detail()`:**

Remove this block:
```python
if not contributor_entity and committee and committee.entity:
    contributor_entity = committee.entity
```

Replace with:
```python
if not contributor_entity:
    return  # anonymous/unknown contributor — skip creating UnifiedContribution
```

Also remove the RECIPIENT person fallback:
```python
# REMOVE this block:
if not recipient_entity and recipient and recipient.entity:
    recipient_entity = recipient.entity
```

The committee entity from `_entity_context` is the correct and only recipient for contributions. If `recipient_entity` is still None after `_entity_context`, skip creating the detail record.

---

## Fix 3 — Add UnifiedExpenditure Table and Detail Builder

**Problem:** `DETAIL_BUILDERS` has no entry for `TransactionType.EXPENDITURE`, so expenditure transactions have no detail record.

### 3a. Add `UnifiedExpenditure` to `app/core/models/tables.py`

```python
class UnifiedExpenditure(SQLModel, table=True):
    """Normalized expenditure detail extracted from transactions."""

    __tablename__ = "unified_expenditures"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()), unique=True, index=True)
    transaction_id: int = Field(
        sa_column=Column(Integer, ForeignKey("unified_transactions.id"), unique=True)
    )
    payer_entity_id: int = Field(foreign_key="unified_entities.id")    # the committee
    payee_entity_id: int = Field(foreign_key="unified_entities.id")    # vendor/person paid
    state_id: int | None = Field(default=None, foreign_key="states.id")
    amount: Decimal | None = Field(default=None, sa_column=Column(MONEY_TYPE))
    expenditure_date: date | None = Field(default=None, index=True)
    expenditure_type: str | None = Field(default=None, sa_column=Column(String(200)))
    description: str | None = Field(default=None, sa_column=Column(Text))
    metadata_json: str | None = Field(default=None, sa_column=Column(Text))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction: "UnifiedTransaction" = Relationship(back_populates="expenditure")
    payer: "UnifiedEntity" = Relationship(
        sa_relationship_kwargs={"foreign_keys": "UnifiedExpenditure.payer_entity_id"}
    )
    payee: "UnifiedEntity" = Relationship(
        sa_relationship_kwargs={"foreign_keys": "UnifiedExpenditure.payee_entity_id"}
    )
    state: State | None = Relationship(back_populates="expenditures")
```

Add the back-reference to `UnifiedTransaction`:
```python
expenditure: Optional["UnifiedExpenditure"] = Relationship(
    back_populates="transaction", sa_relationship_kwargs={"uselist": False}
)
```

Add to `State.expenditures` relationship list.

### 3b. Add `_build_expenditure_detail()` to `processor.py`

```python
def _build_expenditure_detail(
    transaction: UnifiedTransaction,
    builder: UnifiedSQLModelBuilder,
    raw_data: dict[str, Any],
    ctx: DetailContext,
) -> None:
    committee = ctx["committee"]
    payee_entity = ctx["contributor_entity"]  # contributor slot holds the payee (after Fix 1)
    payer_entity = committee.entity if committee and committee.entity else None
    if not (payer_entity and payee_entity):
        return
    transaction.expenditure = UnifiedExpenditure(
        transaction=transaction,
        payer=payer_entity,
        payee=payee_entity,
        amount=transaction.amount,
        expenditure_date=transaction.transaction_date,
        expenditure_type=builder._get_field_value(raw_data, "expenditure_type"),
        description=transaction.description,
        state_id=builder.state_id,
    )
```

Register it:
```python
DETAIL_BUILDERS[TransactionType.EXPENDITURE] = _build_expenditure_detail
```

### 3c. Generate and apply an Alembic migration

```bash
alembic revision --autogenerate -m "add unified_expenditures table"
alembic upgrade head
```

---

## Fix 4 — State-Scope Entity Deduplication

**File:** `app/core/builders.py`, `_find_entity()`

Add `state_id` to the WHERE clause:

```python
query = select(UnifiedEntity).where(
    UnifiedEntity.entity_type == entity_type,
    UnifiedEntity.normalized_name == normalized_name,
    UnifiedEntity.state_id == self.state_id,  # ← add this line
)
```

---

## Fix 5 — NULL-Safe Address Deduplication

**File:** `app/core/unified_state_loader.py`, `_persist_transaction_from_record()`

Replace the address dedup query with NULL-safe comparisons:

```python
if person.address:
    addr = person.address

    def _null_safe(col, val):
        return col.is_(None) if val is None else col == val

    existing_address = session.exec(
        select(UnifiedAddress).where(
            _null_safe(UnifiedAddress.street_1, addr.street_1),
            _null_safe(UnifiedAddress.city,     addr.city),
            _null_safe(UnifiedAddress.state,    addr.state),
            _null_safe(UnifiedAddress.zip_code, addr.zip_code),
        )
    ).first()

    if existing_address:
        person.address_id = existing_address.id
        person.address = existing_address
    else:
        session.add(addr)
        session.flush()
        person.address_id = addr.id
```

Apply the same pattern in `builders.py`, `_find_address_by_fields()`.

---

## Fix 6 — Remove Phantom RECIPIENT Role from Transaction Persons

**File:** `app/core/processor.py`, `_attach_transaction_persons()`

After Fix 1 lands, `PersonRole.RECIPIENT` will be `None` for all TEC records (since the committee is the implicit recipient). The `_attach_transaction_persons` loop already skips `None` persons. No additional change needed here — verify that no `PersonRole.RECIPIENT` rows are inserted after Fix 1.

If `PersonRole.RECIPIENT` is still used elsewhere, add an explicit guard:

```python
# In _attach_transaction_persons — skip RECIPIENT; committees are captured via committee_id FK
if role == PersonRole.RECIPIENT:
    continue
```

---

## Fix 7 — Add DB-Level Unique Constraints

Create a new Alembic migration:

```bash
alembic revision -m "add dedup unique indexes"
```

In the migration's `upgrade()`:

```python
op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_persons_name_state
    ON unified_persons (lower(first_name), lower(last_name), state_id)
    WHERE organization IS NULL
      AND first_name IS NOT NULL
      AND last_name IS NOT NULL;
""")

op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_persons_org_state
    ON unified_persons (lower(organization), state_id)
    WHERE organization IS NOT NULL;
""")

op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_addresses_city_state_zip_nostreet
    ON unified_addresses (lower(city), lower(state), zip_code)
    WHERE street_1 IS NULL;
""")

op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_addresses_full
    ON unified_addresses (lower(street_1), lower(city), lower(state), zip_code)
    WHERE street_1 IS NOT NULL;
""")

op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_txperson_txid_personid_role
    ON unified_transaction_persons (transaction_id, person_id, role);
""")

op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_transactions_source_id
    ON unified_transactions (transaction_id, committee_id)
    WHERE transaction_id IS NOT NULL;
""")

op.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uix_entities_type_name_state
    ON unified_entities (entity_type, normalized_name, state_id)
    WHERE state_id IS NOT NULL;
""")
```

---

## Implementation Checklist

Before marking complete, verify each fix:

- [ ] **Fix 1:** Run a smoke test on a single contribution row. Confirm exactly one `UnifiedTransactionPerson` row (role=CONTRIBUTOR). Confirm `PersonRole.RECIPIENT`, `PersonRole.PAYEE`, `PersonRole.CANDIDATE` all return `None` from `_build_participants` for a RCPT record.
- [ ] **Fix 1 (expenditure):** Run a smoke test on an expenditure row. Confirm exactly one `UnifiedTransactionPerson` row (role=PAYEE). Confirm the PAYEE person has `payeeNameFirst`/`payeeNameLast` — NOT `contributorNameFirst`.
- [ ] **Fix 2:** Verify that `UnifiedContribution.contributor_entity_id` → a person/org entity; `UnifiedContribution.recipient_entity_id` → a committee entity. Run: `SELECT c.id, pe.entity_type as contrib_type, re.entity_type as recip_type FROM unified_contributions c JOIN unified_entities pe ON c.contributor_entity_id = pe.id JOIN unified_entities re ON c.recipient_entity_id = re.id LIMIT 10;` — `contrib_type` should be `person` or `organization`; `recip_type` should be `committee`.
- [ ] **Fix 3:** After re-ingest, run `SELECT COUNT(*) FROM unified_expenditures;` — should be non-zero.
- [ ] **Fix 4:** Run `SELECT state_id, COUNT(*) FROM unified_entities GROUP BY state_id;` — confirm all entities are state-scoped.
- [ ] **Fix 5:** Run `SELECT COUNT(*) FROM unified_addresses;` before and after. Address count should drop substantially (was near transaction count; should converge toward distinct city/state/zip combinations).
- [ ] **Fix 6:** Run `SELECT role, COUNT(*) FROM unified_transaction_persons GROUP BY role;` — `recipient` count should be 0 or very low.
- [ ] **Fix 7:** Attempt to manually insert a duplicate person; confirm the unique index rejects it.
- [ ] **All fixes:** Truncate all transaction-related tables, re-ingest a full Texas file, confirm `SELECT COUNT(*) FROM unified_transactions WHERE transaction_date IS NULL;` returns 0.

---

## Notes

- Run impact analysis (`gitnexus_impact`) on `build_person`, `_build_participants`, `_get_field_value`, and `_find_entity` before editing — per project CLAUDE.md requirements.
- Run `gitnexus_detect_changes()` before committing.
- After committing, run `npx gitnexus analyze` to refresh the index (or `npx gitnexus analyze --embeddings` if embeddings exist — check `.gitnexus/meta.json`).
- The existing data in the DB carries pre-fix corruption. After all fixes are applied, truncate `unified_transaction_persons`, `unified_transactions`, `unified_persons`, `unified_addresses`, `unified_entities`, `unified_contributions`, `unified_expenditures`, `file_origins` and re-ingest.
