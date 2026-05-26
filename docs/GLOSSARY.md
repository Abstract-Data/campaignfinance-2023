# Glossary
# Campaign Finance — Domain Terminology
# Last Updated: 2026-05-25

Reference for campaign finance domain terms used throughout the codebase, docs, and
agent prompts. Agents should consult this before interpreting field names, writing
comments, or generating user-facing messages.

---

## Campaign Finance Terms

**Amendment / Amended Filing**  
A corrected version of a previously submitted campaign finance report. The `amended`
field on `UnifiedTransaction` flags these records. Amended filings supersede the
original but both are typically retained for audit purposes.

**Campaign Committee**  
The formal organization formed to receive contributions and make expenditures on behalf
of a candidate or ballot measure. Represented by the `committee` table and
`UnifiedCommittee` model in the unified schema.

**Contribution**  
A transfer of money, goods, or services to a political committee. The primary transaction
type in the dataset. Regulated by disclosure thresholds — contributions above a certain
dollar amount require detailed contributor identification.

**Contributor**  
The person or organization making a financial contribution. Split into:
- **Individual contributor**: a natural person (`INDIVIDUAL` discriminator value)
- **Entity contributor**: a corporation, PAC, party, or other organization (`ENTITY`)

This distinction drives the `OklahomaContribution` four-level model split and the
`validate_individual_entity_discriminator` mixin in Texas validators.

**Expenditure**  
A payment made by a political committee for goods or services. Includes travel
expenditures (which carry additional fields: `departure_city`, `arrival_city`,
`transportation_type_cd`, etc.).

**Filer**  
The campaign committee that files the disclosure report. The TEC (Texas Ethics
Commission) assigns a unique **Filer ID** to each registered committee.

**Filing / Report**  
The periodic disclosure document submitted to the regulating body. A single filing
contains many transactions. The `UnifiedReport` model and `report` table track
filing-level metadata.

**Loan**  
A transfer to a campaign committee that must be repaid. Carries `loan_guaranteed_flag`
and `loan_guarantee_amount` when a third party guarantees repayment.

**PAC (Political Action Committee)**  
A type of political committee organized to raise and spend money electing or defeating
candidates. Not a candidate committee.

**Pledge**  
A committed but not-yet-transferred contribution. Treated as a transaction type in the
`transaction_type` field.

**Regulator**  
The state-level agency that collects and publishes campaign finance disclosures.
Examples: Texas Ethics Commission (TEC), Oklahoma Ethics Commission.

**TEC**  
Texas Ethics Commission — the regulator for Texas campaign finance data.

---

## Data Pipeline Terms

**Canonical Layer**  
The deduplicated, entity-resolved output tables: `canonical_entity`, `canonical_address`,
`canonical_campaign`, `canonical_name_history`. These are the gold-standard records
produced by the Splink entity resolution phase. See `docs/DATA_RELATIONSHIPS.md`.

**DriftDetector**  
`app/scrapers/drift_detector.py` — structural fingerprint comparison tool for Selenium
scrapers. Raises `DriftDetectedError` if the portal HTML structure has changed
significantly since the fixture baseline. See `docs/STATES.md`.

**Entity Resolution**  
The process of determining that two records refer to the same real-world entity (person,
organization, or address). Implemented using Splink in `app/resolve/`. Produces
`canonical_*` records. See `docs/adr/0003-ai-governance-entity-resolution.md`.

**Field Mapping**  
The translation layer between a state's raw column names and the unified field names in
`UnifiedFieldLibrary`. Defined as `StateFieldMapping` objects in
`app/core/unified_field_library.py`.

**Fuzzy Field Match**  
The third-level fallback in `UnifiedSQLModelBuilder._get_field_value()` — word-overlap
matching when a direct lookup and state field mapping both fail. Logs at DEBUG when it
fires. Set `strict_field_resolution=True` in tests to disable fuzzy matching and force
explicit mappings.

**LazyFrame**  
A Polars `LazyFrame` is a deferred computation graph. All Polars operations in the
pipeline should use `LazyFrame` and defer `.collect()` to the final output step. Never
call `.collect()` mid-pipeline then re-wrap the result.

**LoadContext**  
`app/core/load_context.py` — a dataclass created fresh on each `load_state()` invocation
that holds all mutable pipeline state: caches, file lists, and run statistics. Passed
explicitly through pipeline steps so `UnifiedStateLoader` itself is stateless and
re-entrant.

**Phase 0**  
The entity resolution preparatory phase. Runs before the main ingestion pipeline.
Code lives under `app/core/source_models/` and `scripts/loaders/`. Requires manual
gate review. See `docs/RUNBOOK.md#phase-0--resolution-manual-gate`.

**PII (Personally Identifiable Information)**  
Fields that identify or could identify a natural person: name, address, employer,
occupation. All PII fields are in the `PERSON_NAME`, `PERSON_ORGANIZATION`,
`PERSON_EMPLOYMENT`, or `PERSON_ADDRESS` `FieldCategory` groups. See
`docs/DATA_DICTIONARY.md` for the full PII classification table.

**`raw_data`**  
JSON column on `UnifiedTransaction` that stores the original source CSV row before
normalization. Contains PII. Must never be logged or included in API responses.
See `docs/DATA_DICTIONARY.md#unifiedtransactionraw_data--access-policy`.

**Splink**  
The probabilistic record linkage library used for entity resolution. Pinned to a
known-good minor version in `pyproject.toml` — update intentionally, not automatically.
API shifts between minor versions; always check Context7 docs before touching Splink
configuration.

**Unified Model**  
The normalized schema that all state data is transformed into before loading into
PostgreSQL. Core tables: `unified_transaction`, `unified_person`, `unified_committee`,
`unified_address`, `unified_report`. Defined in `app/core/models/tables.py`.
See `docs/DATA_RELATIONSHIPS.md` for the full ERD.

---

## Code Conventions

**`MONEY_TYPE`**  
The `Numeric(15, 2)` SQLAlchemy column type used for all monetary fields. Defined in
`app/core/models/tables.py`. Always use `MONEY_TYPE` — never `Numeric(15, 2)` inline.

**`SecretStr`**  
Pydantic type for credential fields. Ensures passwords and tokens never appear in
`str(repr())` of a settings object. Required for all credential-bearing fields.

**`strict_field_resolution`**  
A flag on `UnifiedSQLModelBuilder` that raises `ValueError` instead of using fuzzy
field matching. Useful in tests and new-state onboarding to confirm all mappings are
explicit.
