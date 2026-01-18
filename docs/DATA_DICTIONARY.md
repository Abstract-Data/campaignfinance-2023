# DATA_DICTIONARY.md

Field definitions, data types, and state-specific schema mappings for the campaign finance system.

> **See also:** `GLOSSARY.md` for terminology, `STATES.md` for state-specific details, `../CONTRIBUTING.md` for adding new fields.

## Unified Fields

The unified field library provides consistent field names across all states. State-specific fields are mapped to these unified fields during data processing.

### Core Transaction Fields

| Unified Field | Type | Description | Required | Validation |
|---------------|------|-------------|----------|------------|
| `transaction_id` | `IDENTIFIER` | Unique identifier for a financial transaction | No | Must be unique |
| `amount` | `CURRENCY` | Monetary amount (negative for refunds) | No | Decimal |
| `transaction_date` | `DATE` | Date when transaction occurred | No | Valid date |
| `description` | `STRING` | Purpose or description of transaction | No | Max 1000 chars |
| `transaction_type` | `CODE` | Type: contribution, expenditure, loan, pledge | No | Enum values |

### Person/Entity Fields

| Unified Field | Type | Description | Required | Validation |
|---------------|------|-------------|----------|------------|
| `person_first_name` | `STRING` | First name of individual | No | Max 100 chars |
| `person_last_name` | `STRING` | Last name of individual | No | Max 100 chars |
| `person_organization` | `STRING` | Organization name for entities | No | Max 200 chars |
| `person_employer` | `STRING` | Employer of individual | No | Max 200 chars |
| `person_occupation` | `STRING` | Occupation of individual | No | Max 200 chars |

### Address Fields

| Unified Field | Type | Description | Required | Validation |
|---------------|------|-------------|----------|------------|
| `address_street_1` | `STRING` | Primary street address | No | Max 200 chars |
| `address_street_2` | `STRING` | Secondary address line | No | Max 200 chars |
| `address_city` | `STRING` | City name | No | Max 100 chars |
| `address_state` | `CODE` | State/province code (e.g., TX, CA) | No | 2 chars |
| `address_zip` | `STRING` | Postal/ZIP code | No | Max 10 chars |

### Committee Fields

| Unified Field | Type | Description | Required | Validation |
|---------------|------|-------------|----------|------------|
| `committee_name` | `STRING` | Name of political committee | No | Max 200 chars |
| `committee_type` | `CODE` | Type: candidate, pac, party, other | No | Enum values |
| `committee_filer_id` | `IDENTIFIER` | State-assigned committee ID | No | Unique per state |

### Filing/Administrative Fields

| Unified Field | Type | Description | Required | Validation |
|---------------|------|-------------|----------|------------|
| `filed_date` | `DATE` | Date report was filed | No | Valid date |
| `amended` | `BOOLEAN` | Whether this is an amended filing | No | Default: False |

### Metadata Fields

| Unified Field | Type | Description | Required | Validation |
|---------------|------|-------------|----------|------------|
| `file_origin` | `STRING` | Source filename | Auto | Set by reader |
| `download_date` | `DATE` | When data was downloaded | Auto | Set by reader |

---

## State-Specific Mappings

### Texas (TEC)

#### Transaction Fields

| TEC Field | Unified Field | Category | Notes |
|-----------|---------------|----------|-------|
| `contributionInfoId` | `transaction_id` | Contributions | Primary key for contributions |
| `expendInfoId` | `transaction_id` | Expenditures | Primary key for expenditures |
| `loanInfoId` | `transaction_id` | Loans | Primary key for loans |
| `contributionAmount` | `amount` | Contributions | String with commas, needs parsing |
| `expendAmount` | `amount` | Expenditures | String with commas |
| `loanAmount` | `amount` | Loans | String with commas |
| `contributionDt` | `transaction_date` | Contributions | Format: YYYYMMDD |
| `expendDt` | `transaction_date` | Expenditures | Format: YYYYMMDD |
| `loanDt` | `transaction_date` | Loans | Format: YYYYMMDD |
| `contributionDescr` | `description` | Contributions | Free text |
| `expendDescr` | `description` | Expenditures | Free text |

#### Contributor Fields (Texas)

| TEC Field | Unified Field | Notes |
|-----------|---------------|-------|
| `contributorNameFirst` | `person_first_name` | For INDIVIDUAL type |
| `contributorNameLast` | `person_last_name` | Required for INDIVIDUAL |
| `contributorNameOrganization` | `person_organization` | Required for ENTITY |
| `contributorEmployer` | `person_employer` | |
| `contributorOccupation` | `person_occupation` | |
| `contributorStreetAddr1` | `address_street_1` | |
| `contributorStreetAddr2` | `address_street_2` | |
| `contributorStreetCity` | `address_city` | |
| `contributorStreetStateCd` | `address_state` | 2-letter code |
| `contributorStreetPostalCode` | `address_zip` | USA only |

#### Filer Fields (Texas)

| TEC Field | Unified Field | Notes |
|-----------|---------------|-------|
| `filerIdent` | `committee_filer_id` | Unique committee identifier |
| `filerName` | `committee_name` | Committee display name |
| `filerTypeCd` | `committee_type` | COH, GPAC, SPAC, etc. |
| `filerStreetAddr1` | `address_street_1` | Committee address |
| `filerStreetCity` | `address_city` | |
| `filerStreetStateCd` | `address_state` | |
| `filerStreetPostalCode` | `address_zip` | |

#### Administrative Fields (Texas)

| TEC Field | Unified Field | Notes |
|-----------|---------------|-------|
| `reportInfoIdent` | N/A | Links to report table |
| `receivedDt` | `filed_date` | When TEC received report |
| `filedDt` | `filed_date` | When report was filed |
| `formTypeCd` | N/A | Form type (COH, GPAC, etc.) |
| `schedFormTypeCd` | N/A | Schedule within form |
| `infoOnlyFlag` | N/A | Superseded indicator |

#### Texas-Specific Fields (Not Mapped)

| TEC Field | Description | Notes |
|-----------|-------------|-------|
| `recordType` | Always "RCPT" for contributions | Validation only |
| `itemizeFlag` | Whether contribution is itemized | Boolean Y/N |
| `travelFlag` | Has associated travel | Boolean Y/N |
| `contributorPersentTypeCd` | INDIVIDUAL or ENTITY | Determines required fields |
| `contributorPacFein` | FEC ID for out-of-state PACs | |
| `contributorOosPacFlag` | Out-of-state PAC indicator | |
| `contributorLawFirmName` | Law firm disclosure | |

---

### Oklahoma

#### Transaction Fields

| Oklahoma Field | Unified Field | Category | Notes |
|----------------|---------------|----------|-------|
| `Receipt ID` | `transaction_id` | Contributions | |
| `Expenditure ID` | `transaction_id` | Expenditures | |
| `Receipt Amount` | `amount` | Contributions | Decimal format |
| `Expenditure Amount` | `amount` | Expenditures | Decimal format |
| `Receipt Date` | `transaction_date` | Contributions | Format: MM/DD/YYYY |
| `Expenditure Date` | `transaction_date` | Expenditures | Format: MM/DD/YYYY |
| `Description` | `description` | Both | |
| `Purpose` | `description` | Expenditures | Alternative field |
| `Receipt Type` | `transaction_type` | Contributions | |
| `Expenditure Type` | `transaction_type` | Expenditures | |

#### Person Fields (Oklahoma)

| Oklahoma Field | Unified Field | Notes |
|----------------|---------------|-------|
| `First Name` | `person_first_name` | |
| `Last Name` | `person_last_name` | |
| `Employer` | `person_employer` | |
| `Occupation` | `person_occupation` | |

#### Address Fields (Oklahoma)

| Oklahoma Field | Unified Field | Notes |
|----------------|---------------|-------|
| `Address 1` | `address_street_1` | |
| `Address 2` | `address_street_2` | |
| `City` | `address_city` | |
| `State` | `address_state` | |
| `Zip` | `address_zip` | |

#### Committee Fields (Oklahoma)

| Oklahoma Field | Unified Field | Notes |
|----------------|---------------|-------|
| `Committee Name` | `committee_name` | |
| `Committee Type` | `committee_type` | |
| `Org ID` | `committee_filer_id` | |

#### Administrative Fields (Oklahoma)

| Oklahoma Field | Unified Field | Notes |
|----------------|---------------|-------|
| `Filed Date` | `filed_date` | |
| `Amended` | `amended` | Boolean |

---

## Field Categories

Fields are organized into semantic categories for easier discovery:

| Category | Description | Example Fields |
|----------|-------------|----------------|
| `TRANSACTION_ID` | Unique identifiers | `transaction_id` |
| `AMOUNT` | Monetary values | `amount` |
| `DATE` | Date/time values | `transaction_date`, `filed_date` |
| `DESCRIPTION` | Text descriptions | `description` |
| `TYPE` | Type codes | `transaction_type` |
| `PERSON_NAME` | Name components | `person_first_name`, `person_last_name` |
| `PERSON_ORGANIZATION` | Organization names | `person_organization` |
| `PERSON_ADDRESS` | Address components | `address_street_1`, `address_city` |
| `PERSON_EMPLOYMENT` | Employment info | `person_employer`, `person_occupation` |
| `COMMITTEE_NAME` | Committee names | `committee_name` |
| `COMMITTEE_TYPE` | Committee types | `committee_type` |
| `FILING_INFO` | Filing metadata | `filed_date` |
| `AMENDMENT_INFO` | Amendment flags | `amended` |

---

## Data Types

| Type | Python Type | Description | Example |
|------|-------------|-------------|---------|
| `STRING` | `str` | Free text | "John Smith" |
| `INTEGER` | `int` | Whole numbers | 12345 |
| `DECIMAL` | `Decimal` | Precise decimals | Decimal("1234.56") |
| `DATE` | `date` | Date only | date(2023, 12, 15) |
| `DATETIME` | `datetime` | Date and time | datetime(2023, 12, 15, 10, 30) |
| `BOOLEAN` | `bool` | True/False | True |
| `CURRENCY` | `Decimal` | Money amounts | Decimal("1000.00") |
| `CODE` | `str` | Enumerated values | "TX", "INDIVIDUAL" |
| `IDENTIFIER` | `str` | Unique IDs | "TX-12345" |

---

## File Categories

### Texas Files

| File Prefix | Category | Primary Fields |
|-------------|----------|----------------|
| `contribs_*.csv` | Contributions | contributionAmount, contributorName* |
| `expend_*.csv` | Expenditures | expendAmount, payeeName* |
| `filers_*.csv` | Filers/Committees | filerIdent, filerName |
| `finals_*.csv` | Final Reports | reportInfoIdent, filedDt |
| `travel_*.csv` | Travel Data | travelAmount, destination |
| `cand_*.csv` | Candidates | candidateName, officeSought |
| `debts_*.csv` | Debts | debtAmount, creditorName |
| `loans_*.csv` | Loans | loanAmount, lenderName |

### Oklahoma Files

| File Suffix | Category | Primary Fields |
|-------------|----------|----------------|
| `*ContributionLoanExtract.csv` | Contributions | Receipt Amount, First Name, Last Name |
| `*ExpenditureExtract.csv` | Expenditures | Expenditure Amount, Payee |
| `*LobbyistExpenditures.csv` | Lobby | Amount, Description |

---

## Adding New Field Mappings

To add a new field mapping:

```python
from app.states.unified_field_library import field_library, StateFieldMapping

# Add a new state field mapping
field_library.add_state_mapping(
    state="texas",
    state_field="newTexasField",
    unified_field="existing_unified_field",
    confidence=1.0,
    notes="Added for 2024 data format"
)
```

To add a new unified field:

```python
from app.states.unified_field_library import (
    field_library, FieldDefinition, FieldCategory, FieldType
)

field_library.add_unified_field(FieldDefinition(
    name="new_unified_field",
    category=FieldCategory.TRANSACTION_ID,
    field_type=FieldType.STRING,
    description="Description of the new field",
    examples=["example1", "example2"],
    validation_rules={"max_length": 100}
))
```
