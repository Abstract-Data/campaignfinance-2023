# GLOSSARY.md

Campaign finance terminology and domain-specific concepts used throughout this codebase.

> **See also:** `DATA_DICTIONARY.md` for field definitions, `STATES.md` for state-specific terms, `../AGENTS.md` for code patterns.

## Core Concepts

### Campaign Finance Entities

| Term | Definition | Example |
|------|------------|---------|
| **Filer** | An entity registered with a state ethics commission to file campaign finance reports. Can be a committee, candidate, or PAC. | "Texans for Good Government" registered with TEC |
| **Committee** | A political organization that raises and spends money to influence elections. The primary organizational unit. | Campaign committee, PAC, party committee |
| **PAC** | Political Action Committee - an organization that pools contributions from members to donate to campaigns or spend on elections. | "Texas Oil & Gas PAC" |
| **Super PAC** | Independent expenditure-only committee that can raise unlimited funds but cannot coordinate with candidates. | "Texans for Liberty" |
| **Principal Committee** | The primary campaign committee authorized by a candidate. | "Smith for Senate Campaign" |
| **Candidate** | An individual seeking elected office who has a campaign committee filing reports. | John Smith running for State Senate |
| **Treasurer** | The individual responsible for managing a committee's finances and filing reports. Required for all committees. | Campaign treasurer signing reports |
| **Chair** | The presiding officer of a political committee. | PAC chairman |

### Transaction Types

| Term | Definition | Database Value |
|------|------------|----------------|
| **Contribution** | Money, goods, or services given TO a political committee or candidate. Inflow of resources. | `CONTRIBUTION` |
| **Expenditure** | Money spent BY a political committee or candidate. Outflow of resources. | `EXPENDITURE` |
| **In-Kind Contribution** | Non-monetary contribution of goods or services with a fair market value. | `IN_KIND` |
| **Loan** | Money borrowed by a committee, which must be repaid. Creates a debt obligation. | `LOAN` |
| **Pledge** | A promise to contribute in the future. Becomes a contribution when fulfilled. | `PLEDGE` |
| **Refund** | Return of a contribution to the donor. Represented as negative amount. | `REFUND` |
| **Transfer** | Movement of funds between affiliated committees. | `TRANSFER` |

### Person/Entity Types

| Term | Definition | Code |
|------|------------|------|
| **Individual** | A natural person making contributions or receiving payments. | `INDIVIDUAL` |
| **Entity** | An organization (corporation, union, PAC, etc.) as opposed to an individual person. | `ENTITY` |
| **Vendor/Payee** | A person or business receiving payment for goods or services from a campaign. | N/A |
| **Contributor** | The person or entity providing funds to a campaign. | N/A |

### Filing & Reporting

| Term | Definition |
|------|------------|
| **Report** | A periodic disclosure document filed with the ethics commission detailing financial activity. |
| **Filing Period** | The time span covered by a single report (e.g., semi-annual, quarterly, 30-day pre-election). |
| **Amendment** | A correction or update to a previously filed report. |
| **Late Filing** | A report submitted after the statutory deadline, often subject to penalties. |
| **Itemized** | A contribution or expenditure that exceeds the threshold requiring detailed disclosure. |
| **Unitemized** | Small contributions/expenditures below the threshold, reported only in aggregate. |

### Compliance Terms

| Term | Definition |
|------|------------|
| **Disclosure** | The legal requirement to publicly report campaign finance information. |
| **Contribution Limit** | Maximum amount an individual or entity can contribute to a candidate or committee. |
| **Aggregate Limit** | Total amount a contributor can give across all candidates/committees in an election cycle. |
| **Earmarked** | Contributions designated for a specific candidate through an intermediary. |
| **Independent Expenditure** | Spending on political communications not coordinated with any candidate. |
| **Electioneering Communication** | Broadcast ads mentioning a candidate within a certain period before an election. |

## State-Specific Terms

### Texas (TEC)

| Term | Definition | Used In |
|------|------------|---------|
| **TEC** | Texas Ethics Commission - the state agency overseeing campaign finance. | All Texas data |
| **Filer Ident** | Unique numeric identifier assigned by TEC to each registered filer. | `filerIdent` field |
| **Report Info Ident** | Unique identifier for a specific filed report. | `reportInfoIdent` field |
| **Form Type** | The specific TEC form used for filing (e.g., COH, GPAC, SPAC). | `formTypeCd` field |
| **Schedule** | Section of a report for specific transaction types (e.g., Schedule A for contributions). | `schedFormTypeCd` field |
| **COH** | Candidate/Officeholder report type. | Form type code |
| **GPAC** | General Purpose PAC report type. | Form type code |
| **SPAC** | Specific Purpose PAC report type. | Form type code |

### Oklahoma

| Term | Definition | Used In |
|------|------------|---------|
| **Guardian** | Oklahoma's online campaign finance filing system. | Portal name |
| **Org ID** | Organization identifier assigned to committees in Oklahoma. | `Org ID` field |
| **Receipt** | Oklahoma's term for contributions/income. | Receipt files |

## Technical Terms

### Data Processing

| Term | Definition |
|------|------------|
| **Unified Field** | A standardized field name that maps to state-specific field names. Enables cross-state analysis. |
| **Field Mapping** | The relationship between a state-specific field name and its unified equivalent. |
| **Normalization** | The process of converting state-specific data formats to a consistent unified format. |
| **Deduplication** | Identifying and removing duplicate records (e.g., same address appearing multiple times). |
| **Validation** | Checking records against defined rules to ensure data quality. |

### Database Concepts

| Term | Definition |
|------|------------|
| **Filer ID** | Primary key for committees in the unified database. State-assigned identifier. |
| **Transaction ID** | Unique identifier for a financial transaction within the unified system. |
| **State ID** | Foreign key linking records to their source state. |
| **File Origin** | Metadata tracking which source file a record came from. |

## Abbreviations

| Abbreviation | Full Term |
|--------------|-----------|
| **CF** | Campaign Finance |
| **PAC** | Political Action Committee |
| **TEC** | Texas Ethics Commission |
| **FEC** | Federal Election Commission |
| **COH** | Candidate/Officeholder |
| **GPAC** | General Purpose Political Action Committee |
| **SPAC** | Specific Purpose Political Action Committee |
| **FEIN** | Federal Employer Identification Number |
| **OOS** | Out of State |

## Data Format Conventions

### Date Formats by State

| State | Format | Example |
|-------|--------|---------|
| Texas | `YYYYMMDD` | `20231215` |
| Oklahoma | `MM/DD/YYYY` | `12/15/2023` |
| Unified | `YYYY-MM-DD` | `2023-12-15` |

### Amount Formats

| State | Format | Example |
|-------|--------|---------|
| Texas | String with commas | `"1,234.56"` |
| Oklahoma | Decimal | `1234.56` |
| Unified | Decimal (Python) | `Decimal("1234.56")` |

### Boolean Representations

| State | True Values | False Values |
|-------|-------------|--------------|
| Texas | `"Y"`, `"1"` | `"N"`, `"0"`, empty |
| Oklahoma | `"Yes"`, `"True"`, `"1"` | `"No"`, `"False"`, `"0"` |
| Unified | Python `True` | Python `False` |
