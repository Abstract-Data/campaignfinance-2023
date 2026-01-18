# STATES.md

State-specific configurations, data sources, and implementation details.

> **See also:** `DATA_DICTIONARY.md` for field mappings, `../CONTRIBUTING.md` for adding new states, `RUNBOOK.md` for troubleshooting.

## Supported States

| State | Code | Status | Categories | Last Updated |
|-------|------|--------|------------|--------------|
| Texas | TX | ✅ Production | Contributions, Expenditures, Filers, Reports, Travel, Candidates, Debts, Loans | Active |
| Oklahoma | OK | ✅ Production | Contributions, Expenditures, Lobby | Active |
| Ohio | OH | 🚧 In Progress | - | - |

---

## Texas (TEC)

### Overview

| Property | Value |
|----------|-------|
| **Agency** | Texas Ethics Commission (TEC) |
| **Portal** | https://www.ethics.state.tx.us/ |
| **Data URL** | https://ethics.state.tx.us/data/search/cf/TEC_CF_CSV.zip |
| **Update Frequency** | Daily |
| **File Format** | CSV (zipped), converted to Parquet |
| **Encoding** | ISO-8859-1 (Latin-1) |

### Data Categories

| Category | File Prefix | Validator | Primary Key |
|----------|-------------|-----------|-------------|
| Contributions | `contribs_` | `TECContribution` | `contributionInfoId` |
| Expenditures | `expend_` | `TECExpense` | `expendInfoId` |
| Filers | `filers_` | `TECFilerName` | `filerIdent` |
| Reports | `finals_` | `TECFinalReport` | `reportInfoIdent` |
| Travel | `travel_` | `TECTravelData` | (composite) |
| Candidates | `cand_` | `CandidateData` | (composite) |
| Debts | `debts_` | `DebtData` | (composite) |
| Loans | `loans_` | - | `loanInfoId` |
| SPACs | `spacs_` | - | - |

### Date Format

```
YYYYMMDD (no separators)
Example: 20231215 → December 15, 2023
```

### Amount Format

```
String with commas
Example: "1,234.56" → needs parsing to Decimal
```

### Unique Quirks

1. **Encoding**: Files use ISO-8859-1, not UTF-8
2. **Date Format**: YYYYMMDD without separators requires custom parsing
3. **Amount Strings**: Commas in amounts need removal before parsing
4. **Person Type**: `contributorPersentTypeCd` determines required fields:
   - `INDIVIDUAL` → requires `contributorNameLast`
   - `ENTITY` → requires `contributorNameOrganization`
5. **Null Values**: Empty strings AND "null" text need handling
6. **Large Files**: Contribution files can exceed 1GB, recommend Polars for reading
7. **File Consolidation**: Multiple files per category need consolidation

### Configuration

```python
# app/states/texas/__init__.py
TEXAS_CONFIGURATION = StateConfig(
    STATE_NAME="Texas",
    STATE_ABBREVIATION="TX",
    CSV_CONFIG=CSVReaderConfig(),
)

TEXAS_CONFIGURATION.CATEGORY_TYPES = CategoryTypes(
    expenses=TexasCategoryConfig(DESC="expenses", VALIDATOR=validators.TECExpense),
    contributions=TexasCategoryConfig(DESC="contributions", VALIDATOR=validators.TECContribution),
    filers=TexasCategoryConfig(DESC="filers", VALIDATOR=validators.TECFilerName),
    reports=TexasCategoryConfig(DESC="reports", VALIDATOR=validators.TECFinalReport),
    travel=TexasCategoryConfig(DESC="travel", VALIDATOR=validators.TECTravelData),
    candidates=TexasCategoryConfig(DESC="candidates", VALIDATOR=validators.CandidateData),
    debts=TexasCategoryConfig(DESC="debts", VALIDATOR=validators.DebtData),
)
```

### Download Process

```python
from app.states.texas import TexasDownloader

# Download (uses Selenium)
download = TexasDownloader()
download.download()

# Get DataFrames
dfs = download.dataframes()
contribution_df = dfs['contribs']  # Polars LazyFrame
```

### Field Mappings

See `DATA_DICTIONARY.md` for complete field mappings.

Key mappings:
- `filerIdent` → `committee_filer_id`
- `contributionAmount` → `amount`
- `contributionDt` → `transaction_date`
- `contributorNameFirst/Last` → `person_first_name/last_name`

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Encoding errors | ISO-8859-1 file | Use `encoding='ISO-8859-1'` |
| Date parse fails | YYYYMMDD format | Custom date parser |
| Missing name | Wrong person type | Check `contributorPersentTypeCd` |
| Validation fails | Empty "null" strings | Clear before validation |

---

## Oklahoma

### Overview

| Property | Value |
|----------|-------|
| **Agency** | Oklahoma Ethics Commission |
| **Portal** | https://guardian.ok.gov/ (Guardian System) |
| **Update Frequency** | Weekly |
| **File Format** | CSV |
| **Encoding** | UTF-8 |

### Data Categories

| Category | File Suffix | Validator | Primary Key |
|----------|-------------|-----------|-------------|
| Contributions | `ContributionLoanExtract` | `OKContribution` | `Receipt ID` |
| Expenditures | `ExpenditureExtract` | `OKExpenditure` | `Expenditure ID` |
| Lobby | `LobbyistExpenditures` | `OKLobbyistExpenditure` | (composite) |

### Date Format

```
MM/DD/YYYY
Example: 12/15/2023 → December 15, 2023
```

### Amount Format

```
Decimal (no formatting)
Example: 1234.56
```

### Unique Quirks

1. **Simpler Format**: More straightforward than Texas
2. **Standard Encoding**: UTF-8 works correctly
3. **Readable Dates**: Standard MM/DD/YYYY format
4. **Clean Amounts**: No commas in numeric values
5. **Lobby Data**: Separate files for lobbyist expenditures
6. **Historical Gaps**: Some older data may have missing fields

### Configuration

```python
# app/states/oklahoma/oklahoma.py (typical structure)
OKLAHOMA_CONFIGURATION = StateConfig(
    STATE_NAME="Oklahoma",
    STATE_ABBREVIATION="OK",
    CSV_CONFIG=CSVReaderConfig(),
)
```

### Field Mappings

See `DATA_DICTIONARY.md` for complete field mappings.

Key mappings:
- `Org ID` → `committee_filer_id`
- `Receipt Amount` / `Expenditure Amount` → `amount`
- `Receipt Date` / `Expenditure Date` → `transaction_date`
- `First Name` / `Last Name` → `person_first_name/last_name`

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Missing fields | Historical data | Handle optional fields |
| Duplicate records | Amended filings | Check `Amended` flag |

---

## Ohio (In Progress)

### Overview

| Property | Value |
|----------|-------|
| **Agency** | Ohio Secretary of State |
| **Portal** | https://www.ohiosos.gov/ |
| **Status** | 🚧 Implementation in progress |

### Planned Categories

- Contributions
- Expenditures
- Committees/Filers

---

## Adding a New State

See `CONTRIBUTING.md` for detailed instructions. Quick checklist:

1. [ ] Create directory: `app/states/{state_name}/`
2. [ ] Create `__init__.py` with `StateConfig`
3. [ ] Create validators in `validators/` subdirectory
4. [ ] Add field mappings to `unified_field_library.py`
5. [ ] Create downloader (if web scraping needed)
6. [ ] Add tests
7. [ ] Update this file (STATES.md)
8. [ ] Update DATA_DICTIONARY.md

---

## State Portal Notes

### Portal Stability

| State | Stability | Notes |
|-------|-----------|-------|
| Texas (TEC) | ⚠️ Sometimes slow | Error pages occasionally, retry logic needed |
| Oklahoma | ✅ Generally stable | Consistent availability |

### Rate Limiting

| State | Recommended Delay | Notes |
|-------|-------------------|-------|
| Texas | 5 seconds/request | Respectful scraping |
| Oklahoma | 3 seconds/request | Standard delay |

### Authentication

| State | Auth Required | Method |
|-------|--------------|--------|
| Texas | No | Public data, direct download |
| Oklahoma | No | Public data |

---

## Data Quality Notes

### Texas Data Quality

| Category | Quality | Common Issues |
|----------|---------|---------------|
| Contributions | ⭐⭐⭐⭐ | Good, some encoding issues |
| Expenditures | ⭐⭐⭐⭐ | Good, similar to contributions |
| Filers | ⭐⭐⭐⭐⭐ | Excellent, consistent |
| Reports | ⭐⭐⭐⭐ | Good |
| Travel | ⭐⭐⭐ | Some missing fields |
| Candidates | ⭐⭐⭐ | Occasional inconsistencies |

### Oklahoma Data Quality

| Category | Quality | Common Issues |
|----------|---------|---------------|
| Contributions | ⭐⭐⭐⭐ | Good overall |
| Expenditures | ⭐⭐⭐⭐ | Good overall |
| Lobby | ⭐⭐⭐ | Some historical gaps |

---

## File Size Estimates

### Texas

| Category | Approx. Size | Record Count |
|----------|--------------|--------------|
| Contributions | 500MB - 1GB | 2-4 million |
| Expenditures | 300MB - 600MB | 1-2 million |
| Filers | 10MB - 50MB | 50,000+ |
| Reports | 100MB - 200MB | 500,000+ |

### Oklahoma

| Category | Approx. Size | Record Count |
|----------|--------------|--------------|
| Contributions | 50MB - 100MB | 200,000+ |
| Expenditures | 50MB - 100MB | 200,000+ |
| Lobby | 10MB - 20MB | 50,000+ |

---

## Scheduled Updates

| State | Recommended Update Schedule |
|-------|----------------------------|
| Texas | Weekly (Sunday night) |
| Oklahoma | Weekly (Sunday night) |

### Update Script Example

```bash
#!/bin/bash
# weekly_update.sh

echo "Downloading Texas data..."
uv run python -c "from app.states.texas import TexasDownloader; TexasDownloader().download()"

echo "Loading Texas data..."
uv run python production_loader.py production texas_full

echo "Update complete!"
```
