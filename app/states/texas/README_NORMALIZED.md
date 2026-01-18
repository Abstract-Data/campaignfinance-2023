# Texas Ethics Commission Normalized Data Models

This module provides normalized SQLModels for Texas Ethics Commission campaign finance data with separate address and person tables for efficient data linking and deduplication.

## Overview

The normalized models solve the problem of duplicate person and address data across multiple campaign finance records by:

1. **Separating addresses and persons into dedicated tables**
2. **Using hash-based deduplication** to identify identical records
3. **Creating foreign key relationships** between all record types
4. **Enabling efficient queries** across multiple campaigns and record types

## Key Benefits

- **Deduplication**: Identical persons and addresses are stored only once
- **Data Integrity**: Consistent person and address data across all records
- **Efficient Queries**: Easy to find all contributions/expenditures for a specific person
- **Geographic Analysis**: Analyze campaign finance by location
- **Relationship Mapping**: Track connections between donors, campaigns, and recipients

## Database Schema

### Core Tables

#### `tx_addresses`
Stores all unique addresses with normalization and deduplication.

**Key Fields:**
- `id`: Primary key
- `address_hash`: SHA256 hash for deduplication
- `street_addr1`, `street_addr2`: Street address
- `city`, `state_cd`, `postal_code`: Location
- `mailing_*`: Mailing address fields (if different)
- `primary_phone_*`: Phone information

#### `tx_persons`
Stores all unique persons (individuals and entities) with normalization.

**Key Fields:**
- `id`: Primary key
- `person_hash`: SHA256 hash for deduplication
- `person_type`: "INDIVIDUAL" or "ENTITY"
- `name_*`: Name fields (organization for entities, first/last for individuals)
- `employer`, `occupation`, `job_title`: Employment information
- `pac_fein`, `oos_pac_flag`: PAC-specific fields
- `law_firm_*`: Law firm associations

### Record Tables

All record tables reference the normalized person and address tables:

- `tx_contributions`: Political contributions
- `tx_expenditures`: Campaign expenditures
- `tx_loans`: Campaign loans
- `tx_pledges`: Campaign pledges
- `tx_filers`: Campaign filers and committees
- `tx_debts`: Outstanding debts
- `tx_credits`: Credits and refunds
- `tx_assets`: Assets (judicial filers)
- `tx_candidates`: Direct campaign expenditures
- `tx_travel`: Travel records
- `tx_cover_sheet1`: Cover sheet information

## Usage

### Basic Setup

```python
from sqlmodel import Session, create_engine, SQLModel
from app.states.texas.normalized_models import *
from app.states.texas.normalized_processor import NormalizedTECProcessor

# Create database
engine = create_engine("sqlite:///texas_campaign_finance.db")
SQLModel.metadata.create_all(engine)

# Create processor
with Session(engine) as session:
    processor = NormalizedTECProcessor(session)
```

### Processing CSV Data

```python
import pandas as pd

# Load raw CSV data
df = pd.read_csv("contribs_01.csv")

# Process contributions
contributions = processor.process_csv_file(df, 'contribution')

# Save to database
processor.save_records(contributions)
```

### Querying Normalized Data

#### Find all contributions from a specific person

```python
from sqlmodel import select

# Find person by name
person_stmt = select(TECPerson).where(TECPerson.name_last == "SMITH")
person = session.exec(person_stmt).first()

# Get all contributions
contrib_stmt = select(TECContribution).where(
    TECContribution.contributor_id == person.id
)
contributions = session.exec(contrib_stmt).all()

# Calculate total
total = sum(c.contribution_amount or 0 for c in contributions)
```

#### Find all people at a specific address

```python
# Find address
address_stmt = select(TECAddress).where(TECAddress.city == "AUSTIN")
addresses = session.exec(address_stmt).all()

# Get people at each address
for addr in addresses:
    people_stmt = select(TECPerson).where(TECPerson.address_id == addr.id)
    people = session.exec(people_stmt).all()
    print(f"Address: {addr.street_addr1}, People: {len(people)}")
```

#### Geographic analysis

```python
from sqlalchemy import func

# Contributions by city
stmt = select(
    TECAddress.city,
    func.sum(TECContribution.contribution_amount).label('total'),
    func.count(TECContribution.id).label('count')
).join(
    TECContribution, TECContribution.contributor_address_id == TECAddress.id
).group_by(TECAddress.city).order_by(func.sum(TECContribution.contribution_amount).desc())

results = session.exec(stmt).all()
```

## Data Processing

### Normalization

The module includes comprehensive data normalization:

- **Address normalization**: Standardizes state codes, postal codes, phone numbers
- **Person normalization**: Handles individual vs entity names, employment data
- **Hash generation**: Creates unique hashes for deduplication

### Deduplication Process

1. **Extract** person/address data from raw records
2. **Normalize** the data (standardize formats, clean values)
3. **Generate hash** for comparison
4. **Check cache** for existing records
5. **Query database** if not in cache
6. **Create new record** if not found
7. **Return ID** for foreign key relationship

### Field Mapping

The processor automatically maps CSV fields to normalized models:

- `contributorNameFirst` → `name_first`
- `contributorStreetCity` → `city`
- `contributorEmployer` → `employer`
- etc.

## File Structure

```
app/states/texas/
├── normalized_models.py          # SQLModel definitions
├── normalization_utils.py        # Data normalization functions
├── normalized_processor.py       # CSV processing and deduplication
├── example_usage.py             # Usage examples
└── README_NORMALIZED.md         # This documentation
```

## Supported Record Types

Based on the CFS documentation, the module supports:

1. **AssetData** (`ASSET`) - Assets valued at $500+ for judicial filers
2. **CandidateData** (`CAND`) - Direct campaign expenditure candidates
3. **ContributionData** (`RCPT`) - Political contributions
4. **CoverSheet1Data** (`CVR1`) - Cover sheet information and totals
5. **CreditData** (`CRED`) - Interest, credits, gains, refunds
6. **DebtData** (`DEBT`) - Outstanding judicial loans
7. **ExpendData** (`EXPN`) - Campaign expenditures
8. **FilerData** (`FILER`) - Filer index
9. **LoanData** (`LOAN`) - Campaign loans
10. **PledgeData** (`PLDG`) - Campaign pledges
11. **TravelData** (`TRVL`) - Travel outside Texas

## Performance Considerations

- **Caching**: Address and person caches reduce database queries
- **Batch processing**: Process multiple records before committing
- **Indexing**: Hash fields are indexed for fast lookups
- **Memory management**: Clear caches periodically for large datasets

## Example Analysis Queries

### Top Contributors by Location

```python
stmt = select(
    TECAddress.city,
    TECPerson.name_last,
    func.sum(TECContribution.contribution_amount).label('total')
).join(
    TECContribution, TECContribution.contributor_address_id == TECAddress.id
).join(
    TECPerson, TECContribution.contributor_id == TECPerson.id
).group_by(
    TECAddress.city, TECPerson.name_last
).order_by(
    func.sum(TECContribution.contribution_amount).desc()
).limit(20)
```

### Campaign Finance Network Analysis

```python
# Find all contributors to a specific campaign
campaign_stmt = select(TECContribution).where(
    TECContribution.filer_name == "CANDIDATE NAME"
)

# Get unique contributors
contributors = set()
for contrib in session.exec(campaign_stmt).all():
    if contrib.contributor:
        contributors.add(contrib.contributor.id)

# Find other campaigns these people contributed to
network_stmt = select(TECContribution).where(
    TECContribution.contributor_id.in_(contributors)
)
```

## Migration from Existing Models

To migrate from the existing flat-file models:

1. **Export existing data** to CSV format
2. **Use the processor** to transform to normalized models
3. **Verify data integrity** with comparison queries
4. **Update application code** to use new models

## Troubleshooting

### Common Issues

1. **Memory usage**: Clear processor cache periodically
2. **Duplicate hashes**: Check normalization logic for edge cases
3. **Foreign key errors**: Ensure all referenced records exist
4. **Performance**: Add database indexes on frequently queried fields

### Debugging

Enable SQLModel echo for query debugging:

```python
engine = create_engine("sqlite:///texas_campaign_finance.db", echo=True)
```

## Future Enhancements

- **Fuzzy matching** for similar but not identical records
- **Geocoding** for address validation and mapping
- **Entity resolution** for business name variations
- **Real-time processing** for live data feeds
- **API endpoints** for web-based queries 