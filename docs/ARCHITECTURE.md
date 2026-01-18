# ARCHITECTURE.md

System architecture, component interactions, and data flow for the campaign finance processing system.

> **See also:** `../AGENTS.md` for code patterns, `DATA_DICTIONARY.md` for field definitions, `STATES.md` for state-specific details.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        Campaign Finance Data Pipeline                            │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │    State     │    │   Generic    │    │   Unified    │    │  Production  │  │
│  │  Downloaders │───▶│ File Reader  │───▶│  Processor   │───▶│    Loader    │  │
│  │  (Selenium)  │    │   (Schema)   │    │  (Mapping)   │    │   (Batch)    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                   │                   │                   │           │
│         ▼                   ▼                   ▼                   ▼           │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  tmp/texas/  │    │    Field     │    │   SQLModel   │    │  PostgreSQL  │  │
│  │  tmp/okla/   │    │   Library    │    │  Validators  │    │   Database   │  │
│  │  (Parquet)   │    │  (Mappings)  │    │   (Pydantic) │    │   (Unified)  │  │
│  └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘  │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

This system processes campaign finance data from multiple U.S. states into a unified database schema. It handles:
- **Data Acquisition**: Automated downloading from state ethics commission portals
- **Schema Normalization**: Mapping state-specific fields to unified schema
- **Validation**: Pydantic/SQLModel-based record validation
- **Deduplication**: Address, committee, and entity caching
- **Batch Loading**: Memory-efficient database operations

## Component Roles & Responsibilities

### Core Abstract Base Classes

| Component | Purpose | Input | Output |
|-----------|---------|-------|--------|
| `StateCategoryClass` | Core data processing pipeline | Category name, config | Validated records |
| `StateFileValidation` | Record validation with Pydantic | Raw dict records | Passed/Failed tuples |
| `FileDownloader` | Data acquisition abstraction | State config | Downloaded files |
| `DBLoaderClass` | Database operations | Validated records | DB commits |
| `StateConfig` | State configuration container | State params | Config object |

### State Implementations

| Component | State | Purpose | Data Categories |
|-----------|-------|---------|-----------------|
| `TECDownloader` | Texas | Download TEC data via Selenium | contributions, expenses, filers, reports |
| `TexasCategory` | Texas | Process TEC files | contributions, expenses, filers, travel, candidates |
| `OklahomaCategory` | Oklahoma | Process OK Ethics data | contributions, expenses, lobby |

### Unified Processing Layer

| Component | Purpose | Input | Output |
|-----------|---------|-------|--------|
| `GenericFileReader` | Schema-driven file parsing | CSV/Parquet files | Normalized dicts |
| `UnifiedModelBuilder` | State→Unified mapping | State records | Unified models |
| `unified_sql_processor` | Record transformation | Raw records | SQLModel instances |
| `field_library` | Cross-state field mapping | State field names | Unified field names |

### Production Loading

| Component | Purpose | Key Features |
|-----------|---------|--------------|
| `ProductionLoader` | Batch database loading | Deduplication, progress, error recovery |
| `UnifiedStateLoader` | Full pipeline orchestration | Auto-linking, relationship creation |
| `db_manager` | Database session management | Connection pooling, transactions |

## Decision Flow

### Data Acquisition Flow

```
State Portal ──▶ Selenium Driver ──▶ Download Manager ──▶ Local Storage
     │                 │                    │                  │
     │                 ▼                    ▼                  ▼
     │          Authentication        Rate Limiting      tmp/{state}/
     │          & Navigation          & Retries         *.parquet
     │                                                        │
     └─────────── State Config ◀──────────────────────────────┘
```

### File Processing Flow

```
Raw File (CSV/Parquet)
        │
        ▼
┌───────────────────────────────┐
│     GenericFileReader         │
│  ┌─────────────────────────┐  │
│  │  Schema Definition      │  │
│  │  - Header mapping       │  │
│  │  - Type conversion      │  │
│  │  - Validation rules     │  │
│  └─────────────────────────┘  │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│     Field Library Lookup      │
│  - State field → Unified      │
│  - Type inference             │
│  - Alias resolution           │
└───────────────────────────────┘
        │
        ▼
┌───────────────────────────────┐
│   Unified SQL Processor       │
│  - Build transaction          │
│  - Build committee            │
│  - Build persons/addresses    │
│  - Build contribution/loan    │
└───────────────────────────────┘
        │
        ▼
   SQLModel Instance
```

### Validation Flow

```
Raw Record Dict
        │
        ▼
┌───────────────────────────────┐
│   StateFileValidation         │
│                               │
│   validator.validate_record() │
│         │                     │
│         ▼                     │
│   Pydantic model_validate()   │
│         │                     │
│    ┌────┴────┐                │
│    │         │                │
│ Success   Failure             │
│    │         │                │
│    ▼         ▼                │
│ ('passed', ('failed',         │
│  SQLModel)  {error})          │
└───────────────────────────────┘
        │
        ▼
   Record ID Generation
   (create_record_id)
```

### Database Loading Flow

```
Validated Records
        │
        ▼
┌───────────────────────────────┐
│    ProductionLoader           │
│                               │
│  ┌─────────────────────────┐  │
│  │   Batch Processing      │  │
│  │   (configurable size)   │  │
│  └─────────────────────────┘  │
│              │                │
│              ▼                │
│  ┌─────────────────────────┐  │
│  │   Deduplication Layer   │  │
│  │   - address_cache       │  │
│  │   - committee_cache     │  │
│  │   - entity_cache        │  │
│  │   - person_cache        │  │
│  │   - campaign_cache      │  │
│  └─────────────────────────┘  │
│              │                │
│              ▼                │
│  ┌─────────────────────────┐  │
│  │   Session Management    │  │
│  │   - Commit frequency    │  │
│  │   - Error rollback      │  │
│  └─────────────────────────┘  │
└───────────────────────────────┘
        │
        ▼
   PostgreSQL Database
```

## Data Flow

### Input Schemas

```python
# State-specific input (Texas example)
TexasContributionInput = {
    'contributionInfoId': str,      # TEC internal ID
    'contributionAmount': str,       # "1,000.00" format
    'contributionDt': str,           # "20230615" format
    'filerIdent': str,               # Committee filer ID
    'contributorNameFirst': str,
    'contributorNameLast': str,
    'contributorStreetAddr1': str,
    'contributorStreetCity': str,
    'contributorStreetStateCd': str,
    'contributorStreetPostalCode': str,
}

# Unified output schema
UnifiedTransactionOutput = {
    'id': int,                       # Auto-generated
    'transaction_id': str,           # Normalized ID
    'amount': Decimal,               # Parsed decimal
    'transaction_date': date,        # Parsed date
    'transaction_type': Enum,        # CONTRIBUTION, EXPENDITURE, etc.
    'state_id': int,                 # FK to states table
    'committee_id': str,             # FK to committees
    'file_origin_id': str,           # FK to file_origins
}
```

### State Management Between Components

```python
# Configuration flows through partial application
TEXAS_CONFIGURATION = StateConfig(
    STATE_NAME="Texas",
    STATE_ABBREVIATION="TX",
    CSV_CONFIG=CSVReaderConfig(),
)

# Partial application binds config to class
TexasCategory = partial(StateCategoryClass, config=TEXAS_CONFIGURATION)
TexasDownloader = partial(TECDownloader, config=TEXAS_CONFIGURATION)

# Runtime state in loader
class ProductionLoader:
    active_state: Optional[State]        # Current state being processed
    active_state_code: Optional[str]     # "TX", "OK", etc.
    address_cache: Dict[Tuple, Address]  # Deduplication cache
    committee_cache: Dict[str, Committee] # Keyed by filer_id
```

### Output Formats

```python
# Loader statistics output
LoaderStats = {
    'total_records': int,
    'successful_records': int,
    'failed_records': int,
    'skipped_records': int,
    'success_rate': float,           # Computed property
    'duration': float,               # Seconds
    'records_per_second': float,     # Computed property
}

# Validation error output
ValidationError = {
    'record': Dict,                  # Original record
    'error': List[Dict],             # Pydantic error details
    'validator': str,                # Validator class name
}
```

## Orchestration Pattern

**Used Pattern:** **Abstract Base Class (ABC) with Partial Application**

### Why This Pattern?

1. **State-Agnostic Core Logic**: The ABCs define the processing pipeline without state-specific knowledge
2. **Configuration Injection**: State-specific behavior comes from configuration, not inheritance
3. **Composability**: Components can be combined flexibly via partial application
4. **Testability**: Each layer can be tested in isolation with mock configs

### Pattern Implementation

```
┌─────────────────────────────────────────────────────────────────┐
│                    Abstract Base Layer                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  StateCategoryClass ◀─── StateConfig ───▶ StateFileValidation  │
│         │                     │                    │            │
│         │                     │                    │            │
│         ▼                     ▼                    ▼            │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐     │
│  │   read()    │      │ CATEGORY_   │      │ validate()  │     │
│  │   load()    │      │ TYPES       │      │ passed/     │     │
│  │   validate()│      │ CSV_CONFIG  │      │ failed      │     │
│  │   load_to_db│      │ TEMP_FOLDER │      │ records     │     │
│  └─────────────┘      └─────────────┘      └─────────────┘     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               │ partial()
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                  State-Specific Layer                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  TexasCategory = partial(StateCategoryClass,                    │
│                          config=TEXAS_CONFIGURATION)             │
│                                                                  │
│  OklahomaCategory = partial(StateCategoryClass,                 │
│                             config=OKLAHOMA_CONFIGURATION)       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Alternative Patterns Considered

| Pattern | Pros | Cons | Why Not Used |
|---------|------|------|--------------|
| Inheritance | Simple, familiar | Rigid, state logic scattered | Hard to add new states |
| Plugin System | Very flexible | Complex, over-engineered | Overkill for ~50 states |
| Dependency Injection | Testable | Requires DI framework | `inject` used minimally |
| **ABC + Partial** | **Flexible, testable, simple** | **Requires Python knowledge** | **✓ Best fit** |

## Tools & External Systems

### Tool Registry

```python
# Data Acquisition Tools
TECDownloader:           # Texas Ethics Commission portal scraper
    risk_level: LOW      # Read-only web scraping
    auth: None           # Public data
    rate_limit: 5s/req   # Respectful scraping

OklahomaDownloader:      # Oklahoma Ethics Commission
    risk_level: LOW
    auth: None
    rate_limit: 3s/req

# File Processing Tools
GenericFileReader:       # CSV/Parquet parser
    risk_level: LOW      # Read-only file operations
    formats: [csv, txt, parquet]
    encoding: [utf-8, iso-8859-1]

FieldLibrary:            # Field mapping registry
    risk_level: LOW      # In-memory lookups
    states: [texas, oklahoma, fec]

# Database Tools
db_manager:              # PostgreSQL connection manager
    risk_level: MEDIUM   # Write operations
    pool_size: 5
    timeout: 30s

ProductionLoader:        # Batch database writer
    risk_level: MEDIUM   # Bulk inserts/updates
    batch_size: 100
    commit_freq: 50

# Validation Tools
StateFileValidation:     # Pydantic/SQLModel validator
    risk_level: LOW      # No side effects
    output: pass/fail tuples
```

### External Systems

```
┌─────────────────────────────────────────────────────────────────┐
│                    External Systems                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │  Texas Ethics    │    │  Oklahoma Ethics │                   │
│  │  Commission      │    │  Commission      │                   │
│  │  (TEC Portal)    │    │  Portal          │                   │
│  │  ───────────     │    │  ───────────     │                   │
│  │  Selenium        │    │  Selenium        │                   │
│  │  WebDriver       │    │  WebDriver       │                   │
│  └──────────────────┘    └──────────────────┘                   │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │  PostgreSQL      │    │  PaperTrail      │                   │
│  │  Database        │    │  Logging         │                   │
│  │  ───────────     │    │  ───────────     │                   │
│  │  SQLModel ORM    │    │  SysLog Handler  │                   │
│  │  Connection Pool │    │  Remote Logging  │                   │
│  └──────────────────┘    └──────────────────┘                   │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │  1Password       │    │  Local File      │                   │
│  │  SDK             │    │  System          │                   │
│  │  ───────────     │    │  ───────────     │                   │
│  │  Secrets Mgmt    │    │  tmp/ storage    │                   │
│  │  Service Account │    │  Parquet files   │                   │
│  └──────────────────┘    └──────────────────┘                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Database Schema

### Entity Relationship Diagram

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│     states      │       │  file_origins   │       │unified_addresses│
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ id (PK)         │◀──┐   │ id (PK)         │       │ id (PK)         │
│ code            │   │   │ state_id (FK)   │───────│ street_1        │
│ name            │   │   │ filename        │       │ street_2        │
└─────────────────┘   │   └─────────────────┘       │ city            │
                      │                             │ state           │
                      │                             │ zip_code        │
                      │                             └─────────────────┘
                      │                                      │
                      │                                      │
┌─────────────────┐   │   ┌─────────────────┐       ┌───────┴─────────┐
│unified_committees│  │   │ unified_persons │       │ unified_entities│
├─────────────────┤   │   ├─────────────────┤       ├─────────────────┤
│ filer_id (PK)   │   │   │ id (PK)         │◀──────│ id (PK)         │
│ name            │   │   │ first_name      │       │ entity_type     │
│ committee_type  │   │   │ last_name       │       │ name            │
│ state_id (FK)   │───┘   │ organization    │       │ normalized_name │
│ address_id (FK) │       │ address_id (FK) │───────│ person_id (FK)  │
│ entity_id (FK)  │───────│ state_id (FK)   │       │ address_id (FK) │
└─────────────────┘       └─────────────────┘       └─────────────────┘
         │                         │                         │
         │                         │                         │
         ▼                         ▼                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      unified_transactions                            │
├─────────────────────────────────────────────────────────────────────┤
│ id (PK)                                                              │
│ transaction_id                                                       │
│ amount                                                               │
│ transaction_date                                                     │
│ transaction_type (enum)                                              │
│ state_id (FK) ───────────────────────────────────────────────────▶  │
│ committee_id (FK) ───────────────────────────────────────────────▶  │
│ file_origin_id (FK) ─────────────────────────────────────────────▶  │
│ campaign_id (FK) ────────────────────────────────────────────────▶  │
└─────────────────────────────────────────────────────────────────────┘
         │
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   unified_transaction_persons                        │
├─────────────────────────────────────────────────────────────────────┤
│ id (PK)                                                              │
│ transaction_id (FK) ─────────────────────────────────────────────▶  │
│ person_id (FK) ──────────────────────────────────────────────────▶  │
│ entity_id (FK) ──────────────────────────────────────────────────▶  │
│ role (enum: CONTRIBUTOR, RECIPIENT, PAYEE)                          │
│ state_id (FK)                                                        │
└─────────────────────────────────────────────────────────────────────┘
```

## Error Recovery & Fallback Logic

### Retry Strategies

```python
# Download retry with exponential backoff
class TECDownloader:
    MAX_RETRIES = 3
    RETRY_DELAYS = [5, 15, 45]  # seconds
    
    def download_with_retry(self, url):
        for attempt, delay in enumerate(self.RETRY_DELAYS):
            try:
                return self._download(url)
            except (TimeoutError, ConnectionError) as e:
                if attempt == self.MAX_RETRIES - 1:
                    raise
                logger.warning(f"Retry {attempt + 1} after {delay}s: {e}")
                time.sleep(delay)
```

### Batch Error Handling

```python
# Continue on record errors, rollback on batch failure
class ProductionLoader:
    def process_batch(self, batch, session):
        batch_success = 0
        batch_errors = 0
        
        for record in batch:
            try:
                transaction = self._process_record(record)
                session.add(transaction)
                batch_success += 1
            except Exception as e:
                # Log error but continue processing
                self.errors.append({'record': record, 'error': str(e)})
                batch_errors += 1
                continue
        
        # Commit successful records
        try:
            session.commit()
        except Exception as e:
            # Rollback entire batch on commit failure
            session.rollback()
            logger.error(f"Batch commit failed: {e}")
            raise
        
        return batch_success, batch_errors
```

### Validation Fallback

```
Record Validation
        │
        ▼
┌───────────────┐
│  Try Strict   │
│  Validation   │
└───────────────┘
        │
   ┌────┴────┐
   │         │
Success   Failure
   │         │
   ▼         ▼
Return   ┌───────────────┐
SQLModel │  Log Error    │
         │  Add to       │
         │  failed_list  │
         └───────────────┘
                │
                ▼
         ┌───────────────┐
         │  Continue     │
         │  Processing   │
         │  Next Record  │
         └───────────────┘
```

### Encoding Fallback

```python
# GenericFileReader encoding fallback
def _read_csv(self, path, encoding="utf-8"):
    try:
        yield from self._read_csv_with_encoding(path, encoding)
    except UnicodeDecodeError:
        fallback = "ISO-8859-1"
        logger.warning(f"Falling back to {fallback} for {path.name}")
        yield from self._read_csv_with_encoding(path, fallback)
```

### Escalation Paths

```
┌─────────────────────────────────────────────────────────────────┐
│                    Error Escalation Levels                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Level 1: Auto-Recovery                                         │
│  ─────────────────────                                          │
│  • Retry with backoff (network errors)                          │
│  • Encoding fallback (file errors)                              │
│  • Skip invalid record (validation errors)                      │
│                                                                  │
│  Level 2: Logged Warning                                        │
│  ────────────────────                                           │
│  • High error rate in batch (>10%)                              │
│  • Missing expected fields                                      │
│  • Duplicate key violations                                     │
│  → Logged to PaperTrail + local log                            │
│                                                                  │
│  Level 3: Batch Failure                                         │
│  ──────────────────────                                         │
│  • Database connection lost                                     │
│  • Commit failure                                               │
│  • Schema mismatch                                              │
│  → Rollback batch, log error, continue next batch              │
│                                                                  │
│  Level 4: Fatal Error                                           │
│  ───────────────────                                            │
│  • Configuration error                                          │
│  • No data files found                                          │
│  • Database unreachable after retries                           │
│  → Stop processing, raise exception, alert via logging         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Performance Considerations

### Memory Management

```python
# Batch processing prevents memory overflow
config = LoaderConfig(
    batch_size=100,           # Records per batch
    commit_frequency=50,      # Batches per commit
    max_records=None,         # Process all (or limit for testing)
)

# Caches bounded by deduplication (unique addresses << total records)
# Typical ratios:
#   - Addresses: ~30% unique (70% dedup)
#   - Committees: ~95% unique (5% dedup per file)
#   - Persons: ~60% unique (40% dedup)
```

### Database Optimization

```python
# Bulk operations instead of individual inserts
session.add_all(batch_records)  # Batch insert
session.flush()                  # Get IDs without commit
session.commit()                 # Single transaction

# Post-load deduplication for any slipped duplicates
self._dedupe_addresses(session)
self._dedupe_persons_and_entities(session)
```

### Lazy Evaluation

```python
# Polars LazyFrame for large datasets
dfs = download.dataframes()  # Returns LazyFrames
contribution_df = dfs['contribs']  # Still lazy

# Only materialize when needed
results = contribution_df.filter(
    pl.col('contributorNameLast') == "SMITH"
).collect()  # Executes here
```
