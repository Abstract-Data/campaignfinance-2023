# AGENTS.md

You are an expert Python software engineer working on a campaign finance data processing system.

## Project Overview

**Tech Stack:**
- Python 3.12+ with uv for package management
- SQLModel/SQLAlchemy for ORM and database operations
- PostgreSQL for production database, SQLite for development
- Pydantic 2.x for data validation and settings management
- Polars for high-performance data processing
- Pandas for data analysis and manipulation
- Selenium for web scraping (state campaign finance portals)
- Pytest + Hypothesis for property-based testing
- Rich for CLI output and progress display

**Architecture:**
- Abstract base class (ABC) pattern for state-agnostic data processing
- Unified field library for cross-state field mapping
- Category-based data organization (contributions, expenditures, filers, etc.)
- Schema-driven file ingestion with automatic header normalization
- Dependency injection via `inject` library
- State-specific validators inheriting from SQLModel

**File Structure:**
```
app/
├── abcs/                  # Abstract base classes
│   ├── abc_category.py    # StateCategoryClass - core data processing
│   ├── abc_validation.py  # StateFileValidation - record validation
│   ├── abc_download.py    # FileDownloader - data acquisition
│   ├── abc_db_loader.py   # DBLoaderClass - database operations
│   └── abc_state_config.py # StateConfig - state configuration
├── funcs/                 # Shared utility functions
│   ├── csv_reader.py      # FileReader class
│   ├── db_loader.py       # Database loading utilities
│   ├── validator_functions.py # Record ID generation, etc.
│   └── file_exporters.py  # CSV/export utilities
├── ingest/                # File ingestion system
│   └── file_reader.py     # GenericFileReader - schema-driven parsing
├── states/                # State-specific implementations
│   ├── texas/             # Texas Ethics Commission data
│   │   ├── validators/    # Pydantic/SQLModel validators
│   │   ├── texas_downloader.py
│   │   └── texas_fields.toml
│   ├── oklahoma/          # Oklahoma campaign finance data
│   ├── unified_models.py  # Unified data models
│   ├── unified_sqlmodels.py # SQLModel implementations
│   ├── unified_field_library.py # Cross-state field mapping
│   └── unified_state_loader.py # Main loading pipeline
├── logger.py              # Logging configuration (PaperTrail + local)
└── main.py                # Entry point for data analysis
docs/                      # Technical documentation
├── ARCHITECTURE.md        # System design and components
├── DATA_DICTIONARY.md     # Field definitions and mappings
├── GLOSSARY.md           # Campaign finance terminology
├── RUNBOOK.md            # Troubleshooting and operations
├── STATES.md             # State-specific configurations
├── TESTING.md            # Test strategies and procedures
└── PRODUCTION_LOADER.md  # Production loader docs
scripts/                   # Utility and operational scripts
├── loaders/              # Data loading scripts
├── debug/                # Debug and diagnostic tools
├── analysis/             # Data analysis scripts
└── db/                   # Database management
tests/                     # Test suite (moved from root)
├── conftest.py           # Shared test fixtures
├── test_*.py             # Integration and functional tests
└── unit/                 # Unit tests (symlink to app/tests)
tmp/                       # Downloaded data files
├── texas/
├── oklahoma/
└── fec/
```

## Documentation Index

This project has comprehensive documentation. **Always consult the relevant docs before making changes.**

| Document | Purpose | When to Use |
|----------|---------|-------------|
| `AGENTS.md` | Agent instructions, code patterns, boundaries | **Start here** - Primary reference for all work |
| `docs/GLOSSARY.md` | Campaign finance terminology | When encountering domain terms you don't understand |
| `docs/DATA_DICTIONARY.md` | Field definitions, state mappings | When working with data fields or adding mappings |
| `docs/STATES.md` | State-specific configurations, quirks | When working on state-specific code or debugging state data |
| `docs/ARCHITECTURE.md` | System design, component interactions | When understanding how components fit together |
| `CONTRIBUTING.md` | How to add features, code style | When adding new states, fields, or features |
| `docs/TESTING.md` | Test strategies, evaluation criteria | When writing or debugging tests |
| `docs/RUNBOOK.md` | Operational issues, debug commands | When troubleshooting errors or performance issues |
| `CHANGELOG.md` | Version history, recent changes | When checking what's changed recently |

### Quick Reference

- **"What does this field mean?"** → `docs/DATA_DICTIONARY.md`, `docs/GLOSSARY.md`
- **"How do I add a new state?"** → `CONTRIBUTING.md`, `docs/STATES.md`
- **"Why is this failing?"** → `docs/RUNBOOK.md`, `docs/TESTING.md`
- **"How does this component work?"** → `docs/ARCHITECTURE.md`
- **"What changed recently?"** → `CHANGELOG.md`

## Commands You Must Know

**Development:**
```bash
uv sync                              # Install/sync dependencies
uv run pytest                        # Run all tests
uv run pytest -v --tb=short          # Run with verbose output
uv run pytest -k "test_name"         # Run specific test
uv run python scripts/loaders/production_loader.py   # Run production loader
uv run python app/main.py            # Run main analysis script
```

**Testing Strategy:**
```bash
# Fast feedback loop - run frequently
uv run pytest app/tests/ -x          # Stop on first failure

# Property-based testing with Hypothesis
uv run pytest app/tests/test_ingest_hypothesis.py -v --hypothesis-show-statistics

# Run specific test class
uv run pytest app/tests/test_validation_class.py::TestUserValidation -v
```

**Data Loading:**
```bash
# Production loader with presets
uv run python scripts/loaders/production_loader.py testing oklahoma_2020
uv run python scripts/loaders/production_loader.py production texas_sample
uv run python scripts/loaders/production_loader.py high_performance oklahoma_2021

# Available presets: development, testing, production, high_performance, safe
```

**Database Operations:**
```bash
# Create/recreate tables
uv run python recreate_tables.py

# Load data to PostgreSQL
uv run python load_to_postgres.py
uv run python simple_postgres_load.py
```

## Code Style Standards

### Naming Conventions
- **Functions/variables:** `snake_case` (`get_filer_data`, `contribution_amount`)
- **Classes:** `PascalCase` (`TECDownloader`, `UnifiedTransaction`)
- **Constants:** `UPPER_SNAKE_CASE` (`TEXAS_CONFIGURATION`, `MAX_RETRIES`)
- **Private methods:** `_leading_underscore` (`_extract_officer_from_record`)
- **Type aliases:** `PascalCase` (`ValidatorType`, `FileRecords`, `CategoryFileList`)

### Data Processing Patterns

**✅ GOOD - Using Abstract Base Classes and State Configuration:**
```python
from app.abcs import StateCategoryClass, StateConfig, CategoryConfig
from functools import partial

TEXAS_CONFIGURATION = StateConfig(
    STATE_NAME="Texas",
    STATE_ABBREVIATION="TX",
    CSV_CONFIG=CSVReaderConfig(),
)

TEXAS_CONFIGURATION.CATEGORY_TYPES = CategoryTypes(
    expenses=TexasCategoryConfig(DESC="expenses", VALIDATOR=validators.TECExpense),
    contributions=TexasCategoryConfig(DESC="contributions", VALIDATOR=validators.TECContribution),
)

TexasCategory = partial(StateCategoryClass, config=TEXAS_CONFIGURATION)
```

**❌ BAD - Hardcoding state-specific logic:**
```python
def load_texas_data():
    # Don't hardcode paths or state-specific logic
    files = Path("/Users/john/texas_data").glob("*.csv")
    for f in files:
        if "contrib" in f.name:
            # State-specific processing scattered everywhere
            pass
```

### Validation Patterns

**✅ GOOD - Using SQLModel validators with Pydantic:**
```python
from sqlmodel import SQLModel, Field
from typing import Optional
from pydantic import field_validator

class TECContribution(SQLModel, table=True):
    """Texas Ethics Commission contribution record."""
    id: Optional[str] = Field(default=None, primary_key=True)
    filerIdent: int
    contributionAmount: float
    contributorNameLast: Optional[str] = None
    
    @field_validator('contributionAmount', mode='before')
    @classmethod
    def parse_amount(cls, v):
        if isinstance(v, str):
            return float(v.replace(',', '').replace('$', ''))
        return v
```

**❌ BAD - Manual validation without Pydantic:**
```python
def validate_contribution(record):
    # Don't manually validate when Pydantic handles this
    if 'amount' not in record:
        raise ValueError("Missing amount")
    try:
        record['amount'] = float(record['amount'])
    except:
        return None
```

### File Reader Patterns

**✅ GOOD - Using schema-driven GenericFileReader:**
```python
from app.ingest import GenericFileReader, build_schema_for_states

# Build schema for specific states
schema = build_schema_for_states(['texas', 'oklahoma'])
reader = GenericFileReader(schema=schema, add_metadata=True, strict=False)

# Read records with automatic normalization
for record in reader.read_records(file_path):
    # Headers are normalized, types are converted
    transaction = unified_sql_processor.process_record(record, state)
```

**❌ BAD - Manual CSV parsing without schema:**
```python
import csv
with open(file_path) as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Manual header normalization and type conversion
        amount = float(row.get('Amount') or row.get('AMOUNT') or 0)
```

### Database Operations

**✅ GOOD - Using the unified database manager:**
```python
from app.states.unified_database import db_manager

with db_manager.get_session() as session:
    # Use merge for upsert behavior
    committee = self._ensure_committee(session, transaction.committee)
    session.add(transaction)
    session.commit()
```

**✅ GOOD - Batch processing with deduplication:**
```python
def _ensure_address(self, session, address):
    if not address:
        return None
    key = self._address_key(address)
    if key in self.address_cache:
        return self.address_cache[key]
    session.add(address)
    session.flush()
    self.address_cache[key] = address
    return address
```

**❌ BAD - Creating duplicates without caching:**
```python
# Don't create addresses/entities without checking cache
session.add(UnifiedAddress(**address_data))
session.commit()  # Creates duplicates!
```

## Testing Practices

### Property-Based Testing with Hypothesis
```python
from hypothesis import given, strategies as st, settings
from app.ingest.file_reader import FieldSpec, FieldType, GenericFileReader, SchemaDefinition

record_strategy = st.fixed_dictionaries({
    "Transaction ID": st.text(min_size=1, max_size=12),
    "Amount": st.decimals(min_value=Decimal("-100000"), max_value=Decimal("100000"), places=2),
    "Transaction Date": st.dates(min_value=dt.date(2000, 1, 1)).map(lambda v: v.strftime("%Y-%m-%d")),
})

@given(st.lists(record_strategy, min_size=1, max_size=5))
@settings(max_examples=25)
def test_generic_file_reader_handles_csv(tmp_path: Path, records: List[Dict[str, str]]) -> None:
    schema, headers = _build_test_schema()
    reader = GenericFileReader(schema=schema, add_metadata=True, strict=True)
    # Write test CSV and verify reader output
    ...
```

### Testing Validation Classes
```python
from app.abcs.abc_validation import StateFileValidation
from sqlmodel import SQLModel, Field

class MockModel(SQLModel):
    """Mock model for testing validation."""
    id: Optional[str] = Field(default=None)
    field: str

@given(st.dictionaries(st.text(), st.text()))
def test_validate_record(record):
    validator = StateFileValidation(validator_to_use=MockModel)
    result = validator.validate_record(record)
    assert result[0] in ['passed', 'failed']
    if result[0] == 'passed':
        assert isinstance(result[1], MockModel)
    else:
        assert isinstance(result[1], dict)
        assert 'error' in result[1]
```

### Use Fixtures for Shared Setup
```python
import pytest
from pathlib import Path

@pytest.fixture
def texas_downloader(tmp_path: Path, monkeypatch):
    """Create a Texas downloader with mocked temp folder."""
    monkeypatch.setattr(StateConfig, "TEMP_FOLDER", property(lambda self: tmp_path))
    return TECDownloader(config=TEXAS_CONFIGURATION)

@pytest.fixture
def sample_contribution_record():
    """Sample contribution record for testing."""
    return {
        'filerIdent': 12345,
        'contributionAmount': '1000.00',
        'contributorNameLast': 'SMITH',
        'contributorNameFirst': 'JOHN',
    }
```

## Git Workflow

### Commit Messages
Follow conventional commits:
```
feat: add Oklahoma campaign finance loader
fix: resolve address deduplication race condition
refactor: extract field mapping to unified library
test: add property tests for file reader
docs: update state configuration examples
```

### Before Every Commit
```bash
uv run pytest                        # All tests must pass
uv run python -m py_compile app/**/*.py  # Syntax check
```

### PR Requirements
- All tests passing
- New features include tests
- Validators use Pydantic/SQLModel patterns
- State-specific code follows ABC patterns
- Field mappings registered in unified_field_library

## Security & Best Practices

### Environment Variables
```python
# ✅ GOOD - Use pydantic-settings
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")
    
    database_url: str
    op_service_account_token: str  # 1Password SDK
    papertrail_host: str = "logs4.papertrailapp.com"
    papertrail_port: int = 33096

settings = Settings()
```

### Secrets Management
- Use 1Password SDK for production secrets (`onepassword-sdk`)
- Store local secrets in `.env` (gitignored)
- Never log sensitive data (API keys, credentials)
- Logger class sends to PaperTrail for remote monitoring

### Database Queries
```python
# ✅ GOOD - Use SQLModel/SQLAlchemy parameterized queries
from sqlmodel import select

async def get_committees_by_state(state: str) -> list[UnifiedCommittee]:
    with db_manager.get_session() as session:
        result = session.exec(
            select(UnifiedCommittee).where(UnifiedCommittee.state == state)
        )
        return result.all()

# ❌ BAD - SQL injection risk
async def get_committees_by_state(state: str):
    query = f"SELECT * FROM committees WHERE state = '{state}'"  # NEVER DO THIS
```

## Boundaries & Guardrails

### ✅ ALWAYS DO
- Use the ABC pattern for new state implementations
- Register field mappings in `unified_field_library.py`
- Write SQLModel validators for data validation
- Use `db_manager.get_session()` for database operations
- Cache addresses, committees, and entities to prevent duplicates
- Add `file_origin` and `download_date` metadata to records
- Use Polars for large data transformations
- Use Rich for CLI progress display
- Log with the `Logger` class (supports PaperTrail)

### ⚠️ ASK FIRST
- Adding new dependencies (check with `uv add`)
- Creating new state implementations
- Modifying unified field mappings (affects all states)
- Changing database schema
- Altering the ABC interfaces
- Modifying production loader configuration

### 🚫 NEVER DO
- Hardcode file paths or state-specific logic outside state modules
- Create database records without deduplication checks
- Use raw SQL strings with user input
- Swallow exceptions with bare `except:`
- Use `print()` instead of the Logger class
- Modify downloaded data files in `tmp/`
- Commit `.env` files or credentials
- Skip validation when loading data

## Documentation Maintenance

### Updating the Runbook

The `docs/RUNBOOK.md` file is a **living document** that should be updated when recurring issues are encountered. As an AI agent working on this codebase, you have a responsibility to keep operational documentation current.

**🔄 When to Update the Runbook:**

1. **Recurring Issues** - If you encounter the same error or issue **2 or more times** across different sessions, add it to `docs/RUNBOOK.md`
2. **New Error Patterns** - When you discover a new class of error with a reliable fix
3. **Improved Diagnostics** - When you find better debug commands or diagnostic approaches
4. **Changed Procedures** - When fixes or procedures change due to codebase updates

**📝 How to Update the Runbook:**

```markdown
### Issue: [Descriptive title of the issue]

**Symptoms:**
- Observable behavior or error message
- Log patterns to look for
- User-reported symptoms

**Diagnosis:**
```bash
# Commands to diagnose the issue
grep "ErrorPattern" campaign_finance_loader.log
```

**Fix:**
1. Step-by-step fix instructions
2. Include code snippets if helpful
3. Note any prerequisites or warnings

**Prevention:**
- How to avoid this issue in the future (if applicable)
```

**✅ Runbook Update Checklist:**

Before adding a new issue to the runbook, verify:
- [ ] Issue has occurred at least twice
- [ ] Root cause is understood
- [ ] Fix has been validated
- [ ] Diagnosis commands actually work
- [ ] Similar issue doesn't already exist in runbook (update existing if so)

**📍 Where to Add in Runbook:**

| Issue Type | Section in docs/RUNBOOK.md |
|------------|----------------------|
| Data loading errors | "Common Issues & Fixes" |
| Database problems | "Common Issues & Fixes" |
| New debug commands | "Debug Commands" |
| Performance issues | "Common Issues & Fixes" or "Alert Thresholds" |
| Quick one-liners | "Quick Reference" |

**Example Update:**

If you've fixed the same "field mapping not found" error twice:

```markdown
### Issue: New field appears in state data file

**Symptoms:**
- `KeyError` for unknown field name
- Warning: "No mapping found for field X"
- Records missing expected data after processing

**Diagnosis:**
```bash
# Check what headers are in the problematic file
uv run python -c "
import polars as pl
df = pl.scan_parquet('tmp/texas/contributions_01.parquet')
print(df.collect_schema().names())
"

# Compare with registered mappings
uv run python -c "
from app.states.unified_field_library import field_library
mappings = field_library.get_state_mappings('texas')
print([m.state_field for m in mappings])
"
```

**Fix:**
1. Add the new field mapping to `app/states/unified_field_library.py`
2. Rebuild the schema and test with a small batch
3. If field is state-specific, check if other states have similar fields

**Prevention:**
- Monitor state portal release notes for schema changes
- Run validation on new data files before full load
```

### Updating Other Documentation

| Document | Update When... |
|----------|---------------|
| `AGENTS.md` | New patterns, conventions, or boundaries established |
| `docs/GLOSSARY.md` | New domain terms, abbreviations, or concepts introduced |
| `docs/DATA_DICTIONARY.md` | New fields added, field mappings changed, new state data |
| `docs/STATES.md` | New state added, state portal changes, data format changes |
| `docs/ARCHITECTURE.md` | Component changes, new integrations, or flow changes |
| `CONTRIBUTING.md` | New contribution patterns, process changes |
| `docs/TESTING.md` | New test strategies, evaluation criteria, or test commands |
| `docs/RUNBOOK.md` | Recurring issues, new debug procedures, or operational changes |
| `CHANGELOG.md` | **Always** - Add entry for any feature, fix, or change |

### Documentation Quality Standards

- **Be Specific**: Include exact commands, file paths, and error messages
- **Be Current**: Remove or update outdated information
- **Be Practical**: Focus on actionable fixes, not theoretical explanations
- **Be Concise**: Runbook entries should be scannable during incidents

## Common Patterns in This Project

### State Category Processing
```python
from app.states.texas import TexasCategory

# Load and validate Texas contributions
contributions = TexasCategory("contributions")
contributions.read()
passed, failed = contributions.validate()

# Load to database
contributions.load_to_db(passed, create_table=True, limit=100000)
```

### Unified Data Processing
```python
from app.states.unified_sqlmodels import unified_sql_processor

# Process state-specific record to unified format
transaction = unified_sql_processor.process_record(
    record,
    state='texas',
    state_id=active_state.id,
    state_code='TX'
)

# Automatic field mapping via unified_field_library
```

### Progress Display with Rich
```python
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn

with Progress(
    SpinnerColumn(),
    TextColumn("[progress.description]{task.description}"),
    BarColumn(),
    console=self.console
) as progress:
    task = progress.add_task("Processing records...", total=total_records)
    for batch in batches:
        process_batch(batch)
        progress.advance(task)
```

### Error Handling in Loaders
```python
from app.logger import Logger

logger = Logger(__name__)

try:
    transaction = self._create_transaction_from_record(record)
except Exception as e:
    error_msg = f"Error processing record in {file_path.name}: {str(e)}"
    self.stats["errors"].append(error_msg)
    logger.error(error_msg)
    continue  # Continue processing other records
```

### Downloader Pattern
```python
from app.states.texas import TexasDownloader

# Initialize downloader with state config
download = TexasDownloader()

# Download data from state portal (uses Selenium)
download.download()

# Get lazy DataFrames (Polars LazyFrame)
dfs = download.dataframes()
contribution_df = dfs['contribs']
expenditure_df = dfs['expend']

# Filter and analyze
results = contribution_df.filter(
    pl.col('contributorNameLast') == "SMITH"
).collect().to_pandas()
```

### Field Library Usage
```python
from app.states.unified_field_library import field_library

# Get field mappings for a state
mappings = field_library.get_state_mappings('texas')

# Build schema for file reading
from app.ingest import build_schema_for_states
schema = build_schema_for_states(['texas', 'oklahoma'])
```
