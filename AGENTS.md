# AGENTS.md
# Version: 1.3.0
# Last Updated: 2026-05-28
# Environment: dev
# Model: claude-sonnet-4-6
# Fallback Model: claude-opus-4-6
# Project: campaignfinance
# Maintainer: John Eakin / Abstract Data

You are an expert Python data engineer working on a campaign finance data processing system.

## Agent Scope

```
Reads:      app/, scripts/, tests/, docs/, .env.example, AGENTS.md, docs/ARCHITECTURE.md
Writes:     app/, scripts/, tests/, docs/
Executes:   uv, ruff, pytest, but (GitButler — feature branches only), gh (read + PR creation)
Off-limits: .env, app/states/texas/texas.env, production PostgreSQL, downloaded data in tmp/, any other repository
```

Agents must not operate outside these boundaries without explicit human approval.

## Model Configuration

```
Primary:  claude-sonnet-4-6
Fallback: claude-opus-4-6
Notes:    Single-model workflow. Prefer the fallback for adversarial review and large-context refactors.
```

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
├── core/                  # Unified cross-state models and loaders
│   ├── models/
│   │   ├── tables.py            # All SQLModel table=True classes (incl. UnifiedExpenditure)
│   │   └── __init__.py          # Re-exports all models
│   ├── unified_models.py        # Unified data models
│   ├── unified_sqlmodels.py     # SQLModel implementations
│   ├── unified_field_library.py # Cross-state field mapping (role-scoped prefixes)
│   ├── unified_database.py      # DB manager; bootstrap() creates tables + dedup indexes
│   ├── builders.py              # UnifiedSQLModelBuilder — constructs models from raw dicts
│   ├── processor.py             # RECORD_TYPE_ROLE_MAP + detail builders per TransactionType
│   ├── unified_integration.py   # Integration helpers
│   └── unified_state_loader.py  # Main loading pipeline (NULL-safe address dedup)
├── states/                # State-specific implementations
│   ├── texas/             # Texas Ethics Commission data
│   │   ├── validators/    # Pydantic/SQLModel validators
│   │   ├── texas_downloader.py
│   │   └── texas_fields.toml
│   ├── oklahoma/          # Oklahoma campaign finance data
│   ├── ohio/              # Ohio campaign finance data
│   ├── fec/               # FEC data
│   ├── postgres_config.py       # PostgreSQL configuration
│   └── postgres_state_loader.py # PostgreSQL loading
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
├── db/                   # Database management
├── reset_and_reingest.py # Truncate stale tables + bootstrap + re-ingest Texas parquet
└── verify_ingest.py      # Post-ingest spec checklist queries (all 7 fix verifications)
tests/                     # Integration and functional tests
├── conftest.py           # Shared test fixtures
├── test_*.py             # Integration and functional tests
└── verify/               # Tool-config verification (check_agents_md.py)
app/tests/                 # Unit tests (test_ingest_hypothesis.py, etc.)
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
| `docs/GUARDRAILS.md` | Safety constraints, Signs, escalation rules | Before schema, loading, scraping, or validation work |
| `docs/DEPLOYMENTS.md` | Build, environments, rollout, rollback | When running loaders or releasing |
| `docs/DATA_RELATIONSHIPS.md` | Relationships across the unified model | When working with the unified schema |
| `docs/adr/` | Architecture / AI decision records | When making or reviewing a significant decision |
| `docs/GITBUTLER.md` | GitButler branch workflow, virtual branch conventions | When branching, stacking, or merging |
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
cf                                   # CLI entry point (app/entrypoint.py; `cf --help`)
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
# Bootstrap DB (create tables + apply 7 dedup unique indexes)
cf bootstrap

# Reset stale data and re-ingest Texas parquet files
uv run python scripts/reset_and_reingest.py           # full reset + load
uv run python scripts/reset_and_reingest.py --dry-run  # preview only
uv run python scripts/reset_and_reingest.py --skip-ingest  # truncate + bootstrap only

# Verify pipeline fixes after ingest (all 7 spec checklist queries)
uv run python scripts/verify_ingest.py
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
from app.core.unified_database import db_manager

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

## GitButler

This project uses **GitButler** for virtual-branch management. The working
branch is `gitbutler/workspace`; feature work happens on virtual branches that
GitButler tracks independently.

**🚫 NEVER DO:** Raw `git checkout -b`, `git branch`, or `git merge` for feature
work — raw git commands desync GitButler's virtual-branch state.
**✅ ALWAYS:** Use the GitButler CLI (`but`) for branch operations.

Virtual-branch workflow:
1. `but branch create <name>` — create a virtual branch
2. Work normally — GitButler tracks file changes per virtual branch
3. `but branch push <name>` — push the branch to the remote
4. Standard PR flow from there

The `.claude/hooks/block-raw-git.sh` PreToolUse hook enforces this by blocking
raw `git checkout`/`branch`/`merge`. See `docs/GITBUTLER.md` for the full
command reference and conflict-resolution patterns.

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
- **Local dev:** The `.env` file is managed by the **"Campaign Finance" 1Password environment** (account `6L2ZRMRSMFAF3IGZJYHIEZF74M`, environment `ojgs6k7robwcaldlzvddz25nmm`). Edit variables via the 1Password app or `mcp__1password__append_variables` — do not hand-edit the mounted file.
- **Production:** Use the 1Password SDK (`onepassword-sdk`) for runtime secret resolution (`app/op.py` → `OnePasswordSettings`).
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
- Use `print()` instead of the Logger class — ✅ use the `Logger` class (`app/logger.py`)
- Modify downloaded data files in `tmp/` — ✅ treat `tmp/` as read-only source data
- Commit `.env` files or credentials — ✅ use `.env` (gitignored) + the 1Password SDK
- Skip validation when loading data — ✅ always run records through the SQLModel/Pydantic validators
- Resolve secrets with the `op read` CLI — ✅ use the 1Password SDK (`app/op.py` → `OnePasswordSettings`)
- Write code against a third-party library API (Polars, SQLModel, Pydantic, Selenium) from memory — ✅ call Context7 first (`resolve-library-id` → `get-library-docs`); these libraries have version-sensitive APIs and training-data memory causes silent breakage
- Run raw `git checkout -b` / `git branch` / `git merge` for feature work — ✅ use the GitButler CLI (`but branch create/switch/push`); raw git desyncs GitButler's virtual branches (see `## GitButler`)

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
from app.core.unified_field_library import field_library
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
from app.core.unified_sqlmodels import unified_sql_processor

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
from app.core.unified_field_library import field_library

# Get field mappings for a state
mappings = field_library.get_state_mappings('texas')

# Build schema for file reading
from app.ingest import build_schema_for_states
schema = build_schema_for_states(['texas', 'oklahoma'])
```

## Documentation Priority (REQUIRED for all library code)

Before writing code that calls a third-party library, follow this order:

1. **Context7 MCP** — call `resolve-library-id` then `get-library-docs`. REQUIRED, not optional. No exceptions for "well-known" libraries.
2. **Official docs via web search** — only if Context7 does not index the library.
3. **Training-data knowledge** — last resort. Flag with `# NOTE: based on training data — verify against current docs`.

Priority libraries on this stack where this matters most: `polars` (LazyFrame API shifts between minor versions), `sqlmodel` (SQLAlchemy 2.x differences are subtle), `pydantic` v2, `selenium`, `usaddress` / `probablepeople`.

## Tool Permissions by Mode

Tool access is scoped by the active context mode. Default to `dev` if none is declared.

### dev mode
```
Reads:    app/, scripts/, tests/, docs/, .env.example
Writes:   app/, scripts/, tests/, docs/
Executes: uv, ruff, pytest, git (feature branches), gh (read + PR creation)
```
Full read/write/execute for implementation.

### review mode
```
Reads:    app/, scripts/, tests/, docs/
Writes:   NONE — produce a findings report only
Executes: git diff, git log, uv run ruff check, uv run pytest (read-only)
```
Read-only analysis. Output goes to a review document, never inline edits.

### research mode
```
Reads:    ALL project files; external docs via Context7 MCP or WebSearch/WebFetch
Writes:   docs/research/, HANDOFF.md only
Executes: grep, glob, git log, git blame (no builds, no tests)
```
Exploration only — no code, no builds, no tests.

## Goal Proposal Protocol

Agents may **propose** a goal-driven work session but may not self-activate one. To propose, write a one-line `/goal: <description>` suggestion and stop. A human activates it by setting `GOAL_MODE=1` in the shell and confirming. `/goal` is not a tool call — it is a human-gated mode. `GOAL_MODE=1` is a session variable; never commit it to `.env.example`.

## Session Management

- Read `HANDOFF.md` at session start if it exists — it carries the previous session's state, decisions, blockers, and the single next action.
- Write `HANDOFF.md` before clearing context, ending a session, or handing off. Use the `session-closer` subagent.
- Archive consumed handoffs to `.claude/handoffs/{YYYY-MM-DD}-{slug}.md`. That directory is gitignored — it holds internal session state.

## Subagent Review Order (SDD)

When work is dispatched through the Subagent-Driven Development workflow (the
`superpowers:subagent-driven-development` driver referenced by the `prompts/`
packs), the orchestrator runs a **two-stage review gate** after each `implementer`
run. The order is mandatory:

1. **`spec-reviewer`** (Stage 1) — verifies the implementation matches the spec
   exactly by reading the actual code. If it reports ❌, the `implementer` fixes
   the gaps and `spec-reviewer` re-runs. Never skip the re-review.
2. **`code-reviewer`** (Stage 2) — only dispatched **after** spec compliance
   passes. Reviews code quality, security, and correctness.

Never run `code-reviewer` while `spec-reviewer` has open issues — spec compliance
first, code quality second. See `.claude/agents/implementer.md` and
`.claude/agents/spec-reviewer.md`.

## Definition of Done

A task is complete only when **all** of the following hold:

**Code quality** — `uv run pytest` passes; `uv run ruff check .` clean; `uv run ruff format --check .` clean; type hints on all new public functions.

**Documentation hygiene** — if a behavior or policy changed, an ADR is added/updated in `docs/adr/`; if an ops procedure changed, `docs/RUNBOOK.md` is updated; if a tool or guardrail was added, this file's version is bumped; `README.md` updated if the public interface changed.

**Safety & security** — no secrets/tokens/credentials committed; no bare `print()`; no `eval()`/`exec()` on external data; no string-interpolated SQL.

**Rollback readiness** (risky operations) — rollback plan in the PR description; DB schema changes have a documented downgrade path.

## Known Template Deviations

Intentional departures from the Abstract Data project-alignment template. Do NOT "fix" these.

1. **`CLAUDE.md` is not a symlink to `AGENTS.md`.** GitNexus auto-regenerates `CLAUDE.md` as a standalone file containing only the GitNexus block (between `<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **campaignfinance** (7934 symbols, 17983 relationships, 300 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/campaignfinance/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/campaignfinance/context` | Codebase overview, check index freshness |
| `gitnexus://repo/campaignfinance/clusters` | All functional areas |
| `gitnexus://repo/campaignfinance/processes` | All execution flows |
| `gitnexus://repo/campaignfinance/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## Keeping the Index Fresh

After committing code changes, the GitNexus index becomes stale. Re-run analyze to update it:

```bash
npx gitnexus analyze
```

If the index previously included embeddings, preserve them by adding `--embeddings`:

```bash
npx gitnexus analyze --embeddings
```

To check whether embeddings exist, inspect `.gitnexus/meta.json` — the `stats.embeddings` field shows the count (0 means no embeddings). **Running analyze without `--embeddings` will delete any previously generated embeddings.**

> Claude Code users: A PostToolUse hook handles this automatically after `git commit` and `git merge`.

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->` markers). The CI `tool-config-verify.yml` workflow intentionally skips the CLAUDE.md symlink check. All project standards live in `AGENTS.md`; `CLAUDE.md` is GitNexus-only.
2. **Hook immutability (`chmod 444`) not enforced via CI.** The hook scripts cannot be made read-only through the sandboxed container — run `chmod 444 .claude/hooks/*.sh` directly from a Mac terminal after stabilizing the hook set. This is an optional hardening step, not a hard requirement.

## Anti-Pattern Warnings

Common failure modes on this project — check generated work against all of them:

1. **Don't-only lists.** Every 🚫 NEVER DO item must carry a paired ✅ alternative, or agents become over-cautious.
2. **Skipping Context7.** Writing Polars/SQLModel/Pydantic code from memory causes silent API breakage. Always `resolve-library-id` → `get-library-docs` first.
3. **Silent row drops.** Filtering or deduplicating pipeline records without logging counts hides data loss. Log row counts at every stage.
4. **Mid-pipeline `.collect()`.** Collecting a LazyFrame then re-wrapping it defeats lazy execution. Defer `.collect()` to the end.
5. **Instruction bloat.** If this file drifts well past its current size, move domain-specific content into scoped docs rather than appending.

## Notion References

Agents create and link Notion tasks using these references:

- **Tasks DB:** `collection://2e97d7f5-6298-80a5-acef-000bb9796a9d`
- **Tasks data_source_id:** `2e97d7f5-6298-80a5-acef-000bb9796a9d`
- **Projects data_source_id:** `da96d1a7-0ba0-4701-83e5-84ee1b053552`
- **Clients data_source_id:** `2e97d7f5-6298-8064-8633-000bc6c51b86`
- **Project Page:** https://www.notion.so/6a5fd2ac191d45859c0318e3acb7f018
- **Client Page:** https://www.notion.so/2f37d7f5629881bb814de76479af10db (Abstract Data — Internal)

All Notion task creation goes through the `notion-publisher` subagent, which links each task to the Project and Client above.

<!-- ════════════════════════════════════════════════════════════════════ -->
<!-- GitNexus-managed block — auto-regenerated by `npx gitnexus analyze`.   -->
<!-- Do not hand-edit between the gitnexus:start / gitnexus:end markers.    -->
<!-- ════════════════════════════════════════════════════════════════════ -->

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **campaignfinance** (5513 symbols, 12520 relationships, 286 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## When Debugging

1. `gitnexus_query({query: "<error or symptom>"})` — find execution flows related to the issue
2. `gitnexus_context({name: "<suspect function>"})` — see all callers, callees, and process participation
3. `READ gitnexus://repo/campaignfinance/process/{processName}` — trace the full execution flow step by step
4. For regressions: `gitnexus_detect_changes({scope: "compare", base_ref: "main"})` — see what your branch changed

## When Refactoring

- **Renaming**: MUST use `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` first. Review the preview — graph edits are safe, text_search edits need manual review. Then run with `dry_run: false`.
- **Extracting/Splitting**: MUST run `gitnexus_context({name: "target"})` to see all incoming/outgoing refs, then `gitnexus_impact({target: "target", direction: "upstream"})` to find all external callers before moving code.
- After any refactor: run `gitnexus_detect_changes({scope: "all"})` to verify only expected files changed.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Tools Quick Reference

| Tool | When to use | Command |
|------|-------------|---------|
| `query` | Find code by concept | `gitnexus_query({query: "auth validation"})` |
| `context` | 360-degree view of one symbol | `gitnexus_context({name: "validateUser"})` |
| `impact` | Blast radius before editing | `gitnexus_impact({target: "X", direction: "upstream"})` |
| `detect_changes` | Pre-commit scope check | `gitnexus_detect_changes({scope: "staged"})` |
| `rename` | Safe multi-file rename | `gitnexus_rename({symbol_name: "old", new_name: "new", dry_run: true})` |
| `cypher` | Custom graph queries | `gitnexus_cypher({query: "MATCH ..."})` |

## Impact Risk Levels

| Depth | Meaning | Action |
|-------|---------|--------|
| d=1 | WILL BREAK — direct callers/importers | MUST update these |
| d=2 | LIKELY AFFECTED — indirect deps | Should test |
| d=3 | MAY NEED TESTING — transitive | Test if critical path |

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/campaignfinance/context` | Codebase overview, check index freshness |
| `gitnexus://repo/campaignfinance/clusters` | All functional areas |
| `gitnexus://repo/campaignfinance/processes` | All execution flows |
| `gitnexus://repo/campaignfinance/process/{name}` | Step-by-step execution trace |

## Self-Check Before Finishing

Before completing any code modification task, verify:
1. `gitnexus_impact` was run for all modified symbols
2. No HIGH/CRITICAL risk warnings were ignored
3. `gitnexus_detect_changes()` confirms changes match expected scope
4. All d=1 (WILL BREAK) dependents were updated

## Keeping the Index Fresh

After committing code changes, the GitNexus index becomes stale. Re-run analyze to update it:

```bash
npx gitnexus analyze
```

If the index previously included embeddings, preserve them by adding `--embeddings`:

```bash
npx gitnexus analyze --embeddings
```

To check whether embeddings exist, inspect `.gitnexus/meta.json` — the `stats.embeddings` field shows the count (0 means no embeddings). **Running analyze without `--embeddings` will delete any previously generated embeddings.**

> Claude Code users: A PostToolUse hook handles this automatically after `git commit` and `git merge`.

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->

<!-- ════════════════════════════════════════════════════════════════════ -->
<!-- Cursor continual-learning notes — auto-maintained by the Cursor hook.  -->
<!-- ════════════════════════════════════════════════════════════════════ -->

## Learned User Preferences

- For multi-agent pipeline/remediation work, do not use one monolithic agent to implement wave tasks; never delegate an entire multi-wave plan to one background `generalPurpose` Task agent—interrupt if dispatched; dispatch parallel task agents plus one `*z` integrator per wave, run waves sequentially, and have the parent session merge outputs and run `/review` after each wave (orchestrator coordinates only)
- Launch an entire parallel wave as one multitask batch (one background Task agent per task brief via `run_in_background: true`), then wait for that wave to complete and merge before starting the next wave
- Integration tasks (`*z`) are single serial agents that run only after all parallel tasks in that wave are merged
- Hand each worker the full `task-*.md` brief from `prompts/data-resolution-pipeline/` or `prompts/review-remediation*/`; do not summarize or substitute a shorter brief
- Use GitButler (`but` commands) for branch/commit workflow; consolidate worker output on one phase branch with one commit per task
- Prefer incremental fixes over redesigns; describe structural or provider changes and wait for explicit approval before implementing
- Do not reiterate or summarize subagent results to the user unless asked or multi-task synthesis is required
- After `/review`, split recommended fixes across parallel agents partitioned by file ownership (same wave pattern as pipeline tasks)
- After each wave or review-remediation batch (including `review-remediation-run2`), run `/review` and loop fix → re-review until DoD PASS; defer nothing from review reports; remediation task agents run `/deslop` on owned files before wave merge
- Parallel task agents must enforce strict brief scope (create ONLY listed files; do not edit sibling-task files or `*z`-owned registries like `review/__init__.py`)
- When executing an attached `.cursor/plans/*.plan.md`, do NOT edit the plan file itself
- When plan todos already exist, mark them in_progress/completed as you work; do not recreate them

## Learned Workspace Facts

- Data-resolution pipeline is orchestrated in 11 waves (Wave 0–10) per `.cursor/plans/data_resolution_waves_d3596502.plan.md`; post-`/review` parallel fixes use `.cursor/plans/resolution_pipeline_fixes_ea1848c8.plan.md`; Phase 0 is a verification gate, not a greenfield rebuild
- Authoritative task briefs live under `prompts/data-resolution-pipeline/` (30 `task-*.md` files); parallel tasks create new files only and `*z` integration tasks own registries, `__init__.py`, and cross-task wiring
- Phase 0 (Wave 0/`0z`) code paths live under `app/core/source_models/`, `scripts/loaders/`, and `tests/resolve/`; Wave 0 agents must not create or edit `app/resolve/`; gate failures commonly involve stubbed transaction loading, incomplete DB bootstrap (`states`/`file_origins`), and missing report reconciliation; before Phase 1 run `uv run cf prepare texas` then full load via `scripts/loaders/production_loader.py`
- Post-implementation review remediation: round 1 briefs under `prompts/review-remediation/` with plan `.cursor/plans/review_remediation_waves_1d9dad56.plan.md`; active Run 2 pack under `prompts/review-remediation-run2/` (waves 1–5 + `COMPLETION.md`) with plan `.cursor/plans/review_remediation_run_2_cfd210f4.plan.md` and phase branch `remediation-r3` (`prompts/review-remediation-round2/` is a separate earlier pack); work uses `remediation/*` GitButler branches and may not be on `gitbutler/workspace`; after Wave 3c, `app/core/unified_database.py` is a thin facade over `repository.py`, `officer_repository.py`, and `analytics.py`
- Resolve pipeline code lives under `app/resolve/` (`models/`, `standardize/`, `stages/`, `review/`, `cli.py`, `run.py`, `reverse.py`); Phase 1 (1z) stages 1→2→3→7; Phase 2 (2z) adds 4→5→6 with survivorship (2d); Phase 3 is tasks 3a/3b/3c + 3z only (TASK-3d removed — feedback loop in 2b, verified by 3z); Phase 2 contracts: `candidate_pairs` → `scored_pairs` → `merge_edges` → `clusters`; Postgres e2e: `uv run --env-file .env python -m app.resolve run|publish --state texas` (resolve CLI does not auto-load `.env`); Texas stage 1 reads all TX entities in Postgres (~850k `resolution_input`), not just the latest load batch
- Stage 2 default blocking: `person_last_phonetic_zip3`, `person_first_initial_last_phonetic`, `org_normalized_zip3` (lone `person_last_phonetic` / `org_normalized` dropped); Postgres defaults to `blocking_backend=sql` in `app/resolve/blocking_sql.py` (temp table + batched block-key joins; `blocking_block_key_batch_size` default 2000); rules must be whitelisted in `_RULE_BLOCK_KEY_SQL` or use `blocking_backend=python`; SQLite/tests use Python backend; zip3 keys are lowercased in both Python and SQL paths
- `max_pairs_per_run` defaults to `2_000_000` in resolve CLI; SQL backend caps via batched DELETE (still costly if tens of millions of pairs are generated first); set `null` in run config JSON to disable cap; Stage 4 scoring uses bulk Splink `predict()` with `retain_intermediate_calculation_columns=True` for `bf_tf_adj_*` explanations, plus `compare_two_records` fallback for pairs missed by bulk blocking
- `canonical_entity.entity_type` uses shared Postgres `entitytype` enum (uppercase labels); `CanonicalEntityTypeType` in `app/resolve/models/canonical.py` binds lowercase Python `EntityType`; `build_address_occupancy_view` must compare with `lower(ce.entity_type::text) = 'person'` on Postgres (see `docs/RUNBOOK.md`)
- `app/resolve/standardize/phonetics.py` truncates metaphone codes to 50 chars (`_PHONETIC_MAX_LENGTH`) to fit Postgres `varchar(50)` on `resolution_input.*_phonetic`
- Quick Texas resolve validation: use `uv run pytest tests/resolve -m "not integration"` and phase integration tests; for prepare→load smoke use `cf prepare texas --skip-download` → `cf load texas --preset development` on a clean DB (~12.5k rows, ~9 min) — not the `testing` preset for speed; full RUNBOOK release gate is hours–days
- `_resolution_schema_models()` in `app/resolve/run.py` must register all staging models (e.g. ScoredPair, ClusterAssignment) or stages 4/6 fail on schema create; `EntityCrosswalk.match_method` must reflect merge path (`exact`, `deterministic_rule`, `probabilistic`, `approved_review`)
- Main app test gates: `uv run pytest tests app/tests --ignore=tests/resolve`; coverage gate `uv run pytest tests/ app/tests/ --cov=app --cov-fail-under=60 --ignore=tests/resolve`; resolve fast tier `uv run pytest tests/resolve -m "not integration"` (+ full `tests/resolve/` + `uv run ruff check app/resolve/` before resolve commits); CI runs resolve via `ci.yml` job `resolve-tests` (reusable `ci-resolve-tests.yml`) plus `ci-resolve-integration.yml` (both `lfs: true`); golden-set CSVs at `tests/resolve/golden/*.csv` are Git LFS tracked — fresh clones need `git lfs pull` for `test_match_quality.py`
- **Pipeline Fix Pack (2026-05-28, Fixes 1–7):** Fix 1 — `RECORD_TYPE_ROLE_MAP` in `processor.py` routes each TEC `record_type` to exactly one `PersonRole` + field-prefix (e.g. `RCPT → {CONTRIBUTOR: "contributor"}`, `EXPN → {PAYEE: "payee"}`); `unified_field_library.py` uses role-scoped unified names (`contributor_first_name`, `payee_first_name`, etc.) instead of the old role-blind `person_first_name`. Fix 2 — removed bad committee-as-contributor fallback in `_build_contribution_detail`. Fix 3 — `UnifiedExpenditure` table added to `tables.py`; `_build_expenditure_detail` added to `processor.py`; payer = committee entity, payee = vendor. Fix 4 — `_find_entity()` in `builders.py` now filters by `state_id`. Fix 5 — NULL-safe address dedup (`col.is_(None) if val is None else col == val`) in both `builders.py` and `unified_state_loader.py`. Fix 6 — explicit `PersonRole.RECIPIENT` guard in `_attach_transaction_persons`. Fix 7 — 7 partial unique indexes applied idempotently in `UnifiedDatabaseManager._apply_dedup_indexes()`, called from `bootstrap()`. Run `scripts/reset_and_reingest.py` to truncate stale data and re-ingest; run `scripts/verify_ingest.py` to confirm all fixes held.
- GitButler often blocks raw `git checkout`; virtual-branch overlap can leave fixes uncommitted — stack with `but move <child> <parent>`; if `but setup` fails with conflicts returning to `gitbutler/workspace`, use `but apply <phase-branch>` instead of raw git; for commit conflicts use `but resolve <commit-id>` → edit files (no `git add`/`git commit` during resolution) → `but resolve finish`; consolidate parallel remediation on one `remediation/*` branch and verify with `git ls-tree` before opening a PR; never commit `tmp/texas/*.parquet` — untrack with `git rm --cached` if accidentally staged

## Enforcement Gate (gate.py)

A session-scoped enforcement gate (`python3 .claude/hooks/gate.py`) runs on every Stop and on all Bash/Edit/Write/MultiEdit tool uses. The gate is wired into `.claude/settings.json` — do not remove or bypass it.

**Disposition ledger — how to close a failed or skipped check:**

A failed or skipped check blocks the session from ending until it is resolved. To dispose of one, run:

```bash
python3 .claude/hooks/gate.py dispose --check '<check-name>' --status fixed|deferred|ticket|ignore --note '<brief reason>'
```

Never skip a check silently. If a check cannot be resolved now, use `deferred` or `ticket` with a note.

**Task-critic receipt (when TASK.md exists):**

When the project has a `TASK.md`, the gate requires a task-critic verdict before the session can end. After verifying all acceptance criteria, record:

```bash
python3 .claude/hooks/gate.py task-critic --verdict PASS|BLOCK --note '<brief summary>'
```

**Dangerous-ops ask behavior:**

When the gate calls `ask` on an operation (force push, hard reset, alembic run, direct SQL write, `but config/reset/undo`, `rm -rf`), stop and surface the ask to the user. Do not attempt to bypass or retry the blocked command without explicit human confirmation.

**Acceptance criteria:**

Multi-step work MUST have acceptance criteria defined in `TASK.md` before implementation starts. If none exist, write them and get approval before proceeding. The task-critic must verify all criteria pass before recording a PASS verdict.
