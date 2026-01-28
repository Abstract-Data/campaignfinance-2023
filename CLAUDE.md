# CLAUDE.md

This file provides guidance for Claude Code (claude.ai/code) when working with this repository.

## Project Summary

Campaign finance data processing system for aggregating, normalizing, and analyzing campaign finance data from multiple US states. Currently supports Texas and Oklahoma with a unified data model for cross-state analysis.

## Quick Start Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run specific test file
uv run pytest tests/test_unified_models.py -v

# Run property-based tests with statistics
uv run pytest app/tests/test_ingest_hypothesis.py -v --hypothesis-show-statistics

# Run production loader
uv run python scripts/loaders/production_loader.py testing oklahoma_2020

# Run main analysis script
uv run python app/main.py
```

## Architecture Overview

```
app/
├── abcs/           # Abstract base classes (StateCategoryClass, StateFileValidation, etc.)
├── funcs/          # Shared utilities (CSV reading, DB loading, validation helpers)
├── ingest/         # Schema-driven file parsing (GenericFileReader)
├── states/         # State implementations + unified models
│   ├── texas/      # Texas Ethics Commission data
│   ├── oklahoma/   # Oklahoma campaign finance data
│   ├── unified_*.py  # Cross-state unified models and processors
│   └── postgres_*.py # PostgreSQL configuration and loading
└── tests/          # Unit tests

tests/              # Integration tests
scripts/            # Utility scripts (loaders, debug, analysis)
docs/               # Technical documentation
```

## Key Patterns

### State Data Processing
- Use `StateCategoryClass` ABC for state-specific data handling
- State config via `StateConfig` dataclass with `CATEGORY_TYPES`
- Validators are SQLModel classes with Pydantic field validators

### File Ingestion
- `GenericFileReader` with schema-driven parsing
- Build schemas with `build_schema_for_states(['texas', 'oklahoma'])`
- Automatic header normalization and type conversion

### Database Operations
- Use `db_manager.get_session()` context manager
- Cache addresses, committees, entities to prevent duplicates
- SQLModel for ORM with Pydantic validation

### Data Loading
```python
from app.states.unified_sqlmodels import unified_sql_processor

transaction = unified_sql_processor.process_record(
    record, state='texas', state_id=active_state.id, state_code='TX'
)
```

## Important Files

| File | Purpose |
|------|---------|
| `app/states/unified_sqlmodels.py` | Unified SQLModel implementations |
| `app/states/unified_field_library.py` | Cross-state field mapping |
| `app/ingest/file_reader.py` | Schema-driven CSV/file parsing |
| `app/abcs/abc_category.py` | Core data processing ABC |
| `scripts/loaders/production_loader.py` | Production data loader |

## Documentation

Comprehensive docs in `docs/` directory:
- `ARCHITECTURE.md` - System design
- `DATA_DICTIONARY.md` - Field definitions
- `STATES.md` - State-specific configurations
- `RUNBOOK.md` - Troubleshooting guide
- `TESTING.md` - Test strategies

See `AGENTS.md` for detailed coding patterns, conventions, and boundaries.

## Testing

- Pytest + Hypothesis for property-based testing
- Tests in `tests/` (integration) and `app/tests/` (unit)
- Run with `uv run pytest` - all tests must pass before commits

## Environment

- Python 3.12+ with uv package manager
- PostgreSQL for production, SQLite for development
- Secrets via 1Password SDK or `.env` file
- Logging to PaperTrail + local files
