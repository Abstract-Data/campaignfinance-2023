# CONTRIBUTING.md

Guidelines for contributing to the campaign finance data processing system.

> **See also:** `AGENTS.md` for code patterns and boundaries, `docs/DATA_DICTIONARY.md` for field definitions, `docs/STATES.md` for state details.

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- PostgreSQL (for production) or SQLite (for development)
- Chrome/ChromeDriver (for web scraping)

### Setup

```bash
# Clone the repository
git clone <repository-url>
cd campaignfinance

# Install dependencies
uv sync

# Verify installation
uv run pytest app/tests/ -v
```

---

## Common Contribution Tasks

### Adding a New State

Follow these steps to add support for a new state:

#### 1. Create State Directory Structure

```bash
mkdir -p app/states/{state_name}/validators
mkdir -p app/states/{state_name}/funcs
touch app/states/{state_name}/__init__.py
touch app/states/{state_name}/{state_name}_fields.toml
```

#### 2. Create State Configuration

```python
# app/states/{state_name}/__init__.py
from app.abcs import StateCategoryClass, StateConfig, CategoryConfig, CSVReaderConfig, CategoryTypes
from functools import partial
from . import validators

{STATE_NAME}_CONFIGURATION = StateConfig(
    STATE_NAME="{State Name}",
    STATE_ABBREVIATION="{XX}",
    CSV_CONFIG=CSVReaderConfig(),
)

{State}CategoryConfig = partial(CategoryConfig, FIELDS={STATE_NAME}_CONFIGURATION.FIELD_DATA)

{STATE_NAME}_CONFIGURATION.CATEGORY_TYPES = CategoryTypes(
    contributions={State}CategoryConfig(DESC="contributions", VALIDATOR=validators.{State}Contribution),
    expenses={State}CategoryConfig(DESC="expenses", VALIDATOR=validators.{State}Expense),
)

{State}Category = partial(StateCategoryClass, config={STATE_NAME}_CONFIGURATION)
```

#### 3. Create Validators

```python
# app/states/{state_name}/validators/{state}_contribution.py
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import date
from pydantic import field_validator, model_validator

class {State}Contribution(SQLModel, table=True):
    __tablename__ = "{xx}_contributions"
    __table_args__ = {"schema": "{state_name}"}
    
    id: Optional[str] = Field(default=None, primary_key=True)
    # Add state-specific fields here
    amount: float = Field(..., description="Contribution amount")
    contribution_date: date = Field(..., description="Date of contribution")
    
    # Add validators as needed
    @model_validator(mode='before')
    @classmethod
    def clear_blank_strings(cls, values):
        for k, v in values.items():
            if v in ["", "null"]:
                values[k] = None
        return values
```

#### 4. Register Field Mappings

```python
# In app/states/unified_field_library.py, add to _initialize_state_mappings()

self.state_mappings["{state_name}"] = [
    StateFieldMapping("{state_name}", "{State Field}", "transaction_id", 1.0),
    StateFieldMapping("{state_name}", "{Amount Field}", "amount", 1.0),
    StateFieldMapping("{state_name}", "{Date Field}", "transaction_date", 1.0),
    # Add all field mappings
]
```

#### 5. Create Downloader (Optional)

```python
# app/states/{state_name}/{state}_downloader.py
from app.abcs import FileDownloaderABC, StateConfig

class {State}Downloader(FileDownloaderABC):
    def __init__(self, config: StateConfig):
        super().__init__(config=config)
    
    @classmethod
    def download(cls):
        # Implement download logic using Selenium
        pass
```

#### 6. Add Tests

```python
# app/tests/test_{state_name}.py
import pytest
from app.states.{state_name} import {State}Category

def test_{state}_contributions_validate():
    contrib = {State}Category("contributions")
    contrib.read()
    passed, failed = contrib.validate()
    assert len(list(passed)) > 0
```

#### 7. Update Documentation

- Add state to `docs/STATES.md`
- Add field mappings to `docs/DATA_DICTIONARY.md`
- Update `docs/GLOSSARY.md` with state-specific terms

---

### Adding a New Field Mapping

When a state adds new fields or you discover unmapped fields:

#### 1. Identify the Field

```bash
# Check headers in the file
uv run python -c "
import polars as pl
df = pl.scan_parquet('tmp/{state}/{file}.parquet')
print(df.collect_schema().names())
"
```

#### 2. Find or Create Unified Field

Check if a suitable unified field exists:
```python
from app.core.unified_field_library import field_library
print([f.name for f in field_library.unified_fields.values()])
```

#### 3. Add the Mapping

```python
# In unified_field_library.py
StateFieldMapping("{state}", "{new_state_field}", "{unified_field}", 1.0),
```

#### 4. Update Documentation

Add to `docs/DATA_DICTIONARY.md` under the state's section.

---

### Adding a New Validator

When you need custom validation logic:

```python
# app/states/{state}/validators/{state}_{category}.py

from pydantic import field_validator, model_validator

class MyValidator(SQLModel, table=True):
    
    @field_validator('field_name', mode='before')
    @classmethod
    def validate_field(cls, v):
        """Validate and transform field value."""
        if isinstance(v, str):
            v = v.strip()
            if v.lower() in ('', 'null', 'none'):
                return None
        return v
    
    @model_validator(mode='before')
    @classmethod
    def validate_model(cls, values):
        """Cross-field validation."""
        # Add validation logic
        return values
```

---

### Adding a New Test

#### Unit Test

```python
# app/tests/test_my_feature.py
import pytest
from app.states.texas import TexasCategory

def test_specific_behavior():
    """Describe what this test verifies."""
    # Arrange
    category = TexasCategory("contributions")
    
    # Act
    result = category.some_method()
    
    # Assert
    assert result is not None
```

#### Property-Based Test

```python
from hypothesis import given, strategies as st, settings

@given(st.dictionaries(st.text(), st.text()))
@settings(max_examples=25)
def test_handles_any_input(data):
    """Test handles arbitrary input without crashing."""
    # Test code here
    pass
```

---

## Code Style

### Naming Conventions

```python
# Functions and variables: snake_case
def get_filer_data():
    contribution_amount = 100.00

# Classes: PascalCase
class TECDownloader:
    pass

# Constants: UPPER_SNAKE_CASE
TEXAS_CONFIGURATION = StateConfig(...)

# Private methods: _leading_underscore
def _extract_officer_from_record(record):
    pass
```

### Type Hints

Always use type hints:

```python
from typing import Dict, List, Optional

def process_records(
    records: List[Dict[str, str]],
    state: str,
    limit: Optional[int] = None
) -> List[UnifiedTransaction]:
    ...
```

### Docstrings

Use docstrings for public functions:

```python
def load_state_data(state: str, data_directory: Path) -> Dict[str, Any]:
    """
    Load all campaign finance data for a state.
    
    Args:
        state: State name (e.g., 'texas', 'oklahoma')
        data_directory: Directory containing state data folders
        
    Returns:
        Summary report of the loading process
        
    Raises:
        ValueError: If no data files found for state
    """
    ...
```

---

## Git Workflow

### Branch Naming

```
feat/add-oklahoma-support
fix/address-deduplication-race-condition
refactor/extract-field-mapping
test/add-hypothesis-tests
docs/update-contributing-guide
```

### Commit Messages

Follow conventional commits:

```
feat: add Oklahoma campaign finance loader
fix: resolve address deduplication race condition
refactor: extract field mapping to unified library
test: add property tests for file reader
docs: update state configuration examples
chore: update dependencies
```

### Pull Request Process

1. **Create a branch** from `main`
2. **Make changes** following the style guide
3. **Run tests** to ensure nothing is broken:
   ```bash
   uv run pytest
   ```
4. **Update documentation** if needed
5. **Submit PR** with clear description
6. **Address review feedback**

### PR Description Template

```markdown
## Summary
Brief description of changes.

## Changes
- Added X
- Fixed Y
- Updated Z

## Testing
- [ ] All existing tests pass
- [ ] Added new tests for new functionality
- [ ] Tested with real data

## Documentation
- [ ] Updated docs/GLOSSARY.md (if new terms)
- [ ] Updated docs/DATA_DICTIONARY.md (if new fields)
- [ ] Updated docs/STATES.md (if state changes)
- [ ] Updated docs/RUNBOOK.md (if operational changes)
```

---

## Testing Requirements

### Before Submitting

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=app --cov-report=term-missing

# Check for syntax errors
uv run python -m py_compile app/**/*.py
```

### Required Coverage

- New features must include tests
- Validators must have unit tests
- Data processing logic should have property-based tests
- Integration tests for new state implementations

---

## Documentation Requirements

### When to Update Docs

| Change Type | Update These Docs |
|-------------|-------------------|
| New state | docs/STATES.md, docs/DATA_DICTIONARY.md |
| New field mapping | docs/DATA_DICTIONARY.md |
| New term/concept | docs/GLOSSARY.md |
| Bug fix pattern | docs/RUNBOOK.md (if recurring) |
| Architecture change | docs/ARCHITECTURE.md |
| New test pattern | docs/TESTING.md |
| New convention | AGENTS.md |

---

## Getting Help

- Check existing documentation first
- Look at similar implementations (e.g., Texas for new states)
- Review test files for usage examples
- Check docs/RUNBOOK.md for common issues

---

## Checklist for Contributors

Before submitting:

- [ ] Code follows naming conventions
- [ ] Type hints added
- [ ] Tests written and passing
- [ ] Documentation updated
- [ ] Commit messages follow conventional format
- [ ] PR description is complete
- [ ] No hardcoded paths or credentials
- [ ] Uses ABC patterns for state-specific code
- [ ] Field mappings registered in unified_field_library
