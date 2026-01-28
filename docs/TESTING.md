# TESTING.md

Testing strategies, evaluation sets, and data quality validation for the campaign finance processing system.

> **See also:** `../AGENTS.md` for code patterns, `RUNBOOK.md` for debugging, `DATA_DICTIONARY.md` for field validation rules.

## Test Categories

### Unit Tests

Test individual components in isolation.

```python
# Testing validation classes
from app.abcs.abc_validation import StateFileValidation
from sqlmodel import SQLModel, Field
from typing import Optional

class MockModel(SQLModel):
    """Mock model for testing validation."""
    id: Optional[str] = Field(default=None)
    field: str

def test_validate_record_passes_valid_data():
    """Valid records should pass validation."""
    validator = StateFileValidation(validator_to_use=MockModel)
    record = {'field': 'test_value'}
    status, result = validator.validate_record(record)
    assert status == 'passed'
    assert isinstance(result, MockModel)

def test_validate_record_fails_invalid_data():
    """Invalid records should fail validation with error details."""
    validator = StateFileValidation(validator_to_use=MockModel)
    record = {}  # Missing required 'field'
    status, result = validator.validate_record(record)
    assert status == 'failed'
    assert 'error' in result
```

### Property-Based Tests with Hypothesis

Test with randomly generated data to find edge cases.

```python
from hypothesis import given, strategies as st, settings
from decimal import Decimal
import datetime as dt

record_strategy = st.fixed_dictionaries({
    "Transaction ID": st.text(min_size=1, max_size=12),
    "Amount": st.decimals(
        min_value=Decimal("-100000"),
        max_value=Decimal("100000"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ).map(lambda value: f"{value:f}"),
    "Transaction Date": st.dates(
        min_value=dt.date(2000, 1, 1),
        max_value=dt.date(2030, 12, 31)
    ).map(lambda value: value.strftime("%Y-%m-%d")),
})

@given(st.lists(record_strategy, min_size=1, max_size=5))
@settings(max_examples=25)
def test_generic_file_reader_handles_csv(tmp_path, records):
    """File reader should handle any valid CSV structure."""
    schema, headers = _build_test_schema()
    reader = GenericFileReader(schema=schema, add_metadata=True, strict=True)
    
    # Write test CSV
    file_path = tmp_path / "sample.csv"
    with file_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records)
    
    # Verify reader output
    output = list(reader.read_records(file_path))
    assert len(output) == len(records)
    assert all("file_origin" in r for r in output)
    assert all("download_date" in r for r in output)
```

### Integration Tests

Test the full data pipeline from file to database.

```python
import pytest
from pathlib import Path
from app.states.texas import TexasCategory, TEXAS_CONFIGURATION

@pytest.fixture
def texas_contributions(tmp_path, monkeypatch):
    """Create Texas contributions category with test data."""
    monkeypatch.setattr(StateConfig, "TEMP_FOLDER", property(lambda self: tmp_path))
    # Create test CSV file
    test_file = tmp_path / "contributions_test.csv"
    # ... populate with test data
    return TexasCategory("contributions")

def test_texas_contributions_validates_records(texas_contributions):
    """Texas contributions should validate correctly."""
    texas_contributions.read()
    passed, failed = texas_contributions.validate()
    
    passed_list = list(passed)
    failed_list = list(failed)
    
    assert len(passed_list) > 0
    assert all(hasattr(r, 'id') for r in passed_list)

def test_production_loader_processes_batch():
    """Production loader should process batches with deduplication."""
    from production_loader import ProductionLoader, LoaderConfig
    
    config = LoaderConfig(batch_size=10, max_records=100)
    loader = ProductionLoader(config)
    
    # Test batch processing
    stats = loader.load_file(test_file_path, state="texas")
    
    assert stats.success_rate > 95.0
    assert len(loader.address_cache) > 0  # Deduplication working
```

### File Consolidation Tests

Test Parquet file consolidation with Hypothesis.

```python
@st.composite
def texas_category_files(draw):
    """Generate random category file structures for testing."""
    categories = draw(
        st.sets(st.sampled_from(["contributions", "expenses", "filers"]), 
                min_size=1, max_size=2)
    )
    result = {}
    for category in categories:
        file_count = draw(st.integers(min_value=1, max_value=3))
        files = []
        for _ in range(file_count):
            columns = draw(st.sets(st.sampled_from(file_columns), min_size=1))
            row_strategy = st.fixed_dictionaries({
                col: st.text(min_size=1, max_size=10) for col in columns
            })
            rows = draw(st.lists(row_strategy, min_size=1, max_size=3))
            files.append((list(columns), rows))
        result[category] = files
    return result

@given(texas_category_files())
@settings(max_examples=10)
def test_texas_consolidate_files(tmp_path, monkeypatch, category_files):
    """File consolidation should merge all category files correctly."""
    # Setup test files
    for category, file_entries in category_files.items():
        for index, (columns, rows) in enumerate(file_entries, start=1):
            df = pl.DataFrame({col: [row[col] for row in rows] for col in columns})
            file_path = tmp_path / f"{category}_{index:02d}.parquet"
            df.write_parquet(file_path)
    
    # Run consolidation
    TECDownloader.consolidate_files()
    
    # Verify results
    for category in category_files:
        consolidated = list(tmp_path.glob(f"{category}_*.parquet"))
        assert len(consolidated) == 1
        df = pl.read_parquet(consolidated[0])
        assert "file_origin" in df.columns
```

## Test Commands

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v --tb=short

# Stop on first failure (fast feedback)
uv run pytest -x

# Run specific test file
uv run pytest app/tests/test_validation_class.py

# Run specific test function
uv run pytest -k "test_validate_record"

# Run with coverage
uv run pytest --cov=app --cov-report=term-missing

# Run with Hypothesis statistics
uv run pytest app/tests/test_ingest_hypothesis.py -v --hypothesis-show-statistics
```

### Hypothesis Configuration

```python
# conftest.py or test file
from hypothesis import settings, Verbosity

# Configure for CI (more examples, longer deadline)
settings.register_profile("ci", max_examples=100, deadline=None)

# Configure for development (fewer examples, faster)
settings.register_profile("dev", max_examples=10)

# Use profile based on environment
settings.load_profile("dev")  # or "ci"
```

## Data Quality Evaluation

### Validation Metrics

Track validation pass/fail rates per category:

```
Category          | Pass Rate | Fail Rate | Common Errors
------------------|-----------|-----------|--------------------
Contributions     | 98.2%     | 1.8%      | Invalid date format
Expenditures      | 97.5%     | 2.5%      | Missing amount
Filers            | 99.1%     | 0.9%      | Invalid filer ID
Reports           | 96.8%     | 3.2%      | Missing report ID
```

### Loader Performance Metrics

```
Metric                  | Target    | Current   | Notes
------------------------|-----------|-----------|------------------
Success Rate            | > 95%     | 97.2%     | Per batch
Records/Second          | > 500     | 650       | With deduplication
Address Dedup Rate      | > 60%     | 72%       | Cache hit rate
Committee Dedup Rate    | > 80%     | 85%       | Cache hit rate
Memory Usage            | < 2GB     | 1.4GB     | Peak during load
Error Recovery          | 100%      | 100%      | Continue on error
```

### Field Mapping Coverage

```python
def test_field_library_coverage():
    """Verify all state fields are mapped."""
    from app.core.unified_field_library import field_library
    
    for state in ['texas', 'oklahoma']:
        mappings = field_library.get_state_mappings(state)
        
        # Check critical fields are mapped
        critical_fields = [
            'amount', 'transaction_date', 'filer_id', 
            'contributor_name', 'committee_name'
        ]
        mapped_unified = {m.unified_field for m in mappings}
        
        for field in critical_fields:
            assert field in mapped_unified, f"Missing {field} mapping for {state}"
```

## Evaluation Sets

### Classification Tests

```json
{
  "evaluation_set": [
    {
      "input_file": "contributions_2020.csv",
      "expected_category": "contributions",
      "expected_record_count": 45000,
      "test_type": "file_classification"
    },
    {
      "input_record": {"contributionAmount": "1000.00", "contributorNameLast": "SMITH"},
      "expected_transaction_type": "CONTRIBUTION",
      "test_type": "transaction_classification"
    }
  ]
}
```

### Data Transformation Tests

```python
@pytest.mark.parametrize("input_amount,expected", [
    ("1,000.00", Decimal("1000.00")),
    ("$500.50", Decimal("500.50")),
    ("-250.00", Decimal("-250.00")),
    ("1000", Decimal("1000")),
    ("", None),
    (None, None),
])
def test_amount_parsing(input_amount, expected):
    """Amount field should handle various formats."""
    from app.ingest.file_reader import _parse_decimal
    assert _parse_decimal(input_amount) == expected

@pytest.mark.parametrize("input_date,expected", [
    ("2023-01-15", date(2023, 1, 15)),
    ("01/15/2023", date(2023, 1, 15)),
    ("15-Jan-2023", date(2023, 1, 15)),
    ("", None),
    (None, None),
])
def test_date_parsing(input_date, expected):
    """Date field should handle various formats."""
    from app.ingest.file_reader import _parse_date
    assert _parse_date(input_date) == expected
```

### Deduplication Tests

```python
def test_address_deduplication():
    """Same address should not create duplicates."""
    addresses = [
        {"street_1": "123 MAIN ST", "city": "AUSTIN", "state": "TX", "zip_code": "78701"},
        {"street_1": "123 Main St", "city": "Austin", "state": "tx", "zip_code": "78701"},
        {"street_1": "123 main st", "city": "austin", "state": "TX", "zip_code": "78701-0000"},
    ]
    
    loader = ProductionLoader(LoaderConfig())
    unique_keys = set()
    
    for addr in addresses:
        key = loader._address_key(UnifiedAddress(**addr))
        unique_keys.add(key)
    
    # All should normalize to same key
    assert len(unique_keys) == 1

def test_committee_deduplication():
    """Committees with same filer_id should deduplicate."""
    with db_manager.get_session() as session:
        # Insert committee
        committee1 = UnifiedCommittee(filer_id="12345", name="Test Committee")
        loader._ensure_committee(session, committee1)
        
        # Try to insert duplicate
        committee2 = UnifiedCommittee(filer_id="12345", name="Test Committee Updated")
        result = loader._ensure_committee(session, committee2)
        
        # Should return cached/existing committee
        assert result.filer_id == "12345"
        
        # Should only have one committee in database
        count = session.exec(text("SELECT COUNT(*) FROM unified_committees WHERE filer_id = '12345'")).first()
        assert count[0] == 1
```

## Regression Testing

### Golden File Tests

Compare output against known-good baselines:

```python
@pytest.fixture
def golden_texas_contribution():
    """Load golden contribution record for comparison."""
    return {
        'transaction_id': 'TX-CONTRIB-12345',
        'amount': Decimal('1000.00'),
        'transaction_date': date(2023, 6, 15),
        'contributor_first_name': 'JOHN',
        'contributor_last_name': 'SMITH',
        'committee_filer_id': '00012345',
    }

def test_texas_contribution_matches_golden(golden_texas_contribution):
    """Texas contribution processing should match golden baseline."""
    raw_record = {
        'contributionInfoId': '12345',
        'contributionAmount': '1000.00',
        'contributionDt': '20230615',
        'contributorNameFirst': 'JOHN',
        'contributorNameLast': 'SMITH',
        'filerIdent': '00012345',
    }
    
    transaction = unified_sql_processor.process_record(raw_record, 'texas')
    
    assert str(transaction.amount) == str(golden_texas_contribution['amount'])
    assert transaction.transaction_date == golden_texas_contribution['transaction_date']
```

### Schema Migration Tests

```python
def test_database_schema_backwards_compatible():
    """New schema should support existing data."""
    # Load sample of existing data
    with db_manager.get_session() as session:
        transactions = session.exec(
            select(UnifiedTransaction).limit(100)
        ).all()
        
        # Verify all required fields present
        for tx in transactions:
            assert tx.id is not None
            assert tx.state_id is not None
            # Check new fields have defaults
            assert hasattr(tx, 'file_origin_id')
```

## Continuous Integration

### Pre-commit Checks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: local
    hooks:
      - id: pytest-quick
        name: Quick Tests
        entry: uv run pytest app/tests/ -x --tb=short
        language: system
        pass_filenames: false
        always_run: true
```

### CI Pipeline Tests

```bash
# CI test script
#!/bin/bash
set -e

echo "🧪 Running unit tests..."
uv run pytest app/tests/test_validation_class.py -v

echo "🔬 Running property tests..."
uv run pytest app/tests/test_ingest_hypothesis.py -v --hypothesis-show-statistics

echo "📊 Running integration tests..."
uv run pytest -k "integration" -v

echo "✅ All tests passed!"
```

### Test Data Management

```bash
# Download test fixtures (small sample files)
uv run python scripts/download_test_fixtures.py

# Verify test data integrity
uv run python scripts/verify_test_data.py

# Generate synthetic test data
uv run python scripts/generate_synthetic_data.py --records 1000 --state texas
```

## Debugging Failed Tests

### Common Issues

1. **Hypothesis finds edge case**: Check the `@example` decorator output
2. **Validation failures**: Check `validator.errors.summary` for details
3. **Database constraint errors**: Check deduplication caches are populated
4. **File encoding issues**: Test with both UTF-8 and ISO-8859-1

### Debug Commands

```bash
# Run with print output visible
uv run pytest -s -v test_file.py::test_function

# Run with debugger on failure
uv run pytest --pdb test_file.py

# Show Hypothesis example database
uv run pytest --hypothesis-show-statistics -v

# Clear Hypothesis example database (force new examples)
rm -rf .hypothesis/
```

### Logging During Tests

```python
import logging

@pytest.fixture(autouse=True)
def enable_test_logging():
    """Enable debug logging during tests."""
    logging.basicConfig(level=logging.DEBUG)
    yield
    logging.basicConfig(level=logging.WARNING)
```

## Performance Testing

### Load Testing

```python
import time
from production_loader import ProductionLoader, LoaderConfig

def test_loader_performance():
    """Loader should meet performance targets."""
    config = LoaderConfig(
        batch_size=100,
        max_records=10000,
        enable_progress=False
    )
    loader = ProductionLoader(config)
    
    start = time.time()
    stats = loader.load_file(large_test_file, state="texas")
    duration = time.time() - start
    
    # Performance assertions
    assert stats.records_per_second > 500, f"Too slow: {stats.records_per_second} rec/s"
    assert stats.success_rate > 95, f"Too many failures: {stats.success_rate}%"
    assert duration < 30, f"Took too long: {duration}s"
```

### Memory Profiling

```python
import tracemalloc

def test_memory_usage():
    """Loader should not exceed memory limits."""
    tracemalloc.start()
    
    config = LoaderConfig(batch_size=100, max_records=50000)
    loader = ProductionLoader(config)
    loader.load_file(test_file, state="texas")
    
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    peak_mb = peak / 1024 / 1024
    assert peak_mb < 2048, f"Memory usage too high: {peak_mb:.1f}MB"
```
