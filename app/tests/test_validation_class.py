from hypothesis import given, strategies as st
from sqlmodel import SQLModel, Field
from typing import Optional
from app.abcs.abc_validation import StateFileValidation


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


@given(st.lists(st.dictionaries(st.text(), st.text())))
def test_validate(records):
    validator = StateFileValidation(validator_to_use=MockModel)
    results = list(validator.validate(iter(records)))
    # Validate returns a generator of (status, record) tuples
    for status, record in results:
        assert status in ['passed', 'failed']
        if status == 'passed':
            assert isinstance(record, MockModel)
        else:
            assert isinstance(record, dict)
            assert 'error' in record