from hypothesis import given, strategies as st
from pydantic import BaseModel
from funcs import StateFileValidation

class MockModel(BaseModel):
    field: str

@given(st.dictionaries(st.text(), st.text()))
def test_validate_record(record):
    validator = StateFileValidation()
    result = validator.validate_record(record, MockModel)
    assert result[0] in ['passed', 'failed']
    if result[0] == 'passed':
        assert isinstance(result[1], MockModel)
    else:
        assert isinstance(result[1], dict)
        assert 'error' in result[1]

@given(st.lists(st.dictionaries(st.text(), st.text())))
def test_validate(records):
    validator = StateFileValidation()
    result = validator.validate(iter(records), MockModel)
    assert isinstance(result, StateFileValidation)
    assert all(isinstance(record, MockModel) for record in result.passed)
    assert all('error' in record for record in result.failed)