"""Property tests for Texas-specific validation functions."""

from __future__ import annotations

import copy
from datetime import date

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic_core import PydanticCustomError

from app.states.texas.funcs.tx_validation_funcs import (
    check_zipcodes,
    phone_number_validation,
    validate_dates,
)


class TestTXDateValidatorProperties:
    @given(st.dictionaries(st.text(), st.one_of(st.text(), st.none(), st.integers())))
    @settings(max_examples=300)
    def test_validate_dates_never_raises_unexpected(self, values: dict) -> None:
        payload = dict(values)
        try:
            result = validate_dates(None, payload)
            assert isinstance(result, dict)
        except PydanticCustomError:
            pass
        except Exception as exc:
            pytest.fail(f"validate_dates raised unexpectedly on {values!r}: {exc}")

    @given(
        st.dates(min_value=date(1990, 1, 1), max_value=date(2030, 12, 31)).map(
            lambda d: d.strftime("%Y%m%d")
        )
    )
    def test_validate_dates_parses_dt_suffix_fields(self, date_str: str) -> None:
        from datetime import date as date_type

        payload = {"receivedDt": date_str, "otherField": "keep"}
        result = validate_dates(None, copy.deepcopy(payload))
        assert isinstance(result["receivedDt"], date_type)
        assert result["otherField"] == "keep"


class TestTXZipcodeValidatorProperties:
    @given(st.dictionaries(st.text(), st.one_of(st.text(), st.none())))
    @settings(max_examples=200)
    def test_check_zipcodes_never_raises_unexpected(self, values: dict) -> None:
        payload = dict(values)
        try:
            result = check_zipcodes(None, payload)
            assert isinstance(result, dict)
        except PydanticCustomError:
            pass
        except Exception as exc:
            pytest.fail(f"check_zipcodes raised unexpectedly: {exc}")

    @given(st.from_regex(r"[0-9]{5}", fullmatch=True))
    def test_postal_code_fields_normalized(self, zipcode: str) -> None:
        payload = {"contributorStreetPostalCode": zipcode}
        result = check_zipcodes(None, copy.deepcopy(payload))
        assert result["contributorStreetPostalCode"] == zipcode


class TestTXPhoneValidatorProperties:
    @given(st.dictionaries(st.text(), st.one_of(st.text(), st.none(), st.integers())))
    @settings(max_examples=200)
    def test_phone_number_validation_never_raises_unexpected(self, values: dict) -> None:
        payload = dict(values)
        try:
            result = phone_number_validation(None, payload)
            assert isinstance(result, dict)
        except PydanticCustomError:
            pass
        except Exception as exc:
            pytest.fail(f"phone_number_validation raised unexpectedly: {exc}")
