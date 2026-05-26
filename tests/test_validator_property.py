"""Property-based tests for core validator functions using Hypothesis."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic_core import PydanticCustomError

from app.core.builders import UnifiedSQLModelBuilder
from app.core.unified_state_loader import (
    UnifiedStateLoader,
    _load_committee_index,
    _load_person_index,
)
from app.funcs.validator_functions import (
    clear_blank_strings,
    format_zipcode,
    validate_date,
    validate_phone_number,
)

_COLUMN = "test_column"


class TestValidateDateProperties:
    @given(st.text())
    @settings(max_examples=500)
    def test_validate_date_never_raises_unexpected_on_string(self, value: str) -> None:
        """validate_date must not raise outside PydanticCustomError."""
        try:
            result = validate_date(value)
            assert result is None or isinstance(result, date)
        except PydanticCustomError:
            pass
        except Exception as exc:
            pytest.fail(f"validate_date raised unexpectedly on input {value!r}: {exc}")

    @given(st.none())
    def test_validate_date_handles_none(self, value: None) -> None:
        assert validate_date(value) is None

    @given(st.just(""))
    def test_validate_date_handles_empty_string(self, value: str) -> None:
        assert validate_date(value) is None

    @given(
        st.dates(min_value=date(1990, 1, 1), max_value=date(2030, 12, 31)).map(
            lambda d: d.strftime("%Y%m%d")
        )
    )
    def test_validate_date_parses_yyyymmdd(self, value: str) -> None:
        result = validate_date(value)
        assert isinstance(result, date)


class TestFormatZipcodeProperties:
    @given(st.text())
    @settings(max_examples=500)
    def test_format_zipcode_never_raises_unexpected(self, value: str) -> None:
        try:
            result = format_zipcode(_COLUMN, value)
            assert result is None or isinstance(result, str)
        except PydanticCustomError:
            pass
        except Exception as exc:
            pytest.fail(f"format_zipcode raised unexpectedly on {value!r}: {exc}")

    @given(st.from_regex(r"[0-9]{5}", fullmatch=True))
    def test_five_digit_zipcode_passes_through(self, value: str) -> None:
        result = format_zipcode(_COLUMN, value)
        assert result == value

    @given(st.from_regex(r"[0-9]{9}", fullmatch=True))
    def test_nine_digit_zipcode_gets_hyphen(self, value: str) -> None:
        result = format_zipcode(_COLUMN, value)
        assert result == value[:5] + "-" + value[5:]

    @given(st.text(min_size=5, max_size=5, alphabet=st.characters(whitelist_categories=("Nd",))))
    def test_output_is_at_most_10_chars(self, value: str) -> None:
        try:
            result = format_zipcode(_COLUMN, value) or ""
        except PydanticCustomError:
            return
        assert len(result) <= 10


class TestValidatePhoneProperties:
    @given(st.text())
    @settings(max_examples=300)
    def test_validate_phone_never_raises_unexpected(self, value: str) -> None:
        try:
            validate_phone_number(_COLUMN, value)
        except (PydanticCustomError, ValueError, AttributeError):
            pass
        except Exception as exc:
            pytest.fail(f"validate_phone_number raised unexpectedly on {value!r}: {exc}")

    @given(st.none() | st.just(""))
    def test_validate_phone_handles_empty(self, value: str | None) -> None:
        assert validate_phone_number(_COLUMN, value) is None


class TestClearBlankStringsProperties:
    @given(st.dictionaries(st.text(), st.one_of(st.text(), st.none())))
    def test_clear_blank_strings_returns_dict(self, values: dict) -> None:
        payload = dict(values)
        result = clear_blank_strings(None, payload)
        assert isinstance(result, dict)

    @given(st.dictionaries(st.text(), st.text(min_size=1)))
    def test_non_blank_values_preserved(self, values: dict) -> None:
        payload = {k: v for k, v in values.items() if v.strip() and v not in {'"', "null"}}
        if not payload:
            return
        result = clear_blank_strings(None, dict(payload))
        for key, value in payload.items():
            assert result.get(key) == value


class TestBuilderFieldResolutionProperties:
    @given(
        st.dictionaries(
            st.text(), st.one_of(st.text(), st.none(), st.integers(), st.floats(allow_nan=False))
        )
    )
    @settings(max_examples=300)
    def test_get_field_value_never_raises(self, raw_data: dict) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        try:
            builder._get_field_value(raw_data, "amount")
        except ValueError:
            pass
        except Exception as exc:
            pytest.fail(f"_get_field_value raised unexpectedly: {exc}")

    @given(
        st.fixed_dictionaries(
            {
                "record_type": st.sampled_from(["RCPT", "EXP", "LOAN"]),
                "filerIdent": st.text(min_size=1, max_size=12),
                "contributionAmount": st.decimals(
                    min_value=0,
                    max_value=1_000_000,
                    places=2,
                    allow_nan=False,
                    allow_infinity=False,
                ).map(str),
                "contributionDt": st.dates(
                    min_value=date(2000, 1, 1), max_value=date(2030, 12, 31)
                ).map(lambda d: d.strftime("%Y-%m-%d")),
            }
        )
    )
    @settings(max_examples=100)
    def test_build_transaction_returns_unified_transaction(self, raw_data: dict) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        txn = builder.build_transaction(raw_data)
        assert txn.state_id == 1
        assert txn.raw_data is not None


class TestUnifiedStateLoaderIndexProperties:
    @given(st.integers(min_value=0, max_value=10_000))
    @settings(max_examples=50)
    def test_load_committee_index_empty_session(self, state_id: int) -> None:
        from sqlmodel import Session, SQLModel, create_engine

        import app.core.models  # noqa: F401

        engine = create_engine("sqlite://")
        for table in SQLModel.metadata.sorted_tables:
            if table.schema is None:
                table.create(engine, checkfirst=True)
        with Session(engine) as session:
            index = _load_committee_index(session, state_id)
        assert isinstance(index, dict)

    @given(st.integers(min_value=0, max_value=10_000))
    @settings(max_examples=50)
    def test_load_person_index_empty_session(self, state_id: int) -> None:
        from sqlmodel import Session, SQLModel, create_engine

        import app.core.models  # noqa: F401

        engine = create_engine("sqlite://")
        for table in SQLModel.metadata.sorted_tables:
            if table.schema is None:
                table.create(engine, checkfirst=True)
        with Session(engine) as session:
            index = _load_person_index(session, state_id)
        assert isinstance(index, dict)

    @pytest.mark.parametrize("state", ["texas", "oklahoma", "ohio"])
    def test_loader_init_normalizes_state(self, state: str, tmp_path) -> None:
        loader = UnifiedStateLoader(state, tmp_path, db_manager=MagicMock())
        assert loader.state == state.lower()
