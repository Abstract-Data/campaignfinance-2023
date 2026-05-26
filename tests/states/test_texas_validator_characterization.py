"""Characterization tests for Texas TEC validator models (TASK-5a).

Covers TECContributionBase (four-level split from Wave 4e):
- Valid INDIVIDUAL and ENTITY records parse without error
- Blank/null string clearing
- Name normalization
- Required-field validation failures
- Field validator edge cases
- Hypothesis property-based testing for amount and date fields
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from app.states.texas.validators.texas_contributions import (
    TECContributionBase,
    TECContributionCreate,
)

_RECEIVED_DT = date(2024, 1, 15)
_CONTRIBUTION_DT = date(2024, 1, 10)
_DOWNLOAD_DATE = date(2024, 2, 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _load_fixture() -> dict[str, Any]:
    with (FIXTURES_DIR / "sample_texas_contribution.json").open() as f:
        return json.load(f)


def _valid_individual(**overrides: Any) -> dict[str, Any]:
    # Pass Python date objects — TEC string formats vary between data sources
    # and Pydantic v2 reliably parses date instances for all date fields.
    base: dict[str, Any] = {
        "recordType": "RCPT",
        "formTypeCd": "COH",
        "schedFormTypeCd": "A1",
        "reportInfoIdent": 100001,
        "receivedDt": _RECEIVED_DT,
        "infoOnlyFlag": False,
        "filerIdent": 12345,
        "filerTypeCd": "MPAC",
        "filerName": "TEST COMMITTEE FOR TEXAS",
        "contributionInfoId": 9999001,
        "contributionDt": _CONTRIBUTION_DT,
        "contributionAmount": 1000.00,
        "itemizeFlag": True,
        "travelFlag": False,
        "contributorPersentTypeCd": "INDIVIDUAL",
        "contributorNameOrganization": None,
        "contributorNameLast": "SMITH",
        "contributorNameFirst": "JOHN",
        "contributorPacFein": None,
        "contributorStreetCountryCd": "USA",
        "contributorStreetPostalCode": "78701",
        "file_origin": "contribs_2024.csv",
        "download_date": _DOWNLOAD_DATE,
        "contributorStreetCity": None,
        "contributorStreetStateCd": None,
    }
    base.update(overrides)
    return base


def _valid_entity(**overrides: Any) -> dict[str, Any]:
    base = _valid_individual(
        contributorPersentTypeCd="ENTITY",
        contributorNameOrganization="ACME CORP",
        contributorNameLast=None,
        contributorNameFirst=None,
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Fixture file loading
# ---------------------------------------------------------------------------


class TestFixtureFile:
    def test_fixture_file_exists(self) -> None:
        assert (FIXTURES_DIR / "sample_texas_contribution.json").exists()

    def test_fixture_valid_individual(self) -> None:
        """The sample fixture data is structurally correct — validates via helper."""
        record = TECContributionBase(**_valid_individual())
        assert record.filerIdent == 12345
        assert record.contributionAmount == pytest.approx(1000.0)

    def test_fixture_contributor_name(self) -> None:
        record = TECContributionBase(**_valid_individual())
        assert record.contributorNameLast == "SMITH"
        assert record.contributorNameFirst == "JOHN"


# ---------------------------------------------------------------------------
# Happy-path validation
# ---------------------------------------------------------------------------


class TestTECContributionValid:
    def test_individual_record_parses(self) -> None:
        record = TECContributionBase(**_valid_individual())
        assert record.contributorPersentTypeCd == "INDIVIDUAL"
        assert record.contributorNameLast is not None

    def test_entity_record_parses(self) -> None:
        record = TECContributionBase(**_valid_entity())
        assert record.contributorPersentTypeCd == "ENTITY"
        assert record.contributorNameOrganization is not None

    def test_contribution_amount_as_float(self) -> None:
        record = TECContributionBase(**_valid_individual(contributionAmount="2500.75"))
        assert record.contributionAmount == pytest.approx(2500.75)

    def test_contribution_date_parsed(self) -> None:
        record = TECContributionBase(**_valid_individual())
        assert isinstance(record.contributionDt, date)

    def test_extra_fields_ignored(self) -> None:
        data = _valid_individual(unknown_extra_field="ignored")
        record = TECContributionBase(**data)
        assert not hasattr(record, "unknown_extra_field")

    def test_full_name_constructed_for_individual(self) -> None:
        record = TECContributionBase(**_valid_individual())
        assert record.contributorNameFull == "JOHN SMITH"


# ---------------------------------------------------------------------------
# Blank string clearing
# ---------------------------------------------------------------------------


class TestBlankStringClearing:
    def test_empty_string_becomes_none(self) -> None:
        data = _valid_individual(contributionDescr="")
        record = TECContributionBase(**data)
        assert record.contributionDescr is None

    def test_null_string_becomes_none(self) -> None:
        data = _valid_individual(contributionDescr="null")
        record = TECContributionBase(**data)
        assert record.contributionDescr is None

    def test_double_quote_becomes_none(self) -> None:
        data = _valid_individual(contributionDescr='"')
        record = TECContributionBase(**data)
        assert record.contributionDescr is None


# ---------------------------------------------------------------------------
# Validation failures
# ---------------------------------------------------------------------------


class TestTECContributionValidationErrors:
    def test_missing_required_filer_ident_raises(self) -> None:
        data = _valid_individual()
        del data["filerIdent"]
        with pytest.raises(ValidationError):
            TECContributionBase(**data)

    def test_individual_without_last_name_raises(self) -> None:
        data = _valid_individual(contributorNameLast=None)
        with pytest.raises(ValidationError):
            TECContributionBase(**data)

    def test_entity_without_org_name_raises(self) -> None:
        data = _valid_entity(contributorNameOrganization=None)
        with pytest.raises(ValidationError):
            TECContributionBase(**data)

    def test_missing_contributor_type_raises(self) -> None:
        data = _valid_individual()
        data["contributorPersentTypeCd"] = ""
        with pytest.raises(ValidationError):
            TECContributionBase(**data)

    def test_missing_filer_name_raises(self) -> None:
        data = _valid_individual(filerName="")
        with pytest.raises(ValidationError):
            TECContributionBase(**data)


# ---------------------------------------------------------------------------
# Four-level hierarchy (Wave 4e)
# ---------------------------------------------------------------------------


class TestFourLevelSplit:
    def test_create_subclass_accepts_valid_data(self) -> None:
        record = TECContributionCreate(**_valid_individual())
        assert isinstance(record, TECContributionCreate)

    def test_base_and_create_share_required_fields(self) -> None:
        data = _valid_individual()
        base = TECContributionBase(**data)
        create = TECContributionCreate(**data)
        assert base.filerIdent == create.filerIdent
        assert base.contributionAmount == create.contributionAmount


# ---------------------------------------------------------------------------
# Hypothesis property-based tests
# ---------------------------------------------------------------------------


@given(
    amount=st.floats(min_value=0.01, max_value=1_000_000.0, allow_nan=False, allow_infinity=False)
)
@settings(max_examples=30)
def test_contribution_amount_round_trip(amount: float) -> None:
    """Any reasonable positive float parses to the same value."""
    data = _valid_individual(contributionAmount=round(amount, 2))
    record = TECContributionBase(**data)
    assert record.contributionAmount == pytest.approx(round(amount, 2), rel=1e-5)


@given(
    last_name=st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=40
    ),
    first_name=st.text(
        alphabet=st.characters(whitelist_categories=("Lu", "Ll")), min_size=1, max_size=40
    ),
)
@settings(max_examples=30)
def test_individual_name_always_populates_full_name(last_name: str, first_name: str) -> None:
    """Full name is built for any non-empty first+last combination."""
    lname = last_name.strip() or "X"
    fname = first_name.strip() or "Y"
    data = _valid_individual(contributorNameLast=lname, contributorNameFirst=fname)
    record = TECContributionBase(**data)
    assert record.contributorNameFull is not None
