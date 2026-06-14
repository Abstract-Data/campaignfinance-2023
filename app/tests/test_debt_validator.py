"""Tests for DebtData camelCase validator with guarantor blocks.

Tests construct DebtData via model_validate — no DB connection required.
All field keys match TEC CSV headers exactly.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.states.texas.validators.texas_debtdata import DebtData

# ---------------------------------------------------------------------------
# Shared base data — only fields common to both lender types
# ---------------------------------------------------------------------------

BASE = {
    "recordType": "DEBT",
    "formTypeCd": "COH",
    "schedFormTypeCd": "DEBT",
    "reportInfoIdent": 123456,
    "receivedDt": "20240315",  # TEC YYYYMMDD format (parsed by validate_dates)
    "infoOnlyFlag": None,
    "filerIdent": "00123456",
    "filerTypeCd": "COH",
    "filerName": "SMITH FOR SENATE",
    "loanInfoId": 9900001,
    "loanGuaranteedFlag": None,
    # address (optional but commonly present)
    "lenderStreetCity": "AUSTIN",
    "lenderStreetStateCd": "TX",
    "lenderStreetCountyCd": "453",
    "lenderStreetCountryCd": "USA",
    "lenderStreetPostalCode": "78701",
    "lenderStreetRegion": None,
    # ingestion metadata
    "file_origin": "debts_20240315.csv",
    "download_date": "2024-03-15",
}


def _individual_row(**overrides) -> dict:
    """Build a valid INDIVIDUAL lender row."""
    row = {
        **BASE,
        "lenderPersentTypeCd": "INDIVIDUAL",
        "lenderNameLast": "DOE",
        "lenderNameFirst": "JOHN",
        "lenderNameSuffixCd": None,
        "lenderNamePrefixCd": None,
        "lenderNameShort": None,
        "lenderNameOrganization": None,
    }
    row.update(overrides)
    return row


def _entity_row(**overrides) -> dict:
    """Build a valid ENTITY lender row."""
    row = {
        **BASE,
        "lenderPersentTypeCd": "ENTITY",
        "lenderNameOrganization": "ACME FINANCIAL LLC",
        "lenderNameLast": None,
        "lenderNameFirst": None,
        "lenderNameSuffixCd": None,
        "lenderNamePrefixCd": None,
        "lenderNameShort": None,
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


class TestValidIndividualLender:
    def test_validates_successfully(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.loanInfoId == 9900001
        assert debt.lenderPersentTypeCd == "INDIVIDUAL"
        assert debt.lenderNameLast == "DOE"
        assert debt.lenderNameFirst == "JOHN"

    def test_record_type_uppercased(self):
        debt = DebtData.model_validate(_individual_row(recordType="debt"))
        assert debt.recordType == "DEBT"

    def test_filer_name_uppercased(self):
        debt = DebtData.model_validate(_individual_row(filerName="Smith for Senate"))
        assert debt.filerName == "SMITH FOR SENATE"

    def test_blank_strings_cleared_to_none(self):
        debt = DebtData.model_validate(_individual_row(lenderNameSuffixCd=""))
        assert debt.lenderNameSuffixCd is None

    def test_null_string_cleared_to_none(self):
        debt = DebtData.model_validate(_individual_row(lenderStreetRegion="null"))
        assert debt.lenderStreetRegion is None

    def test_ingestion_fields_preserved(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.file_origin == "DEBTS_20240315.CSV"
        assert str(debt.download_date) == "2024-03-15"

    def test_id_defaults_to_none(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.id is None

    def test_id_can_be_set(self):
        debt = DebtData.model_validate(_individual_row(id="abc123"))
        assert debt.id == "ABC123"


class TestValidEntityLender:
    def test_validates_successfully(self):
        debt = DebtData.model_validate(_entity_row())
        assert debt.lenderPersentTypeCd == "ENTITY"
        assert debt.lenderNameOrganization == "ACME FINANCIAL LLC"

    def test_individual_name_fields_absent(self):
        debt = DebtData.model_validate(_entity_row())
        assert debt.lenderNameLast is None
        assert debt.lenderNameFirst is None


# ---------------------------------------------------------------------------
# Validation-error tests
# ---------------------------------------------------------------------------


class TestIndividualMissingNames:
    def test_missing_last_name_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_individual_row(lenderNameLast=None))
        errors = exc_info.value.errors()
        assert any(e["type"] == "individual_lender_info" for e in errors)

    def test_missing_first_name_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_individual_row(lenderNameFirst=None))
        errors = exc_info.value.errors()
        assert any(e["type"] == "individual_lender_info" for e in errors)

    def test_blank_last_name_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_individual_row(lenderNameLast=""))
        errors = exc_info.value.errors()
        assert any(e["type"] == "individual_lender_info" for e in errors)

    def test_blank_first_name_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_individual_row(lenderNameFirst=""))
        errors = exc_info.value.errors()
        assert any(e["type"] == "individual_lender_info" for e in errors)


class TestEntityMissingOrganization:
    def test_missing_organization_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_entity_row(lenderNameOrganization=None))
        errors = exc_info.value.errors()
        assert any(e["type"] == "entity_lender_info" for e in errors)

    def test_blank_organization_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_entity_row(lenderNameOrganization=""))
        errors = exc_info.value.errors()
        assert any(e["type"] == "entity_lender_info" for e in errors)


class TestInvalidLenderType:
    def test_unknown_type_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_individual_row(lenderPersentTypeCd="UNKNOWN"))
        errors = exc_info.value.errors()
        assert any(e["type"] == "lender_type" for e in errors)

    def test_none_type_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DebtData.model_validate(_individual_row(lenderPersentTypeCd=None))
        errors = exc_info.value.errors()
        assert any(e["type"] == "lender_type" for e in errors)


# ---------------------------------------------------------------------------
# Guarantor block round-trip tests
# ---------------------------------------------------------------------------


class TestGuarantorBlock1:
    def test_guarantor1_fields_round_trip(self):
        row = _individual_row(
            guarantorPersentTypeCd1="INDIVIDUAL",
            guarantorNameLast1="JONES",
            guarantorNameFirst1="MARY",
            guarantorNameSuffixCd1=None,
            guarantorNamePrefixCd1=None,
            guarantorNameShort1=None,
            guarantorNameOrganization1=None,
            guarantorStreetCity1="DALLAS",
            guarantorStreetStateCd1="TX",
            guarantorStreetCountyCd1="113",
            guarantorStreetCountryCd1="USA",
            guarantorStreetPostalCode1="75201",
            guarantorStreetRegion1=None,
        )
        debt = DebtData.model_validate(row)
        assert debt.guarantorPersentTypeCd1 == "INDIVIDUAL"
        assert debt.guarantorNameLast1 == "JONES"
        assert debt.guarantorNameFirst1 == "MARY"
        assert debt.guarantorStreetCity1 == "DALLAS"
        assert debt.guarantorStreetStateCd1 == "TX"
        assert debt.guarantorStreetCountryCd1 == "USA"
        assert debt.guarantorStreetPostalCode1 == "75201"
        assert debt.guarantorStreetRegion1 is None

    def test_guarantor1_defaults_to_none_when_absent(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.guarantorPersentTypeCd1 is None
        assert debt.guarantorNameLast1 is None
        assert debt.guarantorNameOrganization1 is None
        assert debt.guarantorStreetCity1 is None

    def test_guarantor1_blank_string_cleared(self):
        debt = DebtData.model_validate(_individual_row(guarantorStreetRegion1=""))
        assert debt.guarantorStreetRegion1 is None


class TestGuarantorBlock2:
    def test_guarantor2_fields_round_trip(self):
        row = _individual_row(
            guarantorPersentTypeCd2="ENTITY",
            guarantorNameOrganization2="GUARANTEE CORP",
            guarantorStreetCity2="HOUSTON",
            guarantorStreetCountryCd2="USA",
        )
        debt = DebtData.model_validate(row)
        assert debt.guarantorPersentTypeCd2 == "ENTITY"
        assert debt.guarantorNameOrganization2 == "GUARANTEE CORP"
        assert debt.guarantorStreetCity2 == "HOUSTON"


class TestGuarantorBlock3:
    def test_guarantor3_fields_default_none(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.guarantorNameLast3 is None
        assert debt.guarantorStreetCity3 is None

    def test_guarantor3_fields_set(self):
        debt = DebtData.model_validate(
            _individual_row(
                guarantorNameLast3="BROWN",
                guarantorNameFirst3="JAMES",
                guarantorStreetCity3="SAN ANTONIO",
            )
        )
        assert debt.guarantorNameLast3 == "BROWN"
        assert debt.guarantorNameFirst3 == "JAMES"
        assert debt.guarantorStreetCity3 == "SAN ANTONIO"


class TestGuarantorBlock4:
    def test_guarantor4_fields_default_none(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.guarantorNameLast4 is None
        assert debt.guarantorStreetPostalCode4 is None

    def test_guarantor4_fields_set(self):
        debt = DebtData.model_validate(
            _individual_row(
                guarantorNameLast4="GARCIA",
                guarantorNameFirst4="ELENA",
                guarantorStreetPostalCode4="78702",
            )
        )
        assert debt.guarantorNameLast4 == "GARCIA"
        assert debt.guarantorStreetPostalCode4 == "78702"


class TestGuarantorBlock5:
    def test_guarantor5_fields_default_none(self):
        debt = DebtData.model_validate(_individual_row())
        assert debt.guarantorNameLast5 is None
        assert debt.guarantorStreetRegion5 is None

    def test_guarantor5_fields_set(self):
        debt = DebtData.model_validate(
            _individual_row(
                guarantorPersentTypeCd5="INDIVIDUAL",
                guarantorNameLast5="WILLIAMS",
                guarantorNameFirst5="ROBERT",
                guarantorStreetCity5="EL PASO",
                guarantorStreetStateCd5="TX",
                guarantorStreetCountryCd5="USA",
            )
        )
        assert debt.guarantorNameLast5 == "WILLIAMS"
        assert debt.guarantorNameFirst5 == "ROBERT"
        assert debt.guarantorStreetCity5 == "EL PASO"
        assert debt.guarantorPersentTypeCd5 == "INDIVIDUAL"


# ---------------------------------------------------------------------------
# Field presence / column count sanity
# ---------------------------------------------------------------------------


class TestModelFieldPresence:
    def test_all_five_guarantor_blocks_present(self):
        """Verify all 65 guarantor fields (5 x 13) are declared on the model."""
        fields = DebtData.model_fields
        guarantor_fields = [f for f in fields if f.startswith("guarantor")]
        assert len(guarantor_fields) == 65, (
            f"Expected 65 guarantor fields, found {len(guarantor_fields)}: {guarantor_fields}"
        )

    def test_lender_camel_case_fields_present(self):
        fields = DebtData.model_fields
        for name in (
            "lenderPersentTypeCd",
            "lenderNameOrganization",
            "lenderNameLast",
            "lenderNameFirst",
            "lenderNameSuffixCd",
            "lenderNamePrefixCd",
            "lenderNameShort",
            "lenderStreetCity",
            "lenderStreetStateCd",
            "lenderStreetCountyCd",
            "lenderStreetCountryCd",
            "lenderStreetPostalCode",
            "lenderStreetRegion",
        ):
            assert name in fields, f"Expected field {name!r} on DebtData"

    def test_snake_case_ingestion_fields_preserved(self):
        fields = DebtData.model_fields
        assert "file_origin" in fields
        assert "download_date" in fields
        assert "id" in fields

    def test_no_amount_fields(self):
        """Confirm no amount/interest fields were accidentally added."""
        fields = DebtData.model_fields
        forbidden = {"loanAmount", "interestRate", "loanBalance", "maturityDt"}
        present = forbidden & set(fields)
        assert not present, f"Unexpected financial fields found: {present}"
