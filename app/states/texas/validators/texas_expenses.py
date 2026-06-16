from datetime import date
from typing import Optional

from pydantic import field_validator, model_validator
from pydantic_core import PydanticCustomError
from sqlmodel import Field

import app.states.texas.funcs.tx_validation_funcs as tx_funcs

from ._mixins import format_individual_payee_name, validate_individual_entity_discriminator
from .texas_settings import TECSettings


class TECExpense(TECSettings, table=True):
    __tablename__ = "tx_expenses"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID")
    recordType: str = Field(..., description="Record type code - always EXPN", max_length=20)
    formTypeCd: str = Field(..., description="TEC form used", max_length=20)
    schedFormTypeCd: str = Field(..., description="TEC Schedule Used", max_length=20)
    reportInfoIdent: int = Field(..., description="Unique report #")
    receivedDt: date = Field(..., description="Date report received by TEC")
    # Optional — some rows omit the flag; was incorrectly marked required.
    infoOnlyFlag: Optional[bool] = Field(default=None, description="Superseded by other report")
    filerIdent: int = Field(..., description="Filer account #")
    filerTypeCd: str = Field(..., description="Type of filer")
    filerName: str = Field(..., description="Filer name")
    expendInfoId: int = Field(primary_key=True, description="Unique expenditure identifier")
    expendDt: date = Field(..., description="Expenditure date")
    # Optional — a small number of records legitimately have no amount (e.g. in-kind)
    expendAmount: Optional[float] = Field(default=None, description="Expenditure amount")
    expendDescr: str = Field(..., description="Expenditure description")
    expendCatCd: Optional[str] = Field(default=None, description="Expenditure category code")
    expendCatDescr: Optional[str] = Field(
        default=None, description="Expenditure category description"
    )
    # Boolean flags — truly optional; absent on some older records
    itemizeFlag: Optional[bool] = Field(
        default=None, description="Y indicates that the expenditure is itemized"
    )
    travelFlag: Optional[bool] = Field(
        default=None, description="Y indicates that the expenditure is for travel"
    )
    politicalExpendCd: Optional[bool] = Field(
        default=None, description="Political expenditure indicator"
    )
    reimburseIntendedFlag: Optional[bool] = Field(
        default=None, description="Reimbursement intended indicator"
    )
    srcCorpContribFlag: Optional[bool] = Field(
        default=None, description="Corporate contribution indicator"
    )
    capitalLivingexpFlag: Optional[bool] = Field(
        default=None, description="Austin living expense indicator"
    )
    payeePersentTypeCd: str = Field(
        ..., description="Type of payee name data - INDIVIDUAL or ENTITY"
    )
    # ENTITY only
    payeeNameOrganization: Optional[str] = Field(
        default=None, description="For ENTITY, the payee organization name"
    )
    # INDIVIDUAL only
    payeeNameLast: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the payee last name"
    )
    payeeNameSuffixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the payee suffix"
    )
    payeeNameFirst: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the payee first name"
    )
    payeeNamePrefixCd: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the payee prefix"
    )
    payeeNameShort: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the payee short name"
    )
    payeeNameFull: Optional[str] = Field(
        default=None, description="For INDIVIDUAL, the payee full name"
    )
    # Address — city/state/country are expected; addr1 and county are often blank
    payeeStreetAddr1: Optional[str] = Field(default=None, description="Payee street address line 1")
    payeeStreetAddr2: Optional[str] = Field(default=None, description="Payee street address line 2")
    payeeStreetCity: Optional[str] = Field(default=None, description="Payee street address city")
    payeeStreetStateCd: str = Field(..., description="Payee street address state code")
    payeeStreetCountyCd: Optional[str] = Field(
        default=None, description="Payee street address Texas county"
    )
    payeeStreetCountryCd: Optional[str] = Field(
        default=None,
        description="Payee street address - country (e.g. USA, UMI, MEX, CAN)",
        max_length=3,
    )
    payeeStreetPostalCode: Optional[str] = Field(
        default=None, description="Payee street address - postal code - for USA addresses only"
    )
    payeeStreetRegion: Optional[str] = Field(
        default=None, description="Payee street address - region for country other than USA"
    )
    creditCardIssuer: Optional[str] = Field(
        default=None, description="Financial institution issuing credit card"
    )
    repaymentDt: Optional[date] = Field(default=None, description="Repayment date")
    file_origin: str = Field(..., description="File origin", max_length=64)
    download_date: date = Field(..., description="Download date")

    address_formatting = model_validator(mode="before")(tx_funcs.address_formatting)
    phone_number_validation = model_validator(mode="before")(tx_funcs.phone_number_validation)

    @model_validator(mode="before")
    @classmethod
    def _check_payee_field(cls, values):
        person_type = values.get("payeePersentTypeCd", "")
        if person_type not in ("INDIVIDUAL", "ENTITY"):
            raise PydanticCustomError(
                "incorrect_value",
                "payeePersentTypeCd must be INDIVIDUAL or ENTITY",
                {
                    "column": "payeePersentTypeCd",
                    "value": values.get("payeePersentTypeCd"),
                },
            )
        values = validate_individual_entity_discriminator(
            values,
            type_field="payeePersentTypeCd",
            individual_name_field="payeeNameLast",
            entity_org_field="payeeNameOrganization",
        )
        if person_type == "INDIVIDUAL" and not values.get("payeeNameFirst"):
            raise PydanticCustomError(
                "missing_required_value",
                "payeeNameFirst is required for INDIVIDUAL payeePersentTypeCd",
                {
                    "column": "payeeNameFirst",
                    "value": values.get("payeeNameFirst"),
                },
            )
        return values

    @model_validator(mode="before")
    @classmethod
    def format_payee_name(cls, values):
        return format_individual_payee_name(values)

    @field_validator(
        "filerName", "expendDescr", "payeeStreetStateCd", "expendDt", "receivedDt", mode="before"
    )
    @classmethod
    def validate_required_fields(cls, v):
        if v == "" or v is None:
            raise PydanticCustomError(
                "missing_required_value", "Field is required", {"column": "filerName", "value": v}
            )
        return v
