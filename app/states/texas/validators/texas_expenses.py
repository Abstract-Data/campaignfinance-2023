from datetime import date, datetime
from typing import Optional, List
from nameparser import HumanName
from pydantic import field_validator, model_validator
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from states.texas.validators.texas_settings import TECSettings
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs


# class TECExpenseCategory(TECSettings, table=True):
#     recordType: str = Field(..., description="Record type code - always EXCAT")
#     expendCategoryCodeValue: str = Field(..., description="Expenditure category code")
#     expendCategoryCodeLabel: str = Field(..., description="Expenditure category description")


class TECExpense(TECSettings, table=True):
    __tablename__ = "tx_expenses"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID")
    recordType: str = Field(
        ...,
        description="Record type code - always EXPN",
        max_length=20
    )
    formTypeCd: str = Field(
        ...,
        description="TEC form used",
        max_length=20
    )
    schedFormTypeCd: str = Field(
        ...,
        description="TEC Schedule Used",
        max_length=20
    )
    reportInfoIdent: int = Field(
        ...,
        description="Unique report #"
    )
    receivedDt: date = Field(
        ...,
        description="Date report received by TEC"
    )
    infoOnlyFlag: Optional[bool] = Field(
        ...,
        description="Superseded by other report"
    )
    filerIdent: int = Field(
        ...,
        description="Filer account #"
    )
    filerTypeCd: str = Field(
        ...,
        description="Type of filer"
    )
    filerName: str = Field(
        ...,
        description="Filer name"
    )
    expendInfoId: int = Field(
        primary_key=True,
        description="Unique expenditure identifier"
    )
    expendDt: date = Field(
        ...,
        description="Expenditure date"
    )
    expendAmount: float = Field(
        ...,
        description="Expenditure amount"
    )
    expendDescr: str = Field(
        ...,
        description="Expenditure description"
    )
    expendCatCd: Optional[str] = Field(
        default=None,
        description="Expenditure category code"
    )
    expendCatDescr: Optional[str] = Field(
        default=None,
        description="Expenditure category description"
    )
    itemizeFlag: Optional[bool] = Field(
        ...,
        description="Y indicates that the expenditure is itemized"
    )
    travelFlag: Optional[bool] = Field(
        ...,
        description="Y indicates that the expenditure is for travel"
    )
    politicalExpendCd: Optional[bool] = Field(
        ...,
        description="Political expenditure indicator",
    )
    reimburseIntendedFlag: Optional[bool] = Field(
        ...,
        description="Reimbursement intended indicator",
    )
    srcCorpContribFlag: Optional[bool] = Field(
        ...,
        description="Corporate contribution indicator",
    )
    capitalLivingexpFlag: Optional[bool] = Field(
        ...,
        description="Austin living expense indicator",
    )
    payeePersentTypeCd: str = Field(
        ...,
        description="Type of payee name data - INDIVIDUAL or ENTITY"
    )
    payeeNameOrganization: Optional[str] = Field(
        ...,
        description="For ENTITY, the payee organization name"
    )
    payeeNameLast: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the payee last name"
    )
    payeeNameSuffixCd: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the payee suffix"
    )
    payeeNameFirst: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the payee first name"
    )
    payeeNamePrefixCd: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the payee prefix"
    )
    payeeNameShort: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the payee short name"
    )
    payeeNameFull: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the payee full name"
    )
    payeeStreetAddr1: Optional[str] = Field(
        ...,
        description="Payee street address line 1"
    )
    payeeStreetAddr2: Optional[str] = Field(
        default=None,
        description="Payee street address line 2"
    )
    payeeStreetCity: Optional[str] = Field(
        ...,
        description="Payee street address city"
    )
    payeeStreetStateCd: str = Field(
        ...,
        description="Payee street address state code"
    )
    payeeStreetCountyCd: Optional[str] = Field(
        ...,
        description="Payee street address Texas county"
    )
    payeeStreetCountryCd: Optional[str] = Field(
        ...,
        description="Payee street address - country (e.g. USA, UMI, MEX, CAN)",
        max_length=3
    )
    payeeStreetPostalCode: Optional[str] = Field(
        default=None,
        description="Payee street address - postal code - for USA addresses only"
    )
    payeeStreetRegion: Optional[str] = Field(
        default=None,
        description="Payee street address - region for country other than USA"
    )
    creditCardIssuer: Optional[str] = Field(
        default=None,
        description="Financial institution issuing credit card"
    )
    repaymentDt: Optional[date] = Field(
        default=None,
        description="Repayment date"
    )
    file_origin: str = Field(
        ...,
        description="File origin"
    )
    download_date: date = Field(
        ...,
        description="Download date"
    )

    clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)
    check_dates = model_validator(mode='before')(tx_funcs.validate_dates)
    check_zipcodes = model_validator(mode='before')(tx_funcs.check_zipcodes)
    address_formatting = model_validator(mode='before')(tx_funcs.address_formatting)
    phone_number_validation = model_validator(mode='before')(tx_funcs.phone_number_validation)

    @model_validator(mode="before")
    @classmethod
    def _check_payee_field(cls, values):
        if values["payeePersentTypeCd"] == "INDIVIDUAL":
            if not values["payeeNameLast"]:
                raise PydanticCustomError(
                    'payee_field_check',
                    "payeeNameLast is required for INDIVIDUAL payeePersentTypeCd",
                    {
                        'column': 'payeeNameLast',
                        'value': values["payeeNameLast"]
                    }
                )
            if not values["payeeNameFirst"]:
                raise PydanticCustomError(
                    'payee_field_check',
                    "payeeNameFirst is required for INDIVIDUAL payeePersentTypeCd",
                    {
                        'column': 'payeeNameFirst',
                        'value': values["payeeNameFirst"]
                    }
                )
        elif values["payeePersentTypeCd"] == "ENTITY":
            if not values["payeeNameOrganization"]:
                raise PydanticCustomError(
                    'payee_field_check',
                    "payeeNameOrganization is required for ENTITY payeePersentTypeCd",
                    {
                        'column': 'payeeNameOrganization',
                        'value': values["payeeNameOrganization"]
                    }
                )
        else:
            raise PydanticCustomError(
                'payee_field_check',
                "payeePersentTypeCd must be INDIVIDUAL or ENTITY",
                {
                    'column': 'payeePersentTypeCd',
                    'value': values["payeePersentTypeCd"]
                }
            )
        return values


    # @model_validator(mode="before")
    # @classmethod
    # def _check_entity_or_individual(cls, values):
    #     if not values["payeePersentTypeCd"] == "INDIVIDUAL":
    #         if values["payeeNameFirst"] and values["payeeNameLast"]:
    #             values["payeePersentTypeCd"] = "INDIVIDUAL"
    #         else:
    #             values["payeePersentTypeCd"] = "ENTITY"
    #     return values

    @model_validator(mode="before")
    @classmethod
    def format_payee_name(cls, values):
        if values["payeePersentTypeCd"] == "INDIVIDUAL":
            _payee_name_fields = [
                x for x in values.keys() if x.startswith("payeeName") and x != "payeeNameOrganization"
            ]
            _name_fields_not_empty = [values[x] for x in _payee_name_fields if values[x] != ""]
            payee_name = funcs.person_name_parser(" ".join(_name_fields_not_empty))
            payee_name.parse_full_name()
            values["payeeNameLast"] = payee_name.last
            values["payeeNameFirst"] = payee_name.first
            values["payeeNameSuffixCd"] = payee_name.suffix
            values["payeeNamePrefixCd"] = payee_name.title
            values["payeeNameFull"] = payee_name.full_name

        return values
