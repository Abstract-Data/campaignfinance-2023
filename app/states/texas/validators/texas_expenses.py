from datetime import date, datetime
from typing import Optional, List
from nameparser import HumanName
from pydantic import field_validator, model_validator, Field
from pydantic_core import PydanticCustomError
from states.texas.validators.texas_settings import TECSettings


class TECExpenseCategory(TECSettings):
    recordType: str = Field(..., description="Record type code - always EXCAT")
    expendCategoryCodeValue: str = Field(..., description="Expenditure category code")
    expendCategoryCodeLabel: str = Field(..., description="Expenditure category description")


class TECExpense(TECSettings):
    recordType: str
    formTypeCd: str
    schedFormTypeCd: str
    reportInfoIdent: int
    receivedDt: date
    infoOnlyFlag: Optional[bool]
    filerIdent: int
    filerTypeCd: str
    filerName: str
    expendInfoId: int
    expendDt: date
    expendAmount: float
    expendDescr: str
    expendCatCd: str
    expendCatDescr: str
    itemizeFlag: Optional[bool]
    travelFlag: Optional[bool]
    politicalExpendCd: Optional[str]
    reimburseIntendedFlag: Optional[str]
    srcCorpContribFlag: Optional[str]
    capitalLivingexpFlag: Optional[str]
    payeePersentTypeCd: str
    payeeNameOrganization: Optional[str]
    payeeNameLast: Optional[str]
    payeeNameSuffixCd: Optional[str]
    payeeNameFirst: Optional[str]
    payeeNamePrefixCd: Optional[str]
    payeeNameShort: Optional[str]
    payeeStreetAddr1: str
    payeeStreetAddr2: Optional[str]
    payeeStreetCity: str
    payeeStreetStateCd: str
    payeeStreetCountyCd: str
    payeeStreetCountryCd: str
    payeeStreetPostalCode: Optional[str]
    payeeStreetRegion: Optional[str]
    creditCardIssuer: Optional[str]
    repaymentDt: Optional[date]
    file_origin: str
    # filer_id: Optional[int]
    # filers: Optional[List]

    @model_validator(mode="before")
    @classmethod
    def add_filer_id(cls, values):
        values["filer_id"] = values["filerIdent"]
        # values["contribution_id"] = values["filerIdent"]
        return values

    @field_validator("expendDt", "repaymentDt", "receivedDt", mode="before")
    @classmethod
    def _check_expend_date(cls, value):
        if value:
            if isinstance(value, str):
                try:
                    return date(
                        int(str(value[:4])), int(str(value[4:6])), int(str(value[6:8]))
                    )
                except ValueError:
                    raise PydanticCustomError(
                        'date_check',
                        f"{value} must be in YYYYMMDD format",
                        {'value': value}
                    )


    # _expense_expend_dt = format_dates("expendDt")
    # _expense_repayment_dt = field_validator("repaymentDt", mode="before")(classmethod, format_dates)

    @model_validator(mode="before")
    @classmethod
    def _check_payee_field(cls, values):
        if values["payeePersentTypeCd"] == "INDIVIDUAL":
            if not values["payeeNameLast"]:
                raise PydanticCustomError(
                    'payee_field_check',
                    "payeeNameLast is required for INDIVIDUAL payeePersentTypeCd",
                    {'value': values["payeeNameLast"]}
                )
            if not values["payeeNameFirst"]:
                raise PydanticCustomError(
                    'payee_field_check',
                    "payeeNameFirst is required for INDIVIDUAL payeePersentTypeCd",
                    {'value': values["payeeNameFirst"]}
                )
        elif values["payeePersentTypeCd"] == "ENTITY":
            if not values["payeeNameOrganization"]:
                raise PydanticCustomError(
                    'payee_field_check',
                    "payeeNameOrganization is required for ENTITY payeePersentTypeCd",
                    {'value': values["payeeNameOrganization"]}
                )
        else:
            raise PydanticCustomError(
                'payee_field_check',
                "payeePersentTypeCd must be INDIVIDUAL or ENTITY",
                {'value': values["payeePersentTypeCd"]}
            )
        return values

    @model_validator(mode="before")
    @classmethod
    def _check_person_name(cls, values):
        def complete_name_cols(person: HumanName):
            values["payeeNameLast"] = person.last
            values["payeeNameSuffixCd"] = person.suffix
            values["payeeNameFirst"] = person.first
            values["payeeNamePrefixCd"] = person.title
            values["payeeNameShort"] = person.nickname
            return values

        if values["payeePersentTypeCd"] == "INDIVIDUAL":
            if values["payeeNameFirst"] and not values["payeeNameLast"]:
                name = HumanName(values["payeeNameFirst"])
                if name.last:
                    complete_name_cols(name)
                else:
                    raise PydanticCustomError(
                        'person_name_check',
                        "payeeNameLast is required if payeeNameFirst is provided",
                        {'value': values["payeeNameLast"]}
                    )

            elif values["payeeNameLast"] and not values["payeeNameFirst"]:
                name = HumanName(values["payeeNameLast"])
                if name.first:
                    complete_name_cols(name)
                else:
                    raise PydanticCustomError(
                        'person_name_check',
                        "payeeNameFirst is required if payeeNameLast is provided",
                        {'value': values["payeeNameFirst"]}
                    )
            else:
                pass
        return values

    @model_validator(mode="before")
    @classmethod
    def _check_entity_or_individual(cls, values):
        if not values["payeePersentTypeCd"] == "INDIVIDUAL":
            if values["payeeNameFirst"] and values["payeeNameLast"]:
                values["payeePersentTypeCd"] = "INDIVIDUAL"
            else:
                values["payeePersentTypeCd"] = "ENTITY"
        return values
