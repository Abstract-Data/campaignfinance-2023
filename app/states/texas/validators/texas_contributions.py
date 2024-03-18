from datetime import date, datetime
from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from states.texas.validators.texas_settings import TECSettings


class TECContribution(TECSettings, table=True):
    __tablename__ = "tx_contributions"
    __table_args__ = {"schema": "texas"}
    recordType: str
    formTypeCd: str
    schedFormTypeCd: str
    reportInfoIdent: int
    receivedDt: date
    infoOnlyFlag: Optional[bool] = None
    filerIdent: int
    filerTypeCd: str
    filerName: str
    contributionInfoId: int = Field(primary_key=True)
    contributionDt: date
    contributionAmount: float
    contributionDescr: Optional[str] = None
    itemizeFlag: Optional[bool] = None
    travelFlag: Optional[bool] = None
    contributorPersentTypeCd: str = Field(...,
        description="Type of contributor name data - INDIVIDUAL or ENTITY"
    )
    contributorNameOrganization: Optional[str] = Field(default=None,
        description="For ENTITY, the contributor organization name"
    )
    contributorNameLast: Optional[str] = Field(default=None,
        description="For INDIVIDUAL, the contributor last name"
    )
    contributorNameSuffixCd: Optional[str] = Field(default=None,
        description="For INDIVIDUAL, the contributor suffix"
    )
    contributorNameFirst: Optional[str] = Field(default=None,
        description="For INDIVIDUAL, the contributor first name"
    )
    contributorNamePrefixCd: Optional[str] = Field(default=None,
        description="For INDIVIDUAL, the contributor prefix"
    )
    contributorNameShort: Optional[str] = Field(default=None,
        description="For INDIVIDUAL, the contributor short name (nickname)"
    )
    contributorStreetCity: Optional[str] = Field(default=None,
        description="The contributor street address city"
    )
    contributorStreetStateCd: Optional[str] = Field(default=None,
        description="Contributor street address - state code (e.g. TX, CA) - for  \
     country=USA/UMI only")

    contributorStreetCountyCd: Optional[str] = None
    contributorStreetCountryCd: Optional[str] = None
    contributorStreetPostalCode: Optional[str] = None
    contributorStreetRegion: Optional[str] = None
    contributorEmployer: Optional[str] = None
    contributorOccupation: Optional[str] = None
    contributorJobTitle: Optional[str] = None
    contributorPacFein: Optional[str] = Field(
                description="Indicates if contributor is an out-of-state PAC",
            )
    contributorOosPacFlag: Optional[bool] = None
    contributorLawFirmName: Optional[str] = None
    contributorSpouseLawFirmName: Optional[str] = None
    contributorParent1LawFirmName: Optional[str] = None
    contributorParent2LawFirmName: Optional[str] = None
    file_origin: str
    # filer_id: Optional[int]
    # filers: Optional[List]

    @model_validator(mode="before")
    @classmethod
    def _clear_empty_values(cls, values):
        for key, value in values.items():
            if value == "":
                values[key] = None
        return values

    @model_validator(mode="before")
    @classmethod
    def add_filer_id(cls, values):
        values["filer_id"] = values["filerIdent"]
        # values["expenses_id"] = values["filerIdent"]
        return values

    @field_validator("contributionDt", "receivedDt", mode="before")
    @classmethod
    def _check_expend_date(cls, value):
        if value:
            if isinstance(value, str):
                return date(
                    int(str(value[:4])), int(str(value[4:6])), int(str(value[6:8]))
                )

    @model_validator(mode="before")
    @classmethod
    def _check_individual_field(cls, values):
        if values["contributorPersentTypeCd"] == "INDIVIDUAL":
            if not values["contributorNameLast"]:
                raise PydanticCustomError(
                    'individual_field_check',
                    "contributorNameLast is required for INDIVIDUAL contributorPersentTypeCd",
                    {'value': values["contributorNameLast"]}
                )
            if not values["contributorNameFirst"]:
                raise PydanticCustomError(
                    'individual_field_check',
                    "contributorNameFirst is required for INDIVIDUAL contributorPersentTypeCd",
                    {'value': values["contributorNameFirst"]}
                )
        elif values["contributorPersentTypeCd"] == "ENTITY":
            if not values["contributorNameOrganization"]:
                raise PydanticCustomError(
                    'individual_field_check',
                    "contributorNameOrganization is required for ENTITY contributorPersentTypeCd",
                    {'value': values["contributorNameOrganization"]}
                )
        else:
            raise PydanticCustomError(
                'individual_field_check',
                "contributorPersentTypeCd must be INDIVIDUAL or ENTITY",
                {'value': values["contributorPersentTypeCd"]}
            )
        return values

    @model_validator(mode="before")
    @classmethod
    def _check_state_code(cls, values):
        if values["contributorStreetCountryCd"] == "USA":
            if not values["contributorStreetPostalCode"]:
                raise PydanticCustomError(
                    'state_code_check',
                    "contributorStreetPostalCode is required for USA contributorStreetCountryCd",
                    {'value': values["contributorStreetPostalCode"]}
                )
        elif values["contributorStreetCountryCd"] != "UMI":
            if not values["contributorStreetRegion"]:
                raise PydanticCustomError(
                    'state_code_check',
                    "contributorStreetRegion is required for non-USA country",
                    {'value': values["contributorStreetRegion"]}
                )
        else:
            raise PydanticCustomError(
                'state_code_check',
                "contributorStreetCountryCd not valid",
                {'value': values["contributorStreetCountryCd"]}
            )
        return values
