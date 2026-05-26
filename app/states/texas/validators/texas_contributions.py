from datetime import date, datetime

from pydantic import field_validator, model_validator
from pydantic_core import PydanticCustomError
from sqlmodel import Field

import app.states.texas.funcs.tx_validation_funcs as tx_funcs
from app.abcs.base_models import CreateValidatorModel

from .texas_settings import TECSettings


class TECContributionBase(CreateValidatorModel, TECSettings):
    recordType: str = Field(
        ...,
        description="Record type code - always RCPT"
    )
    formTypeCd: str = Field(
        ...,
        description="TEC form used"
    )
    schedFormTypeCd: str = Field(
        ...,
        description="TEC Schedule Used"
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
        default=None,
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
    contributionInfoId: int = Field(
        primary_key=True,
        description="Contribution unique identifier"
    )
    contributionDt: date = Field(
        ...,
        description="Contribution date"
    )
    contributionAmount: float = Field(
        ...,
        description="Contribution amount"
    )
    contributionDescr: Optional[str] = Field(
        default=None,
        description="Contribution description"
    )
    itemizeFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution is itemized"
    )
    travelFlag: Optional[bool] = Field(
        default=None,
        description="Y indicates that the contribution has associated travel"
    )
    contributorPersentTypeCd: str = Field(...,
                                          description="Type of contributor name data - INDIVIDUAL or ENTITY"
                                          )
    contributorNameOrganization: Optional[str] = Field(
        default=None,
        description="For ENTITY, the contributor organization name"
    )
    contributorNameLast: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the contributor last name"
    )
    contributorNameSuffixCd: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the contributor suffix"
    )
    contributorNameFirst: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the contributor first name"
    )
    contributorNamePrefixCd: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the contributor prefix"
    )
    contributorNameShort: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the contributor short name (nickname)"
    )
    contributorNameFull: Optional[str] = Field(
        default=None,
        description="For INDIVIDUAL, the contributor full name"
    )
    contributorStreetCity: Optional[str] = Field(
        default=None,
        description="The contributor street address city"
    )
    contributorStreetStateCd: Optional[str] = Field(
        default=None,
        description="Contributor street address - state code (e.g. TX, CA) - for  \
     country=USA/UMI only"
    )
    contributorStreetCountyCd: Optional[str] = Field(
        default=None,
        description="Contributor street address - Texas county")
    contributorStreetCountryCd: Optional[str] = Field(
        default=None,
        description="Contributor street address - country (e.g. USA, UMI, MEX, CAN)"
    )
    contributorStreetPostalCode: Optional[str] = Field(
        default=None,
        description="Contributor street address - postal code - for USA addresses only"
    )
    contributorStreetRegion: Optional[str] = Field(
        default=None,
        description="Contributor street address - region for country other than USA"
    )
    contributorEmployer: Optional[str] = Field(
        default=None,
        description="Contributor employer"
    )
    contributorOccupation: Optional[str] = Field(
        default=None,
        description="Contributor occupation"
    )
    contributorJobTitle: Optional[str] = Field(
        default=None,
        description="Contributor job title"
    )
    contributorPacFein: Optional[str] = Field(
        description="FEC ID of out-of-state PAC contributor",
    )
    contributorOosPacFlag: Optional[bool] = Field(
        default=None,
        description="Indicates if contributor is an out-of-state PAC "
    )
    contributorLawFirmName: Optional[str] = Field(
        default=None,
        description="Contributor law firm name"
    )
    contributorSpouseLawFirmName: Optional[str] = Field(
        default=None,
        description="Contributor spouse law firm name"
    )
    contributorParent1LawFirmName: Optional[str] = Field(
        default=None,
        description="Contributor parent #1 law firm name"
    )
    contributorParent2LawFirmName: Optional[str] = Field(
        default=None,
        description="Contributor parent #2 law firm name"
    )
    file_origin: str = Field(
        ...,
        description="The file origin of the data"
    )
    download_date: date = Field(
        ...,
        description="The date the data was downloaded"
    )

    # filer_id: Optional[int]
    # filers: Optional[List]

    @model_validator(mode="before")
    @classmethod
    def clear_blank_strings(cls, values):
        """
        Clear out all blank strings or ones that contain 'null' from records.
        :param cls:
        :param values:
        :return:
        """
        for k, v in values.items():
            if v in ["", '"', "null"]:
                values[k] = None
        return values

    # Contribution records use a stricter blank-string rule than funcs.clear_blank_strings.
    check_phone_numbers = model_validator(mode="before")(tx_funcs.phone_number_validation)
    check_address_format = model_validator(mode="before")(tx_funcs.address_formatting)

    @model_validator(mode="before")
    @classmethod
    def format_contributor_name(cls, values):
        if values["contributorPersentTypeCd"] == "INDIVIDUAL":
            if values["contributorNameLast"] and values["contributorNameFirst"]:
                values["contributorNameFull"] = f"{values['contributorNameFirst']} {values['contributorNameLast']}"

        return values

    @model_validator(mode="before")
    @classmethod
    def _check_individual_field(cls, values):
        if values["contributorPersentTypeCd"] == "INDIVIDUAL":
            if not values["contributorNameLast"]:
                raise PydanticCustomError(
                    'missing_required_value',
                    "contributorNameLast is required for INDIVIDUAL contributorPersentTypeCd",
                    {
                        'column': 'contributorNameLast',
                        'value': values["contributorNameLast"]}
                )
        elif values["contributorPersentTypeCd"] == "ENTITY":
            if not values["contributorNameOrganization"]:
                raise PydanticCustomError(
                    'individual_field_check',
                    "contributorNameOrganization is required for ENTITY contributorPersentTypeCd",
                    {
                        'column': 'contributorNameOrganization',
                        'value': values["contributorNameOrganization"]}
                )
        else:
            pass
        return values

    @field_validator('contributorPersentTypeCd', 'receivedDt', 'filerName', mode='before')
    @classmethod
    def validate_contributor_type(cls, v):
        if not v:
            raise PydanticCustomError(
                'missing_required_value',
                "contributorPersentTypeCd must be INDIVIDUAL or ENTITY",
                {
                    'column': 'contributorPersentTypeCd',
                    'value': v
                }
            )
        return v


class TECContributionCreate(TECContributionBase):
    """Ingestion/API create shape — excludes server-set ``id``."""


class TECContributionRead(TECContributionBase):
    """Downstream read shape — includes server-set metadata."""

    id: str
    created_at: datetime


class TECContribution(TECContributionBase, table=True):
    """Database table model for Texas contributions."""

    __tablename__ = "tx_contributions"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(
        default=None,
        description="Unique identifier",
    )
