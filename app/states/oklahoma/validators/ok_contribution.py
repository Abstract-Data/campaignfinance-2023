from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator, AliasChoices
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from datetime import date
from ok_settings import OklahomaSettings

"""
Oklahoma Expenditure Model/Validator 
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKReceiptsAndTransfersInFileLayout.pdf
"""


class OklahomaContribution(OklahomaSettings, table=True):
    id: int = Field(
        default=None,
        primary_key=True,
        description="This is the unique ID of the paying candidate or committee.",
        alias="RECEIPT / TRANSFER ID")
    orgId: int = Field(
        description="This is the unique ID of the receiving candidate or committee.",
        alias="ORG ID")
    contribType: str = Field(
        description="This is the Receipt / Transfer Type.",
        alias="RECEIPT / TRANSFER TYPE")
    contribDate: date = Field(
        description="Receipt / Transfer Date",
        alias="RECEIPT / TRANSFER DATE")
    contribAmount: float = Field(
        description="Receipt / Transfer Amount",
        alias='RECEIPT / TRANSFER AMOUNT')
    contribDesc: str = Field(
        description="This is the description provided for the receipt / transfer.",
        alias="DESCRIPTION")
    contribSourceType: str = Field(
        description="Type of entity that is the source of the Receipt / Transfer.",
        alias="RECEIPT / TRANSFER SOURCE TYPE")
    firstName: Optional[str] = Field(
        description="Source First Name",
        alias="FIRST NAME")
    middleName: Optional[str] = Field(
        description="Source Middle Initial or Name if provided",
        alias="MIDDLE NAME")
    lastName: Optional[str] = Field(
        description="Source Last Name",
        alias="LAST NAME")
    suffix: Optional[str] = Field(
        description="Source Name Suffix",
        alias="SUFFIX")
    spouseName: Optional[str] = Field(
        description="Source Spouse Name",
        alias="SPOUSE NAME")
    address1: Optional[str] = Field(
        description="Source Street, PO Box, or other directional information",
        alias="ADDRESS 1")
    address2: Optional[str] = Field(
        description="Source Suite/Apartment number, or other directional information",
        alias="ADDRESS 2")
    city: Optional[str] = Field(
        description="Source City",
        alias="CITY")
    state: Optional[str] = Field(
        description="Source State",
        alias="STATE")
    zip: Optional[str] = Field(
        description="Source Zip Code",
        alias="ZIP")
    filedDate: date = Field(
        description="Receipt / Transfer Filed Date",
        alias="FILED DATE")
    committeeType: str = Field(
        description="Indicates type of receiving committee",
        alias="COMMITTEE TYPE")
    committeeName: str = Field(
        description="This is the name of the receiving committee",
        alias="COMMITTEE NAME")
    candidateName: str = Field(
        description="This is the name of the receiving candidate",
        alias="CANDIDATE NAME")
    amended: bool = Field(
        default=None,
        description="Y/N indicator to show if an amendment was filed for this record.",
        alias="AMENDED")
    employer: Optional[str] = Field(
        description="Source’s employer displays in cases where this information is provided.",
        alias="EMPLOYER")
    occupation: Optional[str] = Field(
        description="The Source’s occupation in cases where this information is provided. "
                    "Only used for Individual donors.",
        alias="OCCUPATION")
