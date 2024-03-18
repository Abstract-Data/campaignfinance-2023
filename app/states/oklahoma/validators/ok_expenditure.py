from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator, AliasChoices
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from ok_settings import OklahomaSettings

"""
Oklahoma Expenditure Model/Validator 
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKExpendituresAndTransfersOutFileLayout.pdf
"""


class OklahomaExpenditure(OklahomaSettings, table=True):
    id: int = Field(
        default=None,
        primary_key=True,
        description="This is the unique ID of the paying candidate or committee.")
    expendType: str = Field(
        description="Indicates Type of Expenditure / Transfer.",
        alias="EXPENDITURE / TRANSFER TYPE")
    expendAmount: float = Field(
        description="Expenditure / Transfer Amount",
        alias='EXPENDITURE /TRANSFER AMOUNT')
    expendDesc: str = Field(
        description="This is the description provided for the expenditure / transfer",
        alias="DESCRIPTION")
    committeeType: str = Field(
        description="Indicates Type of paying committee",
        alias="COMMITTEE TYPE")
    committeeName: str = Field(
        description="This is the name of the paying committee",
        alias="COMMITTEE NAME")
    candidateName: str = Field(
        description="This is the name of the paying candidate",
        alias="CANDIDATE NAME")
    recipientFirstName: Optional[str] = Field(
        description="Recipient First Name",
        alias="FIRST NAME")
    middleName: Optional[str] = Field(
        description="Recipient Middle Initial or Name if provided",
        alias="MIDDLE NAME")
    lastName: Optional[str] = Field(
        description="Last Name of Recipient (entity paid), if an individual person. "
                    "If not an individual, the entity full name will be in LAST NAME field",
        alias="LAST NAME")
    suffix: Optional[str] = Field(
        description="Recipient Name Suffix",
        alias="SUFFIX")
    address1: Optional[str] = Field(
        description="Recipient Street, PO Box, or other directional information",
        alias="ADDRESS 1")
    address2: Optional[str] = Field(
        description="Recipient Suite/Apartment number, or other directional information",
        alias="ADDRESS 2")
    city: Optional[str] = Field(
        description="Recipient City",
        alias="CITY")
    state: Optional[str] = Field(
        description="Recipient State",
        alias="STATE")
    zip: Optional[str] = Field(
        description="Recipient Zip Code",
        alias="ZIP")
    expendId: int = Field(
        description="This is the Expenditure / Transfer internal ID. This ID is unique.",
        alias="EXPENDITURE / TRANSFER ID")
    amended: bool = Field(
        default=None,
        description="Y/N indicator to show if an amendment was filed for this record.",
        alias="AMENDED")
