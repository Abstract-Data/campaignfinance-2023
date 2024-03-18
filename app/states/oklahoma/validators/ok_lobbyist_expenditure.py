from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator, AliasChoices
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from datetime import date
from ok_settings import OklahomaSettings

"""
Oklahoma Expenditure Model/Validator 
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKLobbyistExpendituresFileLayout.pdf
"""


class OklahomaLobbyistExpenditure(OklahomaSettings, table=True):
    expenditureId: int = Field(
        alias="EXPENDITURE ID",
        description="This is the Expenditure internal ID. This ID is unique.")
    lobbyistId: int = Field(
        alias="LOBBYIST ID",
        description="This is the unique ID of the lobbyist.")
    lobbyistFirstName: str = Field(
        alias="LOBBYIST FIRST NAME",
        description="Lobbyist First Name")
    lobbyistMiddleName: Optional[str] = Field(
        alias="LOBBYIST MIDDLE NAME",
        description="Lobbyist Middle Initial or Name if provided.")
    lobbyistLastName: str = Field(
        alias="LOBBYIST LAST NAME",
        description="Last Name of Lobbyist.")
    lobbyistSuffix: Optional[str] = Field(
        alias="LOBBYIST SUFFIX",
        description="Lobbyist Name Suffix")
    expenditureType: str = Field(
        alias="EXPENDITURE TYPE",
        description="Indicates Type of Expenditure.")
    expenditureDate: date = Field(
        alias="EXPENDITURE DATE",
        description="Expenditure Date")
    expenditureCost: float = Field(
        alias="EXPENDITURE COST",
        description="Expenditure Cost")
    mealType: Optional[str] = Field(
        alias="MEAL TYPE",
        description="Meal Type")
    otherMealDescription: Optional[str] = Field(
        alias="OTHER MEAL DESCRIPTION",
        description="Other Meal Description")
    explanation: Optional[str] = Field(
        alias="EXPLANATION",
        description="This is the explanation provided for the expenditure.")
    recipientFirstName: str = Field(
        alias="RECIPIENT FIRST NAME",
        description="Recipient First Name")
    recipientMiddleName: Optional[str] = Field(
        alias="RECIPIENT MIDDLE NAME",
        description="Recipient Middle Name")
    recipientLastName: str = Field(
        alias="RECIPIENT LAST NAME",
        description="Recipient Last Name")
    recipientSuffix: Optional[str] = Field(
        alias="RECIPIENT SUFFIX",
        description="Recipient Suffix")
    recipientType: str = Field(
        alias="RECIPIENT TYPE",
        description="Legislator or Non-Legislator State Officer or Employee")
    recipientTitle: Optional[str] = Field(
        alias="RECIPIENT TITLE",
        description="Recipient Title")
    recipientAgencyOffice: Optional[str] = Field(
        alias="RECIPIENT AGENCY/OFFICE",
        description="Recipient Agency/Office Name")
    relationshipToStateOfficerOrEmployee: Optional[str] = Field(
        alias="RELATIONSHIP TO STATE OFFICER OR EMPLOYEE",
        description="Relationship to State Officer or Employee if recipient is family member")
    familyMemberName: Optional[str] = Field(
        alias="FAMILY MEMBER NAME",
        description="Family Member Name")
    principalName: str = Field(
        alias="PRINCIPAL NAME",
        description="This is the name of the lobbyist principal related to the expenditure.")
    principalPercentageOfCost: float = Field(
        alias="PRINCICPAL’S PERCENTAGE OF COST",
        description="Principal’s Percentage of Cost")
    caucus: Optional[str] = Field(
        alias="CAUCUS",
        description="Caucus Name")
    committeeSubcommittee: Optional[str] = Field(
        alias="COMMITTEE/SUBCOMMITTEE",
        description="Committee/Subcommittee Name")
    eventLocation: Optional[str] = Field(
        alias="EVENT LOCATION",
        description="Event Location")
    eventCity: Optional[str] = Field(
        alias="EVENT CITY",
        description="Event City")
    eventState: Optional[str] = Field(
        alias="EVENT STATE",
        description="Event State")
