from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator, AliasChoices
from sqlmodel import SQLModel, Field
from pydantic_core import PydanticCustomError
from datetime import date
from .ok_settings import OklahomaSettings
import funcs.validator_functions as funcs

"""
Oklahoma Expenditure Model/Validator 
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKLobbyistExpendituresFileLayout.pdf
"""


class OklahomaLobbyistExpenditure(OklahomaSettings, table=True):
    __tablename__ = 'lobby_expenses'
    __table_args__ = {'schema': 'oklahoma'}
    id: Optional[int] = Field(default=None, primary_key=True)
    expenditure_id: int = Field(..., title="Expenditure ID")
    lobbyistid: int = Field(..., title="Lobbyist ID")
    lobbyist_firstname: Optional[str] = Field(default=None, title="Lobbyist First Name")
    lobbyist_middlename: Optional[str] = Field(default=None, title="Lobbyist Middle Name")
    lobbyist_lastname: str = Field(..., title="Lobbyist Last Name")
    lobbyist_suffix: Optional[str] = Field(default=None, title="Lobbyist Suffix")
    expenditure_type: str = Field(..., title="Expenditure Type")
    expenditure_date: date = Field(..., title="Expenditure Date")
    expenditure_cost: float = Field(..., title="Expenditure Cost")
    meal_type: Optional[str] = Field(default=None, title="Meal Type")
    other_meal_description: Optional[str] = Field(default=None, title="Other Meal Description")
    explanation: Optional[str] = Field(default=None, title="Explanation")
    recipient_first_name: Optional[str] = Field(default=None, title="Recipient First Name")
    recipient_middle_name: Optional[str] = Field(default=None, title="Recipient Middle Name")
    recipient_last_name: Optional[str] = Field(default=None, title="Recipient Last Name")
    recipient_suffix: Optional[str] = Field(default=None, title="Recipient Suffix")
    recipient_type: Optional[str] = Field(default=None, title="Recipient Type")
    recipient_title: Optional[str] = Field(default=None, title="Recipient Title")
    recipient_agency_office: Optional[str] = Field(default=None, title="Recipient Agency Office")
    relationship_to_state_officer_or_employee: Optional[str] = Field(default=None, title="Relationship to State Officer or Employee")
    family_member_name: Optional[str] = Field(default=None, title="Family Member Name")
    principal_name: Optional[str] = Field(default=None, title="Principal Name")
    principal_percentage_cost: Optional[str] = Field(default=None, title="Principal Percentage Cost")
    caucus: Optional[str] = Field(default=None, title="Caucus")
    committee_subcommittee: Optional[str] = Field(default=None, title="Committee Subcommittee")
    event_location: Optional[str] = Field(default=None, title="Event Location")
    event_city: Optional[str] = Field(default=None, title="Event City")
    event_state: Optional[str] = Field(default=None, title="Event State", max_length=2)
    download_date: date
    file_origin: str

    clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)

    validate_dates = field_validator(
        'expenditure_date',
        mode='before')(lambda v: funcs.validate_date(v, fmt='%m/%d/%Y'))

    @model_validator(mode='before')
    @classmethod
    def parse_candidate_name(cls, values):
        if 'lobbyist_firstname' in values:
            _lobbyist_name_components = [
                values[x] for x in [
                    'lobbyist_firstname',
                    'lobbyist_middlename',
                    'lobbyist_lastname',
                    'lobbyist_suffix'
                ] if x is not None
            ]
            name = funcs.person_name_parser(' '.join(_lobbyist_name_components))
            values['lobbyist_firstname'] = name.first
            values['lobbyist_lastname'] = name.last
            values['lobbyist_middlename'] = name.middle
            values['lobbyist_suffix'] = name.suffix

        if 'recipient_first_name' in values:
            _recipient_name_components = [
                values[x] for x in [
                    'recipient_first_name',
                    'recipient_middle_name',
                    'recipient_last_name',
                    'recipient_suffix'
                ] if x is not None
            ]
            name = funcs.person_name_parser(' '.join(_recipient_name_components))
            values['recipient_first_name'] = name.first
            values['recipient_lastname'] = name.last
            values['recipient_middlename'] = name.middle
            values['recipient_suffix'] = name.suffix
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_meal_type(cls, values):
        if 'meal_type' in values:
            if values['meal_type'].upper() in ['BREAKFAST', 'DINNER', 'LUNCH', 'OTHER']:
                return values
            elif values['meal_type'] == '':
                values['meal_type'] = None
                return values
            else:
                raise PydanticCustomError(
                    'invalid_type',
                    f"{values['meal_type']} is not a valid meal type",
                    {
                        'column': 'meal_type',
                        'value': values['meal_type']
                    }
                )

    @model_validator(mode='before')
    @classmethod
    def validate_recipient_type(cls, values):
        if 'recipient_type' in values:
            if values['recipient_type'].upper() in ['LEGISLATOR', 'NON-LEGISLATOR STATE OFFICER OR EMPLOYEE']:
                return values
            elif values['recipient_type'] == '':
                values['recipient_type'] = None
                return values
            else:
                raise PydanticCustomError(
                    'invalid_type',
                    f"{values['recipient_type']} is not a valid recipient type",
                    {
                        'column': 'recipient_type',
                        'value': values['recipient_type']
                    }
                )
