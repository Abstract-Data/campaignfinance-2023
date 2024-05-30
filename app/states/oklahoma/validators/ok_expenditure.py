from typing import Optional, Annotated, List
from pydantic import field_validator, model_validator, AliasChoices
from sqlmodel import SQLModel, Field
from sqlmodel.main import PydanticFieldInfo
from pydantic_core import PydanticCustomError
from .ok_settings import OklahomaSettings
import funcs.validator_functions as funcs
from datetime import date

"""
Oklahoma Expenditure Model/Validator 
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKExpendituresAndTransfersOutFileLayout.pdf
"""


# class OklahomaExpenditure(OklahomaSettings):
#     id: int = Field(
#         default=None,
#         primary_key=True,
#         description="This is the unique ID of the paying candidate or committee.")
#     expendType: str = Field(
#         description="Indicates Type of Expenditure / Transfer.",
#         title="EXPENDITURE / TRANSFER TYPE")
#     expendAmount: float = Field(
#         description="Expenditure / Transfer Amount",
#         title='EXPENDITURE /TRANSFER AMOUNT')
#     expendDesc: str = Field(
#         description="This is the description provided for the expenditure / transfer",
#         title="DESCRIPTION")
#     committeeType: str = Field(
#         description="Indicates Type of paying committee",
#         title="COMMITTEE TYPE")
#     committeeName: str = Field(
#         description="This is the name of the paying committee",
#         title="COMMITTEE NAME")
#     candidateName: str = Field(
#         description="This is the name of the paying candidate",
#         title="CANDIDATE NAME")
#     recipientFirstName: Optional[str] = Field(
#         description="Recipient First Name",
#         title="FIRST NAME")
#     middleName: Optional[str] = Field(
#         description="Recipient Middle Initial or Name if provided",
#         title="MIDDLE NAME")
#     lastName: Optional[str] = Field(
#         description="Last Name of Recipient (entity paid), if an individual person. "
#                     "If not an individual, the entity full name will be in LAST NAME field",
#         title="LAST NAME")
#     suffix: Optional[str] = Field(
#         description="Recipient Name Suffix",
#         title="SUFFIX")
#     address1: Optional[str] = Field(
#         description="Recipient Street, PO Box, or other directional information",
#         title="ADDRESS 1")
#     address2: Optional[str] = Field(
#         description="Recipient Suite/Apartment number, or other directional information",
#         title="ADDRESS 2")
#     city: Optional[str] = Field(
#         description="Recipient City",
#         title="CITY")
#     state: Optional[str] = Field(
#         description="Recipient State",
#         title="STATE")
#     zip: Optional[str] = Field(
#         description="Recipient Zip Code",
#         title="ZIP")
#     expendId: int = Field(
#         description="This is the Expenditure / Transfer internal ID. This ID is unique.",
#         title="EXPENDITURE / TRANSFER ID")
#     amended: bool = Field(
#         default=None,
#         description="Y/N indicator to show if an amendment was filed for this record.",
#         title="AMENDED")

class OklahomaExpenditure(OklahomaSettings, table=True):
    __tablename__ = 'expenditures'
    __table_args__ = {'schema': 'oklahoma'}
    id: int = Field(default=None)
    expenditure_id: Optional[int] = Field(default=None, title='Expenditure ID', primary_key=True)
    org_id: Optional[int] = Field(default=None, title='Org ID')
    expenditure_type: str = Field(..., title='Expenditure Type')
    expenditure_date: date = Field(..., title='Expenditure Date')
    expenditure_amount: Optional[float] = Field(default=0.00, title='Expenditure Amount')
    description: Optional[str] = Field(default=None, title='Description')
    purpose: Optional[str] = Field(default=None, title='Purpose')
    last_name: Optional[str] = Field(default=None, title='Last Name')
    first_name: Optional[str] = Field(default=None, title='First Name')
    middle_name: Optional[str] = Field(default=None, title='Middle Name')
    suffix: Optional[str] = Field(default=None, title='Suffix')
    address_1: Optional[str] = Field(default=None, title='Address 1')
    address_2: Optional[str] = Field(default=None, title='Address 2')
    city: Optional[str] = Field(default=None, title='City')
    state: Optional[str] = Field(default=None, title='State')
    zip5: Optional[int] = Field(default=None, title='Zip')
    zip4: Optional[int] = Field(default=None, title='Zip+4')
    zip_foreign: Optional[str] = Field(default=None, title='Foreign Zip')
    country: Optional[str] = Field(default='USA', title='Country', max_length=3)
    filed_date: date = Field(..., title='Filed Date')
    committee_type: Optional[str] = Field(default=None, title='Committee Type')
    committee_name: Optional[str] = Field(default=None, title='Committee Name')
    candidate_name: Optional[str] = Field(default=None, title='Candidate Name')
    candidate_firstname: Optional[str] = Field(default=None, title='Candidate First Name')
    candidate_lastname: Optional[str] = Field(default=None, title='Candidate Last Name')
    candidate_middlename: Optional[str] = Field(default=None, title='Candidate Middle Name')
    amended: str = Field(..., title='Amended', max_length=1, regex='[YN]')
    employer: Optional[str] = Field(default=None, title='Employer')
    occupation: Optional[str] = Field(default=None, title='Occupation')
    download_date: date = Field(..., title='Date Downloaded')
    file_origin: str = Field(..., title='File Origin')

    _clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)

    _validate_expenditure_date = \
        field_validator(
            'expenditure_date', 'filed_date',
            mode='before')(lambda v: funcs.validate_date(v, fmt='%m/%d/%Y'))
    @model_validator(mode='before')
    @classmethod
    def parse_candidate_name(cls, values):
        if values.get('candidate_name'):
            name = funcs.person_name_parser(values['candidate_name'])
            values['candidate_firstname'] = name.first
            values['candidate_lastname'] = name.last
            values['candidate_middlename'] = name.middle
        return values

    @model_validator(mode='before')
    @classmethod
    def parse_zipcode(cls, values):
        if values.get('zip'):
            if len(values['zip']) == 9 and values['zip'].isdigit():
                values['zip5'] = int(values['zip'][:5])
                values['zip4'] = int(values['zip'][5:])
            elif len(values['zip']) == 5:
                values['zip5'] = int(values['zip'])
            elif '-' in values['zip']:
                _zip5 = int(values['zip'].split('-')[0])
                _zip4 = int(values['zip'].split('-')[1])
                if len(str(_zip5)) == 5 and len(str(_zip4)) == 4:
                    values['zip5'] = _zip5
                    values['zip4'] = _zip4
            elif not values['zip'].isdigit():
                values['zip_foreign'] = values['zip']
                values['country'] = values['state']
            else:
                raise PydanticCustomError(
                    'zip_code_format',
                    f"{values['zip']} is not a valid zip code format",
                    {
                        'column': 'zip',
                        'value': values['zip']
                    }
                )
        return values
    # {'Expenditure ID': '215966',
    #  'Org ID': '9777',
    #  'Expenditure Type': 'Ordinary and Necessary Campaign Expense',
    #  'Expenditure Date': '01/01/2021',
    #  'Expenditure Amount': '24.33',
    #  'Description': '',
    #  'Purpose': 'Unknown',
    #  'Last Name': 'NON-ITEMIZED RECIPIENT',
    #  'First Name': '',
    #  'Middle Name': '',
    #  'Suffix': '',
    #  'Address 1': '',
    #  'Address 2': '',
    #  'City': '',
    #  'State': '',
    #  'Zip': '',
    #  'Filed Date': '04/18/2021',
    #  'Committee Type': 'Candidate Committee',
    #  'Committee Name': '',
    #  'Candidate Name': 'MICHEAL BERGSTROM',
    #  'Amended': 'N',
    #  'Employer': '',
    #  'Occupation': '',
    #  'download_date': '2024-01-08',
    #  'file_origin': '2021_ExpenditureExtract'}
