from datetime import date
from typing import Optional

import funcs.validator_functions as funcs
from pydantic import field_validator, model_validator
from pydantic_core import PydanticCustomError
from sqlmodel import Field

from .ok_settings import OklahomaSettings

"""
Oklahoma Expenditure Model/Validator
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKExpendituresAndTransfersOutFileLayout.pdf
"""


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
    lastname: Optional[str] = Field(default=None, title='Last Name')
    firstname: Optional[str] = Field(default=None, title='First Name')
    middlename: Optional[str] = Field(default=None, title='Middle Name')
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
    candidate_suffix: Optional[str] = Field(default=None, title='Candidate Name Suffix')
    amended: str = Field(..., title='Amended', max_length=1, regex='[YN]')
    employer: Optional[str] = Field(default=None, title='Employer')
    occupation: Optional[str] = Field(default=None, title='Occupation')
    download_date: date = Field(..., title='Date Downloaded')
    file_origin: str = Field(..., title='File Origin')

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
                    "Zipcode is not a valid zip code format",
                    {
                        'column': 'zip',
                        'value': values['zip']
                    }
                )
        return values
