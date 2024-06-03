from typing import Optional
from pydantic import field_validator, model_validator
from sqlmodel import Field
from pydantic_core import PydanticCustomError
from datetime import date
from .ok_settings import OklahomaSettings
import funcs.validator_functions as funcs

"""
Oklahoma Expenditure Model/Validator 
Based on key information from the Oklahoma Ethics Commission
URL: https://guardian.ok.gov/PublicSite/Resources/PublicDocuments/OKReceiptsAndTransfersInFileLayout.pdf
"""


class OklahomaContribution(OklahomaSettings, table=True):
    __tablename__ = 'contributions'
    __table_args__ = {'schema': 'oklahoma'}
    id: Optional[int] = Field(default=None, primary_key=True)
    receipt_id: Optional[int] = Field(default=None, title='Receipt ID')
    org_id: Optional[int] = Field(default=None, title='Organization ID')
    receipt_type: str = Field(title='Receipt Type')
    receipt_date: date = Field(title='Receipt Date')
    receipt_amount: float = Field(title='Receipt Amount')
    description: Optional[str] = Field(default=None, title='Description')
    receipt_source_type: Optional[str] = Field(default=None, title='Receipt Source Type')
    last_name: Optional[str] = Field(default=None, title='Last Name')
    first_name: Optional[str] = Field(default=None, title='First Name')
    middle_name: Optional[str] = Field(default=None, title='Middle Name')
    suffix: Optional[str] = Field(default=None, title='Suffix')
    address_1: Optional[str] = Field(title='Address Field 1')
    address_2: Optional[str] = Field(default=None, title='Address Field 2')
    city: Optional[str] = Field(default=None, title='City')
    state: Optional[str] = Field(default=None, title='State')
    zip5: Optional[int] = Field(default=None, title='Zip5')
    zip4: Optional[int] = Field(default=None, title='Zip+4')
    zip_foreign: Optional[str] = Field(default=None, title='Foreign Zip')
    country: Optional[str] = Field(default='USA', title='Country', max_length=3)
    filed_date: date = Field(title='Date Filed')
    committee_type: Optional[str] = Field(default=None, title='Committee Type')
    committee_name: Optional[str] = Field(default=None, title='Committee Name')
    candidate_name: Optional[str] = Field(default=None, title='Candidate Name')
    candidate_lastname: Optional[str] = Field(default=None, title='Candidate Last Name')
    candidate_firstname: Optional[str] = Field(default=None, title='Candidate First Name')
    candidate_middlename: Optional[str] = Field(default=None, title='Candidate Middle Name')
    amended: str = Field(..., title='Amended', max_length=1, regex='[YN]')
    employer: Optional[str] = Field(default=None, title='Employer')
    occupation: Optional[str] = Field(default=None, title='Occupation')
    download_date: date = Field(title='Date Downloaded')
    file_origin: str = Field(title='File Origin')

    _clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)

    _validate_dates = \
        field_validator(
            'receipt_date', 'filed_date',
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

    @field_validator('amended', mode='before')
    @classmethod
    def validate_amended(cls, v):
        if v not in ['Y', 'N']:
            return 'Y'
        #     raise PydanticCustomError(
        #         'amended_format',
        #         f"{v} is not a valid format for amended",
        #         {
        #             'column': 'amended',
        #             'value': v
        #         }
        #     )
        return v

    @field_validator('country', mode='before')
    @classmethod
    def validate_country(cls, v):
        if len(v) > 3:
            raise PydanticCustomError(
                'country_format',
                "Not a valid format for country",
                {
                    'column': 'country',
                    'value': v
                }
            )
        return v
