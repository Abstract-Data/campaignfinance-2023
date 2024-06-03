from pydantic import ConfigDict, model_validator
from sqlmodel import SQLModel
import funcs.validator_functions as funcs
from pydantic_core import PydanticCustomError

"""
======================
==== OKEC Settings ===
======================
"""


class OklahomaSettings(SQLModel):
    model_config = ConfigDict(
        populate_by_name=True,
        str_strip_whitespace=True,
        str_to_upper=True,
    )

    _clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)

    # @model_validator(mode='before')
    # @classmethod
    # def validate_dates(cls, values):
    #     for key, value in values.items():
    #         if key.endswith('date'):
    #             if value:
    #                 values[key] = funcs.validate_date(value, fmt='%m/%d/%Y')
    #     return values
    #
    # @model_validator(mode='before')
    # def parse_names(cls, values):
    #     for key, value in values.items():
    #         if key.endswith('name'):
    #             _name = funcs.person_name_parser(value)
    #             values[key.replace('name', 'firstname')] = _name.first
    #             values[key.replace('name', 'lastname')] = _name.last
    #             values[key.replace('name'), 'middlename'] = _name.middle
    #             values[key.replace('name'), 'suffix'] = _name.suffix
    #         return values
    #
    # @model_validator(mode='before')
    # @classmethod
    # def parse_zipcode(cls, values):
    #     if values.get('zip'):
    #         if len(values['zip']) == 9 and values['zip'].isdigit():
    #             values['zip5'] = int(values['zip'][:5])
    #             values['zip4'] = int(values['zip'][5:])
    #         elif len(values['zip']) == 5:
    #             values['zip5'] = int(values['zip'])
    #         elif '-' in values['zip']:
    #             _zip5 = int(values['zip'].split('-')[0])
    #             _zip4 = int(values['zip'].split('-')[1])
    #             if len(str(_zip5)) == 5 and len(str(_zip4)) == 4:
    #                 values['zip5'] = _zip5
    #                 values['zip4'] = _zip4
    #         elif not values['zip'].isdigit():
    #             values['zip_foreign'] = values['zip']
    #             values['country'] = values['state']
    #         else:
    #             raise PydanticCustomError(
    #                 'zip_code_format',
    #                 f"{values['zip']} is not a valid zip code format",
    #                 {
    #                     'column': 'zip',
    #                     'value': values['zip']
    #                 }
    #             )
    #     return values
