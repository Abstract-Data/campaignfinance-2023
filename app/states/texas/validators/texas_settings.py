from dataclasses import Field
from typing import Optional
from datetime import date
from pydantic import ConfigDict, model_validator, BaseModel, BeforeValidator
from sqlmodel import SQLModel, Field
import states.texas.funcs.tx_validation_funcs as tx_funcs
import funcs.validator_functions as funcs
from funcs.record_keygen import RecordKeyGenerator

"""
======================
==== TEC Settings ====
======================
"""
def check_contains_factory(match_string: str):
    def check_contains(value: str) -> str:
        if value and match_string not in value:
            raise ValueError(f"Value must contain '{match_string}'")
        return value
    return check_contains

class TECSettings(SQLModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        str_to_upper=True,
        from_attributes=True,
        arbitrary_types_allowed=True,
    )
    def __repr__(self):
        return self.__class__.__name__

    clear_blank_strings = model_validator(mode='before')(funcs.clear_blank_strings)
    check_dates = model_validator(mode='before')(tx_funcs.validate_dates)
    # check_zipcodes = model_validator(mode='before')(tx_funcs.check_zipcodes)
    check_phone_numbers = model_validator(mode='before')(tx_funcs.phone_number_validation)
    # check_address_format = model_validator(mode='before')(tx_funcs.address_formatting)

    @staticmethod
    def generate_key(*args):
        _values = "".join([x for x in args if x])
        return RecordKeyGenerator.generate_static_key(_values)


class TECBaseModel(TECSettings):
    recordType: Optional[str] = Field(BeforeValidator(check_contains_factory('recordType')), description="Record type code")
    formTypeCd: Optional[str] = Field(BeforeValidator(check_contains_factory('formType')), description="TEC form used")
    schedFormTypeCd: Optional[str] = Field(BeforeValidator(check_contains_factory('schedFormType')), description="TEC Schedule Used")
    receivedDt: Optional[date] = Field(BeforeValidator(check_contains_factory('receivedDt')), description="Date report received by TEC")
    filerIdent: Optional[str] = Field(default=None, description="Filer account #", max_length=100)
    filerTypeCd: Optional[str] = Field(default=None, description="Type of filer", max_length=30)
    # flags: Optional[TECFlags] = Field(default=None, description="Flags")
    #
    # def __init__(self, **data):
    #     _flags = TECFlags(**data)
    #     if len(_flags.model_dump(exclude_none=True).values()) > 0:
    #         self.flags = _flags
    #     super().__init__(**data)