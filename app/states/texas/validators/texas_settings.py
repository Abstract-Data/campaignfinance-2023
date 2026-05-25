import app.funcs.validator_functions as funcs
import app.states.texas.funcs.tx_validation_funcs as tx_funcs
from app.funcs.record_keygen import RecordKeyGenerator
from pydantic import ConfigDict, model_validator
from sqlmodel import SQLModel

"""
======================
==== TEC Settings ====
======================
"""


class TECSettings(SQLModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        str_to_upper=True,
        from_attributes=True,
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
