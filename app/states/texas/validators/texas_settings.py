import app.states.texas.funcs.tx_validation_funcs as tx_funcs
from app.funcs.record_keygen import RecordKeyGenerator
from pydantic import ConfigDict
from pydantic import model_validator
from sqlmodel import SQLModel

from ._mixins import AddressValidatedModel

"""
======================
==== TEC Settings ====
======================
"""


class TECSettings(AddressValidatedModel, SQLModel):
    model_config = ConfigDict(
        str_strip_whitespace=True,
        str_to_upper=True,
        from_attributes=True,
    )

    def __repr__(self):
        return self.__class__.__name__

    check_phone_numbers = model_validator(mode="before")(tx_funcs.phone_number_validation)

    @staticmethod
    def generate_key(*args):
        _values = "".join([x for x in args if x])
        return RecordKeyGenerator.generate_static_key(_values)
