from datetime import date
from typing import Optional
from sqlmodel import Field, JSON, Relationship
from pydantic import field_validator, model_validator, BeforeValidator, create_model
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError
from .texas_settings import TECSettings
from .texas_address import TECAddress
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs
from funcs.record_keygen import RecordKeyGenerator


class TECPersonName(TECSettings):
    __tablename__ = "tx_person_names"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID", primary_key=True)
    prefix: Optional[str] = Field(
        ...,
        description="Prefix",
        max_length=20
    )
    last: str = Field(
        ...,
        description="Last name",
        max_length=50
    )
    first: str = Field(
        ...,
        description="First name",
        max_length=50
    )
    suffix: Optional[str] = Field(
        ...,
        description="Suffix",
        max_length=20
    )
    field_pfx: Optional[str] = Field(
        default=None,
        description="Field prefix"
    )

    addresses: list["TECAddress"] = Field(default=None)
    raw_input: dict = Field(default=None)

    @model_validator(mode='before')
    @classmethod
    def store_raw(cls, values):
        values['raw_input'] = {k: v for k, v in values.items()}
        return values

    @model_validator(mode='before')
    @classmethod
    def set_type_prefix(cls, values):
        _key_pfx = next((key for key in values.keys() if 'NamePrefixCd' in key), None)
        values['field_pfx'] = _key_pfx.split('NamePrefixCd')[0]
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_name(cls, values):
        values['prefix'] = next((value for key, value in values.items() if 'NamePrefixCd' in key), None)
        values['first'] = next((value for key, value in values.items() if 'NameFirst' in key), None)
        values['last'] = next((value for key, value in values.items() if 'NameLast' in key), None)
        values['suffix'] = next((value for key, value in values.items() if 'NameSuffixCd' in key), None)
        return values

    @model_validator(mode='before')
    @classmethod
    def validate_address(cls, values):
        if not values:
            return values
        _pfx = next((key for key in values.keys() if 'NamePrefixCd' in key), None)
        if _pfx:
            _pfx = _pfx.split('NamePrefixCd')[0]
        _mail_keys = [key for key in values.keys() if 'Mail' in key and key.startswith(_pfx)]
        gen_address = {
            k: v for k, v in values.items() if (
                    k.startswith(_pfx)
                    and k not in _mail_keys)
        }
        mail_dict = {k: values.get(k) for k in _mail_keys}

        _address = TECAddress(**gen_address) if gen_address else None
        _mail = TECAddress(**mail_dict) if mail_dict else None
        if any([_address, _mail]):
            values['addresses'] = [x for x in [_address, _mail] if x]
        return values

