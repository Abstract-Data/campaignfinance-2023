from datetime import date
from typing import Optional, Annotated
from sqlmodel import Field, JSON, Relationship
from pydantic import field_validator, model_validator, BeforeValidator, create_model, AliasChoices
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError
from .texas_settings import TECBaseModel
from .texas_address import TECAddressBase
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs
from funcs.record_keygen import RecordKeyGenerator
import re


class TECPersonNameBase(TECBaseModel):
    id: Optional[str] = Field(
        default=None,
        description="Unique record ID",
        primary_key=True)
    persentTypeCd: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('PersentTypeCd')),
        description="Type of filer name data - INDIVIDUAL or ENTITY")
    nameOrganization: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NameOrganization')),
        description="For ENTITY, the filer organization name")
    prefix: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NamePrefixCd')),
        description="Prefix",
        max_length=20,
    )
    last: str = Field(
        BeforeValidator(funcs.check_contains_factory('NameLast')),
        description="Last name",
        max_length=50
    )
    first: str = Field(
        BeforeValidator(funcs.check_contains_factory('NameFirst')),
        description="First name",
        max_length=50
    )
    middle: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NameMiddle')),
        description="Middle name",
        max_length=50
    )
    suffix: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NameSuffixCd')),
        description="Suffix",
        max_length=20
    )
    field_pfx: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NameSuffixCd')),
        description="Field prefix"
    )
    nameFull: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NameFull')),
        description="For INDIVIDUAL, Full name of the filer")
    nameShort: Optional[str] = Field(
        BeforeValidator(funcs.check_contains_factory('NameShort')),
        description="For INDIVIDUAL, Short name of the filer")
    primaryPhoneNumber: Optional[PhoneNumber] = Field(BeforeValidator(funcs.check_contains_factory('PrimaryPhoneNumber')), description="Primary phone number")
    primaryPhoneExt: Optional[str] = Field(BeforeValidator(funcs.check_contains_factory('PrimaryPhoneExt')), description="Primary phone number extension")
    # addresses: list["TECAddress"] = Field(default_factory=list)

    # @model_validator(mode='before')
    # @classmethod
    # def store_raw(cls, values):
    #     values['raw_input'] = {k: v for k, v in values.items()}
    #     return values

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

        _address = TECAddressBase(**gen_address) if gen_address else None
        _mail = TECAddressBase(**mail_dict) if mail_dict else None
        if any([_address, _mail]):
            values['addresses'] = [x for x in [_address, _mail] if x]
        return values
