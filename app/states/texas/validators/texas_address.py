from __future__ import annotations
from datetime import date
from typing import Optional, Annotated, Any
from sqlmodel import Field, Relationship
from pydantic import field_validator, model_validator, ValidatorFunctionWrapHandler, WrapValidator, BeforeValidator
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError

from . import TECBaseModel
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs
from scourgify import NormalizeAddress
from scourgify.exceptions import AddressNormalizationError
import usaddress
from icecream import ic
from funcs.record_keygen import RecordKeyGenerator

ADDRESS_LIST = {}

class TECAddressBase(TECBaseModel):
    # __tablename__ = "tx_addresses"
    # __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID", primary_key=True)
    address1: Optional[str] = Field(default=None, description="Address line 1")
    address2: Optional[str] = Field(default=None, description="Address line 2")
    city: Optional[str] = Field(default=None, description="City")
    state: Optional[str] = Field(default=None, description="State")
    postalCode: Optional[str] = Field(default=None, description="Postal code")
    county: Optional[str] = Field(default=None, description="County")
    country: Optional[str] = Field(default=None, description="Country")
    region: Optional[str] = Field(default=None, description="Region")
    standardized: str = Field(default=None, description="Address standardized")
    person_name_id: Optional[str] = Field(default=None, foreign_key="tx_person_names.id")
    # person_name: list['TECPersonName'] = Relationship(back_populates="addresses")

    @model_validator(mode='before')
    @classmethod
    def fill_fields(cls, values):
        if not values:
            return values
        mappings = {
            'address1': 'Addr1',
            'address2': 'Addr2',
            'city': 'City',
            'state': 'StateCd',
            'postalCode': 'PostalCode',
            'county': 'CountyCd',
            'country': 'CountryCd',
            'region': 'Region'
        }

        for field, key_part in mappings.items():
            values[field] = next((values.get(x) for x in values.keys() if key_part in x), None)
        return values

    @model_validator(mode='after')
    def standardize_address(self):
        _address = ""
        if self.address1:
            _address = self.address1.strip()
        if self.address2:
            _address += f" {self.address2}".strip()
        if self.city:
            _address += f", {self.city}".strip()
        if self.state:
            _address += f", {self.state}".strip()
        if self.postalCode:
            _address += f", {self.postalCode}".strip()
        # if self.country:
        #     _address += f", {self.country}"
        if _address:
            try:
                _normalize = NormalizeAddress(_address).normalize()
                self.standardized = ", ".join(x for x in _normalize.values() if x)
                return self
            except AddressNormalizationError as e:
                pass


        _new_address = {}
        _has_full_zip = False
        _standardized = None
        if "PO" or "PO BOX" in _address:
            _address = _address.replace(',', ' ')
            print("PO BOX FOUND", _address)
            parsed = usaddress.parse(_address)
            for part, type_ in parsed:
                match type_:
                    case "USPSBoxType":
                        if _adr1 := _new_address.get('address1'):
                            _new_address['address1'] += f" {part}".strip()
                        else:
                            _new_address['address1'] = part
                    case "USPSBoxID":
                        if _new_address.get('address1'):
                            _new_address['address1'] += f" {part}".strip()
                        # else:
                        #     _new_address['address1'] = part
                    case "PlaceName":
                        if _new_address.get('city'):
                            _new_address['city'] += f" {part}".strip()
                        else:
                            _new_address['city'] = part
                    case "StateName":
                        if _new_address.get('state'):
                            _new_address['state'] += f" {part}".strip()
                        else:
                            _new_address['state'] = part
                    case "ZipCode":
                        if _new_address.get('postalCode'):
                            _new_address['postalCode'] += f" {part}".strip()
                        else:
                            _new_address['postalCode'] = part
            if _pc :=_new_address.get('postalCode'):
                if '-' in _pc:
                    _has_full_zip = True


            has_address_lines = all(
                [
                    _new_address.get('address1'),
                    _new_address.get('city'),
                    _new_address.get('state'),
                    _new_address.get('postalCode')
                ]
            )
            if has_address_lines:
                std = ", ".join(
                    [
                        v.strip() for k, v in _new_address.items() if v
                    ]
                )
                if _has_full_zip:
                    ADDRESS_LIST[_pc] = std
                    _standardized = std
                else:
                    _search_for_address = ADDRESS_LIST.get(_pc)
                    if _search_for_address:
                        _standardized = _search_for_address
                self.standardized = _standardized
                self.id = self.generate_key(self.standardized)
            return self


class TECPersonAddressLinkModel(TECBaseModel, table=True):
    __tablename__ = "tx_person_address_link"
    __table_args__ = {"schema": "texas"}
    address_id: str = Field(foreign_key="texas.tx_addresses.id", primary_key=True)
    person_id: Optional[str] = Field(default=None, foreign_key="texas.tx_person_names.id", primary_key=True)
    treasurer_id: Optional[str] = Field(default=None, foreign_key="texas.tx_treasurers.id", primary_key=True)
    assistant_treasurer_id: Optional[str] = Field(default=None, foreign_key="texas.tx_assistant_treasurers.id", primary_key=True)

