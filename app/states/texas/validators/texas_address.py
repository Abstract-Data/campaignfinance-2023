from __future__ import annotations
from datetime import date
from typing import Optional, Annotated, Any
from sqlmodel import Field
from pydantic import field_validator, model_validator, ValidatorFunctionWrapHandler, WrapValidator, BeforeValidator
from pydantic_extra_types.phone_numbers import PhoneNumber
from pydantic_core import PydanticCustomError
from .texas_settings import TECSettings
import funcs.validator_functions as funcs
import states.texas.funcs.tx_validation_funcs as tx_funcs
from scourgify import NormalizeAddress
from scourgify.exceptions import AddressNormalizationError
import usaddress
from funcs.record_keygen import RecordKeyGenerator


class TECAddress(TECSettings):
    __tablename__ = "tx_addresses"
    __table_args__ = {"schema": "texas"}
    id: Optional[str] = Field(default=None, description="Unique record ID")
    address1: Optional[str] = Field(default=None, description="Address line 1")
    address2: Optional[str] = Field(default=None, description="Address line 2")
    city: Optional[str] = Field(default=None, description="City")
    state: Optional[str] = Field(default=None, description="State")
    postalCode: Optional[str] = Field(default=None, description="Postal code")
    county: Optional[str] = Field(default=None, description="County")
    country: Optional[str] = Field(default=None, description="Country")
    region: Optional[str] = Field(default=None, description="Region")
    standardized: Optional[dict[str, str] | str] = Field(default=None, description="Address standardized")

    @model_validator(mode='before')
    @classmethod
    def fill_fields(cls, values):
        if not values:
            return values
        values['address1'] = next((values.get(x) for x in values.keys() if x.endswith('Addr1')), None)
        values['address2'] = next((values.get(x) for x in values.keys() if x.endswith('Addr2')), None)
        values['city'] = next((values.get(x) for x in values.keys() if x.endswith('City')), None)
        values['state'] = next((values.get(x) for x in values.keys() if x.endswith('StateCd')), None)
        values['postalCode'] = next((values.get(x) for x in values.keys() if x.endswith('PostalCode')), None)
        values['county'] = next((values.get(x) for x in values.keys() if x.endswith('CountyCd')), None)
        values['country'] = next((values.get(x) for x in values.keys() if x.endswith('CountryCd')), None)
        values['region'] = next((values.get(x) for x in values.keys() if x.endswith('Region')), None)
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
            self.standardized = ", ".join([v.strip() for k, v in _new_address.items() if v])
            self.id = self.generate_key(self.standardized)
            return self
