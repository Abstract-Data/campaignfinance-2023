from __future__ import annotations
import phonenumbers
from datetime import datetime
from typing import List, Tuple, Dict, Optional
from nameparser import HumanName
import probablepeople
from functools import singledispatch
import usaddress
import re
from pydantic_core import PydanticCustomError
from pydantic import ValidationError
import hashlib
from sqlmodel import SQLModel


def create_record_id(record: SQLModel) -> str:
    """
    Create a unique record ID from a record.
    :param record: SQLModel
    :return: str
    """
    record = dict(record)
    record.pop("id", None)  # Remove the ID field to generate a unique ID
    record.pop('file_origin', None)
    record.pop('download_date', None)
    record_hash = hashlib.shake_256(str([x for x in record.values() if x is not None]).encode()).hexdigest(10)
    return record_hash


def clear_blank_strings(cls, values):
    """
    Clear out all blank strings or ones that contain 'null' from records.
    :param cls:
    :param values:
    :return:
    """
    for k, v in values.items():
        if v in ["", '"', "null"]:
            values[k] = None
    return values


def validate_phone_number(column: str, phone_number: str | int) -> Optional[str]:
    if phone_number:
        # If sum of digits is 0, then it's not a phone number
        if phone_number.isdigit():
            if sum(map(int, phone_number)) == 0:
                return None

        try:
            _number = phonenumbers.parse(phone_number, "US")
            _formatted_phone = phonenumbers.format_number(
                _number, phonenumbers.PhoneNumberFormat.E164
            )
        except phonenumbers.phonenumberutil.NumberParseException:
            raise PydanticCustomError(
                'bad_phone_number_format',
                "Phone is not a parseable phone number",
                {
                    'column': column,
                    'value': phone_number
                }
            )

        if not phonenumbers.is_valid_number(_number):
            raise PydanticCustomError(
                'bad_phone_number_format',
                "Phone is not a valid phone number",
                {
                    'column': column,
                    'value': phone_number}
            )
        formatted_phone = phonenumbers.format_number(_number, phonenumbers.PhoneNumberFormat.E164)
        return formatted_phone


def validate_date(v, fmt="%Y%m%d"):
    if v:
        try:
            _value = datetime.strptime(str(v), fmt).date()
        except ValueError:
            raise PydanticCustomError(
                'bad_date_format',
                f"Date must be in {fmt} format",
                {
                    'column': 'date',
                    'value': v
                }
            )
        return _value


def create_address_dict(address: List[Tuple[str, str]]) -> Dict[str, str]:
    return {adr[1]: adr[0] for adr in address}


def format_address(column, address: str | List[str]) -> Tuple[str, str]:
    if isinstance(address, list):
        address = " ".join(address)
    try:
        parsed_address = usaddress.parse(address)

        address1 = []
        address2 = []

        for component, tag in parsed_address:
            if tag in ['AddressNumber', 'StreetName', 'StreetNamePostType']:
                address1.append(component)
            elif tag in ['OccupancyType', 'OccupancyIdentifier']:
                address2.append(component)

        address1 = ' '.join(address1)
        address2 = ' '.join(address2)
    except ValidationError:
        raise PydanticCustomError(
            'bad_address_format',
            "Address must be a valid address",
            {
                'column': column,
                'value': address
            }
        )

    return address1, address2


def format_zipcode(column, zipcode: str) -> str:
    zipcode = zipcode.strip()
    if re.match(r"^\d{9}$", zipcode):
        zipc = zipcode[:5]
        plus4 = zipcode[5:]
        strings = [zipc, plus4]
        return '-'.join(strings)
    elif re.match(r"^\d{5}-$", zipcode):
        return zipcode[:5]
    elif re.match(r"^\d{5}-\d{4}$", zipcode):
        return zipcode
    elif re.match(r"^\d{5}$", zipcode):
        return zipcode
    else:
        if len(zipcode) > 5:
            raise PydanticCustomError(
                'bad_zipcode_format',
                "Zipcode is less than 5 chars",
                {
                    'column': column,
                    'value': zipcode
                }
            )
        elif 5 < len(zipcode) > 9:
            raise PydanticCustomError(
                'bad_zipcode_format',
                "Zipcode is more than 5 and less than 9 chars",
            )
        raise PydanticCustomError(
            'bad_zipcode_format',
            "Zipcode must be in 5 digit format or 5 digit + 4 digit format",
            {
                'column': column,
                'value': zipcode
            }
        )


@singledispatch
def person_name_parser(name: str) -> HumanName:
    return HumanName(name)


@person_name_parser.register
def _(name: list) -> HumanName:
    return HumanName(" ".join(name))


def organization_name_parser(org: str) -> probablepeople.tag:
    return probablepeople.tag(org)
