from pydantic_core import PydanticCustomError
import funcs.validator_functions as funcs
from datetime import datetime


def validate_dates(cls, values):
    if not values:
        return values
    for key, value in values.items():
        if key.endswith("Dt") and value:
            if isinstance(value, str):
                try:
                    values[key] = datetime.strptime(value, "%Y%m%d").date()
                except ValueError:
                    raise PydanticCustomError(
                        'bad_date_format',
                        f"{value} must be in YYYYMMDD format",
                        {
                            'column': key,
                            'value': value
                        }
                    )
    return values


def address_formatting(cls, values):
    original_address = None
    formatted_address = None
    _field_addresses = [_field.split('Addr')[0] for _field in values.keys() if _field.endswith('Addr1')]
    for address in _field_addresses:
        addr1 = f"{address}Addr1"
        addr2 = f"{address}Addr2"
        _address = [addr1, addr2]
        if all(_address):
            original_address = " ".join([values[x] for x in _address])
            formatted_address = funcs.format_address(column=address, address=original_address)
        elif _address[0]:
            original_address = values[addr1]
            formatted_address = funcs.format_address(column=address, address=original_address)
        if all([original_address, formatted_address]):
            if original_address != formatted_address:
                values[addr1] = formatted_address[0]
                values[addr2] = formatted_address[1]
    return values


def check_zipcodes(cls, values):
    if not values:
        return values
    for key, value in values.items():
        if key.endswith("PostalCode") and value:
            original_zip = value
            formatted_zip = funcs.format_zipcode(column=key, zipcode=value)
            values[key] = formatted_zip
    return values


def phone_number_validation(cls, values):
    if not values:
        return values
    for key, value in values.items():
        if key.endswith("PhoneNumber") and value:
            formatted_phone = funcs.validate_phone_number(column=key, phone_number=value)
            values[key] = formatted_phone
    return values
