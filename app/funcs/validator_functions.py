import phonenumbers
from datetime import datetime
from typing import List, Tuple, Dict


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
        if k in ["", '"', "null"]:
            values[k] = values[k].replace(k, None)
    return values


def validate_phone_number(cls, v):
    if v:
        # If sum of digits is 0, then it's not a phone number
        if sum(map(int, v)) == 0:
            return None
        try:
            _number = phonenumbers.parse(v, "US")
            _formatted_phone = phonenumbers.format_number(
                _number, phonenumbers.PhoneNumberFormat.E164
            )
        except phonenumbers.phonenumberutil.NumberParseException:
            raise ValueError("Incorrectly formatted phone number")

        if not phonenumbers.is_valid_number(_number):
            raise ValueError("Not a valid phone number")
        formatted_phone = phonenumbers.format_number(_number, phonenumbers.PhoneNumberFormat.E164)
        return formatted_phone


def validate_date(cls, v):
    if v:
        try:
            _value = datetime.strptime(str(v), "%Y%m%d").date()
        except ValueError:
            raise ValueError(f"Invalid date format. Must be YYYYMMDD.")
        return _value


def create_address_dict(address: List[Tuple[str, str]]) -> Dict[str, str]:
    return {adr[1]: adr[0] for adr in address}