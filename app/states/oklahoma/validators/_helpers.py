"""Shared parsing helpers for Oklahoma validator SQLModel classes."""

from __future__ import annotations

from typing import Any

import app.funcs.validator_functions as funcs
from pydantic_core import PydanticCustomError


def parse_candidate_name(values: dict[str, Any]) -> dict[str, Any]:
    """Split ``candidate_name`` into first/last/middle fields."""
    if values.get("candidate_name"):
        name = funcs.person_name_parser(values["candidate_name"])
        values["candidate_firstname"] = name.first
        values["candidate_lastname"] = name.last
        values["candidate_middlename"] = name.middle
    return values


def parse_zipcode(values: dict[str, Any], *, zip_key: str = "zip") -> dict[str, Any]:
    """Normalize a combined zip column into zip5/zip4/foreign fields."""
    raw_zip = values.get(zip_key)
    if not raw_zip:
        return values

    raw_zip = str(raw_zip).strip()
    if not raw_zip:
        return values

    if len(raw_zip) == 9 and raw_zip.isdigit():
        values["zip5"] = int(raw_zip[:5])
        values["zip4"] = int(raw_zip[5:])
    elif len(raw_zip) == 5:
        values["zip5"] = int(raw_zip)
    elif "-" in raw_zip:
        zip5 = int(raw_zip.split("-")[0])
        zip4 = int(raw_zip.split("-")[1])
        if len(str(zip5)) == 5 and len(str(zip4)) == 4:
            values["zip5"] = zip5
            values["zip4"] = zip4
    elif not raw_zip.isdigit():
        values["zip_foreign"] = raw_zip
        if state := values.get("state"):
            values["country"] = state
    else:
        raise PydanticCustomError(
            "zip_code_format",
            "Zipcode is not a valid zip code format",
            {"column": zip_key, "value": raw_zip},
        )
    return values
