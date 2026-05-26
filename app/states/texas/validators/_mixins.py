"""Shared validators and address helpers for Texas TEC SQLModel classes."""

from __future__ import annotations

from typing import Any

import app.funcs.validator_functions as funcs
import app.states.texas.funcs.tx_validation_funcs as tx_funcs
from pydantic import model_validator


def _tec_address(**fields: Any) -> Any:
    from .texas_address import TECAddress

    return TECAddress(**fields)


def extract_address(values: dict[str, Any], prefix: str) -> dict[str, Any]:
    """Return input fields whose keys start with *prefix*."""
    return {k: v for k, v in values.items() if k.startswith(prefix)}


def bind_street_mailing_pair(
    values: dict[str, Any],
    *,
    street_prefix: str,
    street_key: str,
    mailing_prefix: str,
    mailing_key: str,
    optional: bool = False,
) -> None:
    """Build ``TECAddress`` instances from prefixed street/mailing columns."""
    street = extract_address(values, street_prefix)
    mailing = extract_address(values, mailing_prefix)
    if optional:
        values[street_key] = _tec_address(**street) if street else None
        values[mailing_key] = _tec_address(**mailing) if mailing else None
    else:
        values[street_key] = _tec_address(**street)
        values[mailing_key] = _tec_address(**mailing)


def bind_street_address_if_present(
    values: dict[str, Any],
    *,
    prefix: str,
    target_key: str,
) -> None:
    """Set *target_key* to a ``TECAddress`` when prefixed columns exist."""
    fields = extract_address(values, prefix)
    if fields:
        values[target_key] = _tec_address(**fields)


def format_filer_check_name(values: dict[str, Any]) -> dict[str, Any]:
    """Normalize filer name parts from ``*NameFirst`` / ``*NameLast`` columns."""
    first_name = next((values.get(x) for x in values if x.endswith("NameFirst")), None)
    last_name = next((values.get(x) for x in values if x.endswith("NameLast")), None)
    if not first_name and not last_name:
        return values

    name = funcs.person_name_parser(f"{first_name} {last_name}")
    values["filerNameLast"] = name.last
    values["filerNameFirst"] = name.first
    values["filerNameMiddle"] = name.middle.replace(".", "")
    values["filerNameSuffixCd"] = name.suffix
    values["filerNamePrefixCd"] = name.title
    values["filerNameFull"] = name.full_name
    return values


def format_individual_payee_name(values: dict[str, Any]) -> dict[str, Any]:
    """Normalize payee individual name fields (TEC expense records)."""
    if values.get("payeePersentTypeCd") != "INDIVIDUAL":
        return values

    payee_name_fields = [
        key
        for key in values
        if key.startswith("payeeName") and key != "payeeNameOrganization"
    ]
    name_parts = [values[key] for key in payee_name_fields if values.get(key) != ""]
    payee_name = funcs.person_name_parser(" ".join(name_parts))
    payee_name.parse_full_name()
    values["payeeNameLast"] = payee_name.last
    values["payeeNameFirst"] = payee_name.first
    values["payeeNameSuffixCd"] = payee_name.suffix
    values["payeeNamePrefixCd"] = payee_name.title
    values["payeeNameFull"] = payee_name.full_name
    return values


class AddressValidatedModel:
    """Mixin registering the common Texas ``before`` validator chain once."""

    clear_blank_strings = model_validator(mode="before")(funcs.clear_blank_strings)
    format_dates = model_validator(mode="before")(tx_funcs.validate_dates)
    format_zipcodes = model_validator(mode="before")(tx_funcs.check_zipcodes)
