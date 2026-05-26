"""Unit tests for Texas validator mixins (INDIVIDUAL/ENTITY discriminator)."""

from __future__ import annotations

import pytest
from pydantic_core import PydanticCustomError

from app.states.texas.validators._mixins import validate_individual_entity_discriminator


def test_individual_requires_name_field() -> None:
    values = {
        "contributorPersentTypeCd": "INDIVIDUAL",
        "contributorNameLast": "Smith",
    }
    result = validate_individual_entity_discriminator(
        values,
        type_field="contributorPersentTypeCd",
        individual_name_field="contributorNameLast",
        entity_org_field="contributorNameOrganization",
    )
    assert result["contributorNameLast"] == "Smith"


def test_entity_requires_organization_field() -> None:
    values = {
        "contributorPersentTypeCd": "ENTITY",
        "contributorNameOrganization": "ACME PAC",
    }
    result = validate_individual_entity_discriminator(
        values,
        type_field="contributorPersentTypeCd",
        individual_name_field="contributorNameLast",
        entity_org_field="contributorNameOrganization",
    )
    assert result["contributorNameOrganization"] == "ACME PAC"


def test_individual_missing_name_raises() -> None:
    values = {"contributorPersentTypeCd": "INDIVIDUAL"}
    with pytest.raises(PydanticCustomError):
        validate_individual_entity_discriminator(
            values,
            type_field="contributorPersentTypeCd",
            individual_name_field="contributorNameLast",
            entity_org_field="contributorNameOrganization",
        )
