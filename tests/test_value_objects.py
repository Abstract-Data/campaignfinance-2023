"""Tests for app.core.value_objects (TASK-4d)."""

import dataclasses

import pytest

from app.core.value_objects import AddressParts, Officer, PersonName


class TestPersonName:
    def test_full_name_individual(self):
        name = PersonName(first="John", middle="Q", last="Public", suffix="Jr")
        assert name.full_name == "John Q Public Jr"

    def test_full_name_organization(self):
        name = PersonName(organization="  ACME Corp  ")
        assert name.full_name == "ACME Corp"

    def test_frozen(self):
        name = PersonName(first="Jane")
        with pytest.raises(dataclasses.FrozenInstanceError):
            name.first = "Janet"  # type: ignore[misc]


class TestAddressParts:
    def test_normalized_strips_and_uppercases_state(self):
        parts = AddressParts(state=" tx ", city=" Austin ", zip_code=" 78701 ")
        normalized = parts.normalized()
        assert normalized.state == "TX"
        assert normalized.city == "Austin"
        assert normalized.zip_code == "78701"


class TestOfficer:
    def test_construction(self):
        officer = Officer(
            name=PersonName(first="Pat", last="Smith"),
            role="treasurer",
            committee_id="F123",
        )
        assert officer.name.full_name == "Pat Smith"
        assert officer.role == "treasurer"
