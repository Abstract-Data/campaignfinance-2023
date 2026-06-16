"""Tests for RecordKeyGenerator static key helpers."""

from __future__ import annotations

from datetime import date

import pytest

from app.funcs.record_keygen import RecordKeyGenerator


def test_generate_static_key_from_tuple() -> None:
    key = RecordKeyGenerator.generate_static_key(("a", 1, date(2024, 1, 1)))
    assert len(key) == 16
    assert key == RecordKeyGenerator.generate_static_key(("a", 1, date(2024, 1, 1)))


def test_generate_static_key_from_string() -> None:
    assert RecordKeyGenerator.generate_static_key(
        "plain-string"
    ) == RecordKeyGenerator.generate_static_key("plain-string")


def test_generate_static_key_date_set() -> None:
    key = RecordKeyGenerator.generate_static_key((date(2024, 6, 1), date(2024, 1, 1)))
    assert len(key) == 16


def test_generate_static_key_unsupported_type_raises() -> None:
    with pytest.raises(ValueError, match="Unsupported type"):
        RecordKeyGenerator.generate_static_key(object())


def test_record_key_generator_instance_fields() -> None:
    gen = RecordKeyGenerator("sample-record")
    assert len(gen.hash) == 32
    assert gen.uid is not None
