"""Tests for TASK-4d — db_manager injection and officer field library delegation."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from app.core.enums import CommitteeRole
from app.core.unified_field_library import UnifiedFieldLibrary
from app.core.unified_state_loader import UnifiedStateLoader


def test_db_manager_injection_property() -> None:
    mock_manager = MagicMock()
    loader = UnifiedStateLoader("texas", Path("/tmp"), db_manager=mock_manager)
    assert loader.db_manager is mock_manager
    assert loader._db_manager is mock_manager


def test_get_officer_fields_delegates_to_field_library() -> None:
    lib = UnifiedFieldLibrary()
    fields = lib.get_officer_fields("texas")
    assert isinstance(fields, dict)
    assert "treasurer_name" in fields
    assert "committee_id" in fields


def test_extract_officer_uses_field_library_not_inline_mappings() -> None:
    lib = UnifiedFieldLibrary()
    # Inject a mock db_manager so no Postgres connection is attempted; the
    # method under test only consults the field_library, never the DB.
    loader = UnifiedStateLoader("texas", Path("/tmp"), field_library=lib, db_manager=MagicMock())
    record = {
        "filer_id": "123",
        "treasurer_name": "Jane Treasurer",
    }
    result = loader._extract_officer_from_record(record)
    assert result is not None
    assert result["committee_id"] == "123"
    assert any(o["role"] == CommitteeRole.TREASURER for o in result["officers"])


def test_create_committee_relationships_calls_injected_db_manager() -> None:
    mock_manager = MagicMock()
    session_cm = MagicMock()
    session = session_cm.__enter__.return_value
    mock_manager.get_session.return_value = session_cm

    loader = UnifiedStateLoader("texas", Path("/tmp"), db_manager=mock_manager)
    loader.committee_officers = {
        "c1": [
            {
                "officers": [
                    {"name": "Jane Doe", "role": CommitteeRole.TREASURER},
                ],
            }
        ],
    }

    person = MagicMock()
    person.id = 42

    with (
        patch.object(loader, "_load_batch_indexes", return_value=({}, {}, 1, "TX")),
        patch.object(loader, "_find_or_create_person", return_value=person),
    ):
        loader._create_committee_relationships()

    mock_manager.add_person_to_committee.assert_called_once()
    kwargs = mock_manager.add_person_to_committee.call_args.kwargs
    assert kwargs["person_id"] == 42
    assert kwargs["committee_id"] == "c1"
    assert kwargs["role"] == CommitteeRole.TREASURER
    assert kwargs["session"] is session
    session.commit.assert_called_once()
