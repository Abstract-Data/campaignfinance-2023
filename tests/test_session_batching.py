"""Tests for TASK-4c — one session per batch and ProcessStats."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import SQLAlchemyError

from app.core.processor import ProcessStats
from app.core.unified_state_loader import UnifiedStateLoader


class _RecordModel(BaseModel):
    amount: float


@pytest.fixture
def loader(tmp_path: Path) -> UnifiedStateLoader:
    state_dir = tmp_path / "texas"
    state_dir.mkdir()
    mock_db = MagicMock()
    return UnifiedStateLoader("texas", tmp_path, db_manager=mock_db)


def test_process_stats_total() -> None:
    stats = ProcessStats(success=5, failures=2)
    assert stats.total == 7
    assert "5 OK" in str(stats)


def test_batch_opens_single_session(loader: UnifiedStateLoader) -> None:
    records = [{"a": 1}, {"a": 2}, {"a": 3}]
    session_cm = MagicMock()
    session = session_cm.__enter__.return_value
    loader._db_manager.get_session.return_value = session_cm

    with (
        patch.object(loader, "_load_batch_indexes", return_value=({}, {}, 1, "TX")),
        patch.object(loader, "_persist_transaction_from_record", return_value=MagicMock(id=1)),
    ):
        stats = loader.process_records_batch(records, auto_link_officers=False)

    loader._db_manager.get_session.assert_called_once()
    session_cm.__enter__.assert_called_once()
    session.commit.assert_called_once()
    assert stats.success == 3


def test_validation_error_increments_failures_and_continues(
    loader: UnifiedStateLoader,
) -> None:
    records = [{"ok": 1}, {"bad": 2}, {"ok": 3}]
    session_cm = MagicMock()

    def _persist(record: dict, *_args, **_kwargs):
        if record.get("bad"):
            raise ValidationError.from_exception_data(
                "RecordModel",
                [{"type": "missing", "loc": ("amount",), "msg": "Field required", "input": record}],
            )
        return MagicMock(id=1)

    loader._db_manager.get_session.return_value = session_cm

    with (
        patch.object(loader, "_load_batch_indexes", return_value=({}, {}, 1, "TX")),
        patch.object(loader, "_persist_transaction_from_record", side_effect=_persist),
    ):
        stats = loader.process_records_batch(records, auto_link_officers=False)

    assert stats.success == 2
    assert stats.failures == 1


def test_sqlalchemy_error_increments_db_errors_and_rolls_back(
    loader: UnifiedStateLoader,
) -> None:
    records = [{"a": 1}]
    session_cm = MagicMock()
    session = session_cm.__enter__.return_value

    loader._db_manager.get_session.return_value = session_cm

    with (
        patch.object(loader, "_load_batch_indexes", return_value=({}, {}, 1, "TX")),
        patch.object(
            loader,
            "_persist_transaction_from_record",
            side_effect=SQLAlchemyError("db fail"),
        ),
    ):
        stats = loader.process_records_batch(records, auto_link_officers=False)

    session.rollback.assert_called_once()
    assert stats.db_errors == 1
    assert stats.success == 0
