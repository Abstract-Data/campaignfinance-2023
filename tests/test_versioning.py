"""Tests for entity version snapshots (TASK-4b)."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from sqlmodel import create_engine

import app.core.models  # noqa: F401 — register unified tables
from app.core.models import UnifiedTransaction, UnifiedTransactionVersion
from app.core.unified_database import UnifiedDatabaseManager, _to_json_safe


@pytest.fixture
def version_db(tmp_path: Path) -> UnifiedDatabaseManager:
    db_path = tmp_path / "versioning.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    UnifiedTransaction.__table__.create(engine, checkfirst=True)
    UnifiedTransactionVersion.__table__.create(engine, checkfirst=True)
    manager = UnifiedDatabaseManager(database_url=url, echo=False)
    manager.engine = engine
    return manager


def test_to_json_safe_handles_date_and_decimal() -> None:
    payload = _to_json_safe({"posted": date(2020, 1, 2), "amount": Decimal("12.50")})
    assert payload == {"posted": "2020-01-02", "amount": 12.5}


def test_update_transaction_records_date_and_decimal_version(
    version_db: UnifiedDatabaseManager,
) -> None:
    with version_db.get_session() as session:
        txn = UnifiedTransaction(
            amount=Decimal("25.00"),
            transaction_date=date(2024, 6, 1),
            description="Test",
        )
        session.add(txn)
        session.commit()
        session.refresh(txn)
        txn_id = txn.id

    updated = version_db.update_transaction(txn_id, {"description": "Updated"})
    assert updated is not None

    versions = version_db.get_transaction_versions(txn_id)
    assert len(versions) == 1
    snapshot = json.loads(versions[0].data)
    assert snapshot["transaction_date"] == "2024-06-01"
    assert snapshot["amount"] == 25.0
