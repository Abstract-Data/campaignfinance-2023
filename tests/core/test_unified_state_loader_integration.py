"""Integration tests for UnifiedStateLoader batch persistence (TASK-4c)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

import app.core.models  # noqa: F401 — register unified tables
from app.core.enums import TransactionType
from app.core.models import State, UnifiedTransaction
from app.core.unified_database import UnifiedDatabaseManager, get_db_manager, reset_db_manager_cache
from app.core.unified_state_loader import UnifiedStateLoader


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    db_path = tmp_path / "loader_integration.db"
    url = f"sqlite:///{db_path}"
    engine = create_engine(url, connect_args={"check_same_thread": False})
    for table in SQLModel.metadata.sorted_tables:
        if table.schema is None:
            table.create(engine, checkfirst=True)
    return engine


@pytest.fixture
def loader_db_manager(sqlite_engine) -> UnifiedDatabaseManager:
    reset_db_manager_cache()
    manager = get_db_manager(database_url=str(sqlite_engine.url), bootstrap=False)
    manager.engine = sqlite_engine
    with manager.get_session() as session:
        session.add(State(code="TX", name="Texas"))
        session.commit()
    yield manager
    reset_db_manager_cache()


@pytest.fixture
def loader(tmp_path: Path, loader_db_manager: UnifiedDatabaseManager) -> UnifiedStateLoader:
    state_dir = tmp_path / "texas"
    state_dir.mkdir()
    return UnifiedStateLoader("texas", tmp_path, db_manager=loader_db_manager)


def _contribution_record() -> dict:
    return {
        "record_type": "RCPT",
        "filerIdent": "99001",
        "contributionAmount": "500.00",
        "contributionDt": "2024-03-01",
    }


def test_process_records_batch_persists_via_get_db_manager(
    loader: UnifiedStateLoader,
    loader_db_manager: UnifiedDatabaseManager,
    sqlite_engine,
) -> None:
    """Batch load uses a real SQLite manager from get_db_manager — no db_manager sentinel."""
    records = [_contribution_record(), _contribution_record()]
    stats = loader.process_records_batch(records, file_path=Path("contrib.parquet"))

    assert stats.success == 2
    assert stats.failures == 0
    assert stats.db_errors == 0

    with Session(sqlite_engine) as session:
        rows = session.exec(select(UnifiedTransaction)).all()
    assert len(rows) == 2
    assert all(r.state_id == 1 for r in rows)
    assert all(r.transaction_type == TransactionType.CONTRIBUTION for r in rows)
    assert all(float(r.amount) == pytest.approx(500.0) for r in rows)


def test_process_records_batch_raises_when_state_missing(
    tmp_path: Path,
    sqlite_engine,
) -> None:
    reset_db_manager_cache()
    manager = get_db_manager(database_url=str(sqlite_engine.url), bootstrap=False)
    manager.engine = sqlite_engine
    loader = UnifiedStateLoader("texas", tmp_path, db_manager=manager)

    with pytest.raises(ValueError, match="states table"):
        loader.process_records_batch([_contribution_record()])

    reset_db_manager_cache()

