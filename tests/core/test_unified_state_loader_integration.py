"""Integration tests for UnifiedStateLoader batch persistence (TASK-4c)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

import app.core.models  # noqa: F401 — register unified tables
from app.core.enums import TransactionType
from app.core.models import FileOrigin, State, UnifiedTransaction
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


def _contribution_record_with_id(tx_id: str, amount: str = "500.00") -> dict:
    """A contribution record with an explicit contributionInfoId (transaction_id)."""
    return {
        "record_type": "RCPT",
        "filerIdent": "99001",
        "contributionInfoId": tx_id,
        "contributionAmount": amount,
        "contributionDt": "2024-03-01",
    }


def test_reload_same_file_yields_same_row_count(
    loader: UnifiedStateLoader,
    loader_db_manager: UnifiedDatabaseManager,
    sqlite_engine,
) -> None:
    """Re-loading the same records (same file name + same transaction_id) must NOT
    duplicate rows — idempotency is now enforced by the natural-key upsert, not
    by early-returning when the file is already loaded."""
    records = [
        _contribution_record_with_id("TX-001"),
        _contribution_record_with_id("TX-002"),
    ]
    file_path = Path("contrib.parquet")

    # First load
    stats1 = loader.process_records_batch(records, file_path=file_path)
    assert stats1.success == 2

    with Session(sqlite_engine) as session:
        rows_after_first = session.exec(select(UnifiedTransaction)).all()
    assert len(rows_after_first) == 2

    # Second load of the exact same records — must stay at 2 rows, not 4
    stats2 = loader.process_records_batch(records, file_path=file_path)
    # Upserted rows come back as success (the existing row is mutated and returned)
    assert stats2.success == 2

    with Session(sqlite_engine) as session:
        rows_after_second = session.exec(select(UnifiedTransaction)).all()
    assert len(rows_after_second) == 2, "Re-loading the same file must not create duplicate rows"


def test_natural_key_upsert_overwrites_mutable_fields(
    loader: UnifiedStateLoader,
    loader_db_manager: UnifiedDatabaseManager,
    sqlite_engine,
) -> None:
    """When a record with the same natural key is re-loaded with an amended amount,
    the existing row's amount must be updated to the new value."""
    file_path = Path("contrib.parquet")

    # Initial load
    records_v1 = [_contribution_record_with_id("TX-100", amount="100.00")]
    stats1 = loader.process_records_batch(records_v1, file_path=file_path)
    assert stats1.success == 1

    with Session(sqlite_engine) as session:
        rows = session.exec(select(UnifiedTransaction)).all()
    assert len(rows) == 1
    assert float(rows[0].amount) == pytest.approx(100.0)

    # Re-load with an amended amount
    records_v2 = [_contribution_record_with_id("TX-100", amount="250.00")]
    stats2 = loader.process_records_batch(records_v2, file_path=file_path)
    assert stats2.success == 1

    with Session(sqlite_engine) as session:
        rows = session.exec(select(UnifiedTransaction)).all()
    assert len(rows) == 1, "Upsert must update in-place, not add a duplicate row"
    assert float(rows[0].amount) == pytest.approx(250.0), (
        "Amended amount must overwrite the original"
    )


def test_reload_creates_file_origin_provenance(
    loader: UnifiedStateLoader,
    loader_db_manager: UnifiedDatabaseManager,
    sqlite_engine,
) -> None:
    """process_records_batch must create a FileOrigin record on first load and
    reuse the existing one on second load (no duplicate FileOrigin rows)."""
    records = [_contribution_record_with_id("TX-200")]
    file_path = Path("contrib_prov.parquet")

    loader.process_records_batch(records, file_path=file_path)

    with Session(sqlite_engine) as session:
        origins = session.exec(select(FileOrigin)).all()
    assert len(origins) == 1
    assert origins[0].filename == file_path.name

    # Second load must not create a second FileOrigin row
    loader.process_records_batch(records, file_path=file_path)

    with Session(sqlite_engine) as session:
        origins_after = session.exec(select(FileOrigin)).all()
    assert len(origins_after) == 1, "FileOrigin must not be duplicated on re-load"
