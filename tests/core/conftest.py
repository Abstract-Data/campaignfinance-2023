"""Shared fixtures for tests/core — idempotency and write-path tests."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlmodel import Session, SQLModel

import app.core.models  # noqa: F401 — register all SQLModel table classes
from app.core.unified_database import UnifiedDatabaseManager


@pytest.fixture()
def dedup_session():
    """sqlite engine with all tables and dedup unique indexes applied."""
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with engine.connect() as conn:
        for ddl in UnifiedDatabaseManager._DEDUP_INDEXES:
            conn.execute(text(ddl))
        conn.commit()
    with Session(engine) as session:
        yield session


@pytest.fixture()
def dedup_engine_session():
    """Yields (engine, session) for tests that need both (e.g. id_maps key-frame reads)."""
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with engine.connect() as conn:
        for ddl in UnifiedDatabaseManager._DEDUP_INDEXES:
            conn.execute(text(ddl))
        conn.commit()
    with Session(engine) as session:
        yield engine, session
