"""Pytest configuration for tests/resolve.

Phase-0 stub SQLModel classes share table names with production models
(``states``, ``unified_committees``, …).  Registering them on the global
``SQLModel.metadata`` causes ``InvalidRequestError`` when a later test module
imports ``app.core.unified_sqlmodels``.

Stub tables use an isolated ``StubSQLModel`` registry/metadata; application
models remain on ``SQLModel.metadata``.  Shared stub models are defined once
here so multiple test modules do not re-register the same table names.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import Engine, MetaData, Table
from sqlalchemy.orm import registry
from sqlmodel import Field, SQLModel

_stub_registry = registry(metadata=MetaData())
stub_metadata: MetaData = _stub_registry.metadata


class StubSQLModel(SQLModel, registry=_stub_registry):
    """Base for minimal test-only tables that mirror production table names."""


class StubState(StubSQLModel, table=True):
    __tablename__ = "states"

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(default="TX", max_length=2)
    name: str | None = Field(default=None, max_length=100)


class StubFileOrigin(StubSQLModel, table=True):
    __tablename__ = "file_origins"

    id: str = Field(primary_key=True, max_length=64)
    state_id: int | None = Field(default=None, foreign_key="states.id")
    filename: str = Field(default="cover.parquet", max_length=500)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class StubUnifiedCommittee(StubSQLModel, table=True):
    __tablename__ = "unified_committees"

    filer_id: str = Field(primary_key=True, max_length=200)
    name: str | None = None


class StubUnifiedEntity(StubSQLModel, table=True):
    __tablename__ = "unified_entities"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()))


class StubUnifiedTransaction(StubSQLModel, table=True):
    __tablename__ = "unified_transactions"

    id: int | None = Field(default=None, primary_key=True)
    uuid: str = Field(default_factory=lambda: str(uuid.uuid4()))
    amount: Decimal | None = None
    transaction_date: date | None = None
    description: str | None = None
    state_id: int | None = Field(default=None, foreign_key="states.id")
    committee_id: str | None = Field(default=None, foreign_key="unified_committees.filer_id")
    report_ident: str | None = Field(default=None, max_length=20, index=True)
    report_id: int | None = Field(default=None, index=True)


def create_stub_tables(
    engine: Engine,
    tables: Sequence[Table] | None = None,
) -> None:
    """Create stub tables registered on ``stub_metadata``."""
    if tables is None:
        stub_metadata.create_all(engine)
    else:
        stub_metadata.create_all(engine, tables=list(tables))


def create_app_tables(
    engine: Engine,
    tables: Sequence[Table] | None = None,
) -> None:
    """Create application tables registered on ``SQLModel.metadata``.

    When no explicit table list is given, skip state-namespaced source tables
    (``schema="texas"`` etc.): SQLite has no schema support and raises
    ``unknown database <schema>`` when ``create_all`` tries to build them.  The
    resolve/app layer only needs the schema-less unified tables.
    """
    if tables is None:
        tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))


def create_resolve_tables(
    engine: Engine,
    *,
    stub_tables: Sequence[Table] | None = None,
    app_tables: Sequence[Table] | None = None,
) -> None:
    """Create stub tables first, then application tables, on one engine."""
    if stub_tables is None:
        create_stub_tables(engine)
    else:
        create_stub_tables(engine, stub_tables)

    if app_tables is None:
        create_app_tables(engine)
    else:
        create_app_tables(engine, app_tables)


def drop_resolve_tables(engine: Engine) -> None:
    """Drop stub and application tables from an engine."""
    stub_metadata.drop_all(engine)
    SQLModel.metadata.drop_all(engine)


# ---------------------------------------------------------------------------
# Integration test helpers (TASK-7)
# ---------------------------------------------------------------------------

TEXAS_DATA_DIR = Path("tmp") / "texas"


def texas_data_available() -> bool:
    """Return True when prepared Texas parquet/CSV files exist under ``tmp/texas``."""
    return TEXAS_DATA_DIR.is_dir() and any(TEXAS_DATA_DIR.iterdir())


def postgres_env_configured() -> bool:
    """Return True when Postgres connection settings are present."""
    from app.resolve.cli import postgres_env_configured as _postgres_env_configured

    return _postgres_env_configured()


def skip_unless_texas_data() -> None:
    """Skip the current test when ``tmp/texas`` is not populated."""
    if not texas_data_available():
        pytest.skip("tmp/texas not present — run `uv run cf prepare texas` for integration tests")


def skip_unless_postgres_env() -> None:
    """Skip the current test when Postgres env vars are not configured."""
    if not postgres_env_configured():
        pytest.skip("Postgres env not configured — set DATABASE_URL or POSTGRES_* vars")


@pytest.fixture(autouse=True)
def _integration_env_gate(request: pytest.FixtureRequest) -> None:
    """Skip ``@pytest.mark.integration`` tests when required env/data is missing."""
    marker = request.node.get_closest_marker("integration")
    if marker is None:
        return

    skip_unless_texas_data()

    # Resolve-run integration tests need Postgres unless explicitly sqlite-only.
    if marker.kwargs.get("requires_postgres"):
        skip_unless_postgres_env()
