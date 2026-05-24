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
from datetime import date, datetime
from decimal import Decimal

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
    created_at: datetime = Field(default_factory=datetime.utcnow)


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
    committee_id: str | None = Field(
        default=None, foreign_key="unified_committees.filer_id"
    )
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
    """Create application tables registered on ``SQLModel.metadata``."""
    if tables is None:
        SQLModel.metadata.create_all(engine)
    else:
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
