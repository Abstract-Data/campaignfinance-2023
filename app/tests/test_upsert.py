"""Tests for app.core.upsert.bulk_upsert — SQLite in-memory only.

All models defined here use schema=None so they work on plain SQLite without
an ATTACH workaround.
"""

from __future__ import annotations

from typing import Optional

import pytest
from sqlmodel import Field, Session, SQLModel, create_engine

from app.core.upsert import bulk_upsert

# --------------------------------------------------------------------------- #
# Local test models (no schema= so SQLite can create them)
# --------------------------------------------------------------------------- #


class UpsertItem(SQLModel, table=True):
    """Simple table: natural integer PK + one value column."""

    __tablename__ = "upsert_item"

    id: int = Field(primary_key=True)
    value: str
    created_at: Optional[str] = Field(default=None)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture()
def engine():
    """Fresh in-memory SQLite engine for each test."""
    _engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    UpsertItem.__table__.create(_engine, checkfirst=True)
    yield _engine
    UpsertItem.__table__.drop(_engine, checkfirst=True)


@pytest.fixture()
def session(engine):
    """Session bound to the test engine."""
    with Session(engine) as s:
        yield s


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #


class TestBulkUpsertDoUpdate:
    """DO UPDATE semantics — second write wins."""

    def test_single_insert(self, session):
        """A fresh row is inserted correctly."""
        total = bulk_upsert(
            session,
            UpsertItem,
            [{"id": 1, "value": "hello"}],
            conflict_cols=["id"],
        )
        assert total == 1
        row = session.get(UpsertItem, 1)
        assert row is not None
        assert row.value == "hello"

    def test_upsert_overwrites_on_conflict(self, session):
        """Inserting the same PK twice with a changed value yields ONE row
        with the second (updated) value — DO UPDATE, not DO NOTHING."""
        bulk_upsert(
            session,
            UpsertItem,
            [{"id": 42, "value": "first"}],
            conflict_cols=["id"],
        )
        bulk_upsert(
            session,
            UpsertItem,
            [{"id": 42, "value": "second"}],
            conflict_cols=["id"],
        )

        # Expire the identity-map cache so we re-read from the DB.
        session.expire_all()
        rows = session.exec(  # type: ignore[call-overload]
            # Use SQLAlchemy select to avoid importing from sqlmodel.sql
            UpsertItem.__table__.select()
        ).all()
        assert len(rows) == 1
        assert rows[0].value == "second"

    def test_created_at_excluded_from_set(self, session):
        """The created_at column is NOT included in the DO UPDATE SET clause,
        so the original timestamp survives a conflict."""
        bulk_upsert(
            session,
            UpsertItem,
            [{"id": 7, "value": "original", "created_at": "2024-01-01"}],
            conflict_cols=["id"],
        )
        bulk_upsert(
            session,
            UpsertItem,
            [{"id": 7, "value": "updated", "created_at": "2099-01-01"}],
            conflict_cols=["id"],
        )

        session.expire_all()
        row = session.get(UpsertItem, 7)
        assert row.value == "updated"
        # created_at must not have been overwritten.
        assert row.created_at == "2024-01-01"


class TestBulkUpsertChunking:
    """Chunk-boundary behaviour."""

    def test_chunk_size_2_over_5_rows(self, session):
        """chunk_size=2 over 5 rows should produce 5 persisted rows."""
        rows = [{"id": i, "value": f"v{i}"} for i in range(1, 6)]
        total = bulk_upsert(
            session,
            UpsertItem,
            rows,
            conflict_cols=["id"],
            chunk_size=2,
        )
        assert total == 5

        session.expire_all()
        persisted = session.exec(UpsertItem.__table__.select()).all()
        assert len(persisted) == 5

    def test_chunk_boundary_exact_multiple(self, session):
        """4 rows with chunk_size=2 => exactly 4 persisted rows."""
        rows = [{"id": i, "value": f"val{i}"} for i in range(10, 14)]
        total = bulk_upsert(
            session,
            UpsertItem,
            rows,
            conflict_cols=["id"],
            chunk_size=2,
        )
        assert total == 4

    def test_empty_rows_returns_zero(self, session):
        """An empty iterable should upsert 0 rows and not raise."""
        total = bulk_upsert(
            session,
            UpsertItem,
            [],
            conflict_cols=["id"],
        )
        assert total == 0


class TestBulkUpsertExplicitUpdateCols:
    """Explicit update_cols parameter."""

    def test_explicit_update_cols_only_updates_listed_cols(self, session):
        """When update_cols is set, only those columns are updated."""
        bulk_upsert(
            session,
            UpsertItem,
            [{"id": 99, "value": "keep", "created_at": "2020-01-01"}],
            conflict_cols=["id"],
            update_cols=["value"],
        )
        bulk_upsert(
            session,
            UpsertItem,
            [{"id": 99, "value": "new_value", "created_at": "2099-01-01"}],
            conflict_cols=["id"],
            update_cols=["value"],
        )

        session.expire_all()
        row = session.get(UpsertItem, 99)
        assert row.value == "new_value"
        # created_at was not in update_cols so must stay as-is.
        assert row.created_at == "2020-01-01"


class TestBulkUpsertDialect:
    """Dialect guard."""

    def test_unsupported_dialect_raises_value_error(self):
        """Any dialect other than sqlite/postgresql raises ValueError."""
        # Build a mock session whose get_bind() returns a fake engine with
        # an unknown dialect.

        class FakeDialect:
            name = "mssql"

        class FakeEngine:
            dialect = FakeDialect()

        class FakeSession:
            def get_bind(self):
                return FakeEngine()

        with pytest.raises(ValueError, match="unsupported dialect"):
            bulk_upsert(
                FakeSession(),  # type: ignore[arg-type]
                UpsertItem,
                [{"id": 1, "value": "x"}],
                conflict_cols=["id"],
            )
