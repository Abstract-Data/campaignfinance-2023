"""Idempotency tests for vectorized ingest write paths (first-write-wins).

Each test verifies that writing the same rows a second time adds zero rows.
sqlite is used for all tests here; Bucket-B tests apply the required partial
unique index before exercising ON CONFLICT … DO NOTHING.

File is append-only until Wave-1-z integration: parallel W1 agents each add
their own named test function; the integrator resolves any merge conflicts.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlmodel import Session, SQLModel

import app.core.models  # noqa: F401 — register all SQLModel table classes


@pytest.fixture()
def session_with_entity_dedup_index():
    """sqlite engine with tables + the partial unique index for unified_entities."""
    engine = create_engine("sqlite://")
    tables = [t for t in SQLModel.metadata.tables.values() if t.schema is None]
    SQLModel.metadata.create_all(engine, tables=list(tables))
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uix_entities_type_name_state
                ON unified_entities (entity_type, normalized_name, state_id)
                WHERE state_id IS NOT NULL
                """
            )
        )
        conn.commit()
    with Session(engine) as session:
        yield session


def test_entities_idempotent(session_with_entity_dedup_index):
    """Writing the same unified_entity row twice must produce exactly one row."""
    import polars as pl

    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedEntity

    session = session_with_entity_dedup_index
    rows = pl.DataFrame(
        [
            {
                "entity_type": "ORGANIZATION",
                "name": "Acme PAC",
                "normalized_name": "acme pac",
                "committee_id": None,
                "notes": None,
                "person_id": None,
                "address_id": None,
                "state_id": 1,
            }
        ]
    )
    kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )
    common.write_frame(session, UnifiedEntity, rows, **kw)
    common.write_frame(session, UnifiedEntity, rows, **kw)  # identical repeat
    n = session.execute(text("SELECT count(*) FROM unified_entities")).scalar()
    assert n == 1, f"expected 1 entity after double-write, got {n}"


def test_entities_idempotent_null_state_not_deduplicated(session_with_entity_dedup_index):
    """Rows with state_id=NULL are NOT covered by the partial index.

    Two NULL-state rows should both be written because the WHERE predicate
    (state_id IS NOT NULL) excludes them from the conflict target.
    """
    import polars as pl

    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedEntity

    session = session_with_entity_dedup_index
    null_row = pl.DataFrame(
        [
            {
                "entity_type": "ORGANIZATION",
                "name": "Stateless Corp",
                "normalized_name": "stateless corp",
                "committee_id": None,
                "notes": None,
                "person_id": None,
                "address_id": None,
                "state_id": None,
            }
        ]
    )
    kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )
    common.write_frame(session, UnifiedEntity, null_row, **kw)
    common.write_frame(session, UnifiedEntity, null_row, **kw)
    n = session.execute(text("SELECT count(*) FROM unified_entities")).scalar()
    # Both rows land: partial index does not cover NULL state_id
    assert n == 2, f"expected 2 null-state rows (index does not cover NULLs), got {n}"


def test_entities_idempotent_batch_dedup(session_with_entity_dedup_index):
    """A batch containing the same entity key twice must produce only one row."""
    import polars as pl

    from app.core.ingest_vectorized import common
    from app.core.models.tables import UnifiedEntity

    session = session_with_entity_dedup_index
    rows = pl.DataFrame(
        [
            {
                "entity_type": "INDIVIDUAL",
                "name": "John Smith",
                "normalized_name": "john smith",
                "committee_id": None,
                "notes": None,
                "person_id": None,
                "address_id": None,
                "state_id": 1,
            },
            {
                "entity_type": "INDIVIDUAL",
                "name": "John Smith",
                "normalized_name": "john smith",
                "committee_id": None,
                "notes": None,
                "person_id": None,
                "address_id": None,
                "state_id": 1,
            },
        ]
    )
    kw = dict(
        conflict_cols=["entity_type", "normalized_name", "state_id"],
        update_cols=[],
        conflict_where="state_id IS NOT NULL",
    )
    common.write_frame(session, UnifiedEntity, rows, **kw)
    n = session.execute(text("SELECT count(*) FROM unified_entities")).scalar()
    assert n == 1, f"expected 1 row from same-key batch, got {n}"
