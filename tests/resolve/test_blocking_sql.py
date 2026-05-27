"""Postgres SQL blocking integration tests (optional DATABASE_URL)."""

from __future__ import annotations

import os

import pytest
from sqlmodel import Session, SQLModel, create_engine, select

from app.resolve.blocking import (
    CandidatePair,
    default_blocking_rules,
    resolve_blocking_backend,
    run_blocking_stage,
)
from app.resolve.blocking_sql import ensure_candidate_pair_unique_index
from app.resolve.standardize.staging import ResolutionInput

pytestmark = pytest.mark.integration


def _postgres_url() -> str | None:
    return os.environ.get("DATABASE_URL") or os.environ.get("TEST_DATABASE_URL")


@pytest.fixture(scope="module")
def pg_engine():
    url = _postgres_url()
    if not url or not url.startswith("postgresql"):
        pytest.skip("DATABASE_URL postgresql not set")
    engine = create_engine(url)
    SQLModel.metadata.create_all(
        engine,
        tables=[ResolutionInput.__table__, CandidatePair.__table__],
    )
    ensure_candidate_pair_unique_index(Session(engine))
    return engine


def test_resolve_blocking_backend_defaults_to_sql_on_postgres(pg_engine):
    with Session(pg_engine) as session:
        assert resolve_blocking_backend(session, {}) == "sql"


def test_run_blocking_stage_sql_dedupes_across_rules(pg_engine):
    with Session(pg_engine) as session:
        run_id = 9001
        session.add(
            ResolutionInput(
                run_id=run_id,
                source_type="unified_person",
                source_id="sql-p1",
                entity_type="person",
                raw_name="n1",
                raw_address="a1",
                first_name="John",
                last_name_phonetic="SM0",
                zip5="78701",
            )
        )
        session.add(
            ResolutionInput(
                run_id=run_id,
                source_type="unified_person",
                source_id="sql-p2",
                entity_type="person",
                raw_name="n2",
                raw_address="a2",
                first_name="Jon",
                last_name_phonetic="SM0",
                zip5="78701",
            )
        )
        session.commit()

        result = run_blocking_stage(
            session,
            run_id=run_id,
            config={"blocking_backend": "sql", "max_block_size": 500},
        )
        pairs = session.exec(
            select(CandidatePair).where(CandidatePair.run_id == run_id)
        ).all()

        assert result["pairs_compared"] == 1
        assert len(pairs) == 1
        assert pairs[0].rule_name == default_blocking_rules()[0].name
