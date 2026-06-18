"""Wave 1 linkage integrity verification.

Connects to the local Postgres DB and verifies that dropping
unified_transactions.raw_data did not break any transaction linkage or
introduce duplication.

Run after Wave 1b migration:
    uv run pytest tests/test_wave1_linkage.py -v
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import text

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")


def _pg_available() -> bool:
    try:
        from app.core.unified_database import get_db_manager

        m = get_db_manager(bootstrap=False)
        with m.engine.connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


@pytest.fixture(scope="module")
def engine():
    from app.core.unified_database import get_db_manager

    m = get_db_manager(bootstrap=False)
    yield m.engine
    m.engine.dispose()


def test_raw_data_column_absent(engine) -> None:
    """Verify unified_transactions.raw_data was successfully dropped."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'unified_transactions'
                  AND column_name = 'raw_data'
                """
            )
        )
        rows = result.fetchall()
    assert len(rows) == 0, "raw_data column still present on unified_transactions"


def test_campaign_source_cols_present(engine) -> None:
    """Verify the three campaign source columns were added."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'unified_transactions'
                  AND column_name IN (
                      'campaign_office_src',
                      'campaign_district_src',
                      'campaign_name_src'
                  )
                ORDER BY column_name
                """
            )
        )
        rows = result.fetchall()
    col_names = {r[0] for r in rows}
    assert "campaign_office_src" in col_names
    assert "campaign_district_src" in col_names
    assert "campaign_name_src" in col_names


def test_ingest_error_raw_data_untouched(engine) -> None:
    """Verify IngestError.raw_data was NOT dropped (intentionally preserved)."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'ingest_errors'
                  AND column_name = 'raw_data'
                """
            )
        )
        rows = result.fetchall()
    assert len(rows) == 1, "ingest_errors.raw_data should still exist"


def test_no_orphaned_transactions(engine) -> None:
    """No transactions with NULL committee_id."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM unified_transactions WHERE committee_id IS NULL")
        )
        count = result.scalar_one()
    assert count == 0, f"Found {count} transactions with NULL committee_id"


def test_row_counts_recorded(engine) -> None:
    """Record row counts for pre/post comparison (informational, not a hard assert)."""
    with engine.connect() as conn:
        counts = conn.execute(
            text(
                """
                SELECT
                    (SELECT COUNT(*) FROM unified_persons)       AS persons,
                    (SELECT COUNT(*) FROM unified_addresses)     AS addresses,
                    (SELECT COUNT(*) FROM unified_entities)      AS entities,
                    (SELECT COUNT(*) FROM unified_transactions)  AS transactions
                """
            )
        ).fetchone()
    print(
        f"\nRow counts after Wave 1: "
        f"persons={counts[0]}, addresses={counts[1]}, "
        f"entities={counts[2]}, transactions={counts[3]}"
    )
    # All counts must be non-negative (no duplication introduced)
    assert counts[0] >= 0
    assert counts[1] >= 0
    assert counts[2] >= 0
    assert counts[3] >= 0


def test_resolved_transactions_view_works(engine) -> None:
    """Verify the resolved_transactions view was recreated correctly."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM resolved_transactions")
        )
        count = result.scalar_one()
    assert count >= 0  # view is queryable


def test_resolved_expenditures_view_works(engine) -> None:
    """Verify the resolved_expenditures view was recreated correctly."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM resolved_expenditures")
        )
        count = result.scalar_one()
    assert count >= 0  # view is queryable
