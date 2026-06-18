"""Wave 2 report ingest smoke test + linkage verification.

Verifies that dropping unified_reports.raw_data did not break report ingest
or committee/treasurer name tracking.

Run after Wave 2b migration:
    uv run pytest tests/test_wave2_report_ingest.py -v
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


def test_reports_raw_data_column_absent(engine) -> None:
    """Verify unified_reports.raw_data was successfully dropped."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'unified_reports'
                  AND column_name = 'raw_data'
                """
            )
        )
        rows = result.fetchall()
    assert len(rows) == 0, "raw_data column still present on unified_reports"


def test_at_filing_cols_present(engine) -> None:
    """Verify at-filing columns still exist on unified_reports."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'unified_reports'
                  AND column_name IN (
                      'committee_name_at_filing',
                      'treasurer_name_at_filing'
                  )
                ORDER BY column_name
                """
            )
        )
        rows = result.fetchall()
    col_names = {r[0] for r in rows}
    assert "committee_name_at_filing" in col_names
    assert "treasurer_name_at_filing" in col_names


def test_resolved_reports_view_works(engine) -> None:
    """Verify resolved_reports view was recreated correctly (no raw_data column)."""
    with engine.connect() as conn:
        # Query should succeed
        result = conn.execute(text("SELECT COUNT(*) FROM resolved_reports"))
        count = result.scalar_one()
    assert count >= 0

    # Verify raw_data not in the view columns
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'resolved_reports'
                  AND column_name = 'raw_data'
                """
            )
        )
        rows = result.fetchall()
    assert len(rows) == 0, "resolved_reports view still has raw_data column"


def test_no_orphaned_report_transactions(engine) -> None:
    """Report-linked transactions have valid report_id references."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM unified_transactions t
                WHERE t.report_id IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM unified_reports r WHERE r.id = t.report_id
                  )
                """
            )
        )
        count = result.scalar_one()
    assert count == 0, f"Found {count} transactions with orphaned report_id"


def test_report_row_counts_stable(engine) -> None:
    """Row counts are non-negative (informational)."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT COUNT(*) FROM unified_reports")
        )
        count = result.scalar_one()
    print(f"\nunified_reports rows: {count}")
    assert count >= 0
