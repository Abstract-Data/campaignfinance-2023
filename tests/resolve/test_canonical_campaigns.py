"""Tests for canonical-campaign builder — Task 2c.

Covers the NULL election_year sentinel (election_cycle == 0) for officeholder
and multi-cycle committees that carry no election year.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text
from sqlmodel import Session

from app.resolve.publish.campaigns import build_canonical_campaigns

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def session() -> Session:
    """Fresh in-memory SQLite session with all tables needed by the builder."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    with Session(engine) as db:
        _create_tables(db)
        yield db
    engine.dispose()


def _create_tables(session: Session) -> None:
    """Create minimal table DDL for the campaign builder (raw SQL, no FK deps)."""
    session.execute(
        text(
            """
            CREATE TABLE unified_campaigns (
                id INTEGER PRIMARY KEY,
                primary_committee_id TEXT,
                candidate_person_id TEXT,
                election_year INTEGER,
                office_sought TEXT,
                name TEXT
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE entity_crosswalk (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                canonical_entity_id INTEGER NOT NULL,
                run_id INTEGER
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE canonical_entity (
                id INTEGER PRIMARY KEY,
                canonical_name TEXT NOT NULL
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE canonical_campaign (
                id INTEGER PRIMARY KEY,
                uuid TEXT NOT NULL UNIQUE,
                committee_entity_id INTEGER NOT NULL,
                office_normalized TEXT,
                election_cycle INTEGER NOT NULL,
                candidate_entity_id INTEGER,
                canonical_name TEXT,
                state_code TEXT NOT NULL,
                district TEXT,
                last_run_id INTEGER,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
    )
    session.commit()


def _seed_officeholder(session: Session) -> None:
    """Insert a canonical entity, crosswalk entry, and one unified_campaign
    row with election_year = NULL (the officeholder case)."""
    session.execute(
        text("INSERT INTO canonical_entity (id, canonical_name) VALUES (1, 'Officeholder PAC')")
    )
    session.execute(
        text(
            """
            INSERT INTO entity_crosswalk
                (source_type, source_id, canonical_entity_id, run_id)
            VALUES ('unified_committee', 'COM-001', 1, 1)
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO unified_campaigns
                (id, primary_committee_id, candidate_person_id, election_year, office_sought, name)
            VALUES (10, 'COM-001', NULL, NULL, 'governor', 'Officeholder PAC Campaign')
            """
        )
    )
    session.commit()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestNullElectionYearSentinel:
    def test_officeholder_committee_produces_one_row(self, session: Session) -> None:
        """A committee with NULL election_year must produce exactly one canonical_campaign."""
        _seed_officeholder(session)

        count = build_canonical_campaigns(session, state_code="TX")

        assert count == 1

    def test_election_cycle_is_zero_sentinel(self, session: Session) -> None:
        """election_cycle must be 0 when election_year is NULL."""
        _seed_officeholder(session)
        build_canonical_campaigns(session, state_code="TX")

        row = session.execute(
            text("SELECT election_cycle FROM canonical_campaign WHERE state_code = 'TX'")
        ).fetchone()
        assert row is not None
        assert row[0] == 0, f"Expected election_cycle=0 (sentinel), got {row[0]}"

    def test_idempotent_two_consecutive_runs_no_duplicate(self, session: Session) -> None:
        """Two consecutive build_canonical_campaigns calls must yield exactly one
        canonical_campaign row (builder is delete-and-rebuild; no duplicate)."""
        _seed_officeholder(session)

        first_count = build_canonical_campaigns(session, state_code="TX")
        second_count = build_canonical_campaigns(session, state_code="TX")

        assert first_count == 1
        assert second_count == 1

        # Confirm exactly one row in the table after both runs.
        total = session.execute(
            text("SELECT COUNT(*) FROM canonical_campaign WHERE state_code = 'TX'")
        ).scalar_one()
        assert total == 1, f"Expected 1 row after two runs, found {total}"

    def test_regular_campaign_unaffected(self, session: Session) -> None:
        """A campaign with a non-NULL election_year is still picked up correctly."""
        session.execute(
            text("INSERT INTO canonical_entity (id, canonical_name) VALUES (2, 'Regular PAC')")
        )
        session.execute(
            text(
                """
                INSERT INTO entity_crosswalk
                    (source_type, source_id, canonical_entity_id, run_id)
                VALUES ('unified_committee', 'COM-002', 2, 1)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO unified_campaigns
                    (id, primary_committee_id, candidate_person_id, election_year, office_sought, name)
                VALUES (20, 'COM-002', NULL, 2024, 'senate', 'Regular PAC 2024')
                """
            )
        )
        session.commit()

        count = build_canonical_campaigns(session, state_code="TX")
        assert count == 1

        row = session.execute(
            text("SELECT election_cycle FROM canonical_campaign WHERE state_code = 'TX'")
        ).fetchone()
        assert row is not None
        assert row[0] == 2024

    def test_mixed_null_and_non_null_election_years(self, session: Session) -> None:
        """Both a NULL and a non-NULL election_year committee produce separate rows."""
        _seed_officeholder(session)
        session.execute(
            text("INSERT INTO canonical_entity (id, canonical_name) VALUES (2, 'Regular PAC')")
        )
        session.execute(
            text(
                """
                INSERT INTO entity_crosswalk
                    (source_type, source_id, canonical_entity_id, run_id)
                VALUES ('unified_committee', 'COM-002', 2, 1)
                """
            )
        )
        session.execute(
            text(
                """
                INSERT INTO unified_campaigns
                    (id, primary_committee_id, candidate_person_id, election_year, office_sought, name)
                VALUES (20, 'COM-002', NULL, 2026, 'governor', 'Regular PAC 2026')
                """
            )
        )
        session.commit()

        count = build_canonical_campaigns(session, state_code="TX")
        assert count == 2

        cycles = sorted(
            row[0]
            for row in session.execute(
                text("SELECT election_cycle FROM canonical_campaign WHERE state_code = 'TX'")
            ).fetchall()
        )
        assert cycles == [0, 2026]
