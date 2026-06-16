"""Phase 4 integration tests — publish views end-to-end.

Covers:
- ``publish/__init__.py`` exports all required symbols.
- ``build_resolved_views`` + ``build_address_occupancy_view`` work together
  on a minimal in-memory schema.
- ``resolved_contributions`` returns rows joined to canonical entities.
- ``address_occupancy`` returns correct rows.
- ``python -m app.resolve publish --state texas --sqlite`` exits 0 and
  prints the expected view names.

TDD steps from task-4z-integration.md:
  Step 2 — write failing integration test
  Step 3 — fix wiring so tests pass

Task: 4z | Branch: resolve/phase-4/task-4z-integration
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_engine():
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


def _create_minimum_schema(session: Session) -> None:
    """Create the minimal tables required for the publish views."""
    stmts = [
        """
        CREATE TABLE IF NOT EXISTS canonical_address (
            id INTEGER PRIMARY KEY,
            standardized_line_1 TEXT,
            standardized_line_2 TEXT,
            city TEXT,
            state TEXT,
            zip5 TEXT,
            zip4 TEXT,
            frequency INTEGER NOT NULL DEFAULT 1
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS canonical_entity (
            id INTEGER PRIMARY KEY,
            canonical_name TEXT NOT NULL,
            entity_type TEXT NOT NULL DEFAULT 'person',
            canonical_address_id INTEGER,
            first_seen_date TEXT,
            last_seen_date TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS entity_crosswalk (
            id INTEGER PRIMARY KEY,
            source_type TEXT NOT NULL,
            source_id TEXT NOT NULL,
            canonical_entity_id INTEGER NOT NULL,
            run_id INTEGER
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS unified_transactions (
            id INTEGER PRIMARY KEY,
            transaction_id TEXT,
            amount NUMERIC,
            transaction_type TEXT,
            report_id INTEGER,
            report_ident TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS unified_contributions (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL,
            contributor_entity_id INTEGER NOT NULL,
            recipient_entity_id INTEGER NOT NULL,
            amount NUMERIC
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS unified_transaction_persons (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL,
            entity_id INTEGER,
            role TEXT NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS unified_loans (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL,
            lender_entity_id INTEGER NOT NULL,
            borrower_entity_id INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS unified_debts (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL,
            creditor_entity_id INTEGER NOT NULL,
            debtor_entity_id INTEGER NOT NULL
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS unified_credits (
            id INTEGER PRIMARY KEY,
            transaction_id INTEGER NOT NULL,
            payor_entity_id INTEGER NOT NULL,
            recipient_entity_id INTEGER NOT NULL
        )
        """,
    ]
    for stmt in stmts:
        session.execute(text(stmt))
    session.commit()


def _seed_contribution_with_entities(session: Session) -> None:
    """Seed a contribution row with two canonical entities and crosswalk rows."""
    session.execute(
        text(
            """
            INSERT INTO canonical_address (id, standardized_line_1, city, state, zip5, frequency)
            VALUES (10, '100 Main St', 'Austin', 'TX', '78701', 1)
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO canonical_entity
                (id, canonical_name, entity_type, canonical_address_id)
            VALUES
                (101, 'Alice Canonical', 'person', 10),
                (102, 'Committee Canonical', 'committee', 10)
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO entity_crosswalk (source_type, source_id, canonical_entity_id, run_id)
            VALUES
                ('unified_entity', '201', 101, 1),
                ('unified_entity', '202', 102, 1)
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO unified_transactions
                (id, transaction_id, amount, transaction_type, report_id, report_ident)
            VALUES
                (1, 'TXN-1', 500.00, 'contribution', 11, 'RPT-11')
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO unified_contributions
                (id, transaction_id, contributor_entity_id, recipient_entity_id, amount)
            VALUES (1, 1, 201, 202, 500.00)
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO unified_transaction_persons (id, transaction_id, entity_id, role)
            VALUES
                (1, 1, 201, 'contributor'),
                (2, 1, 202, 'recipient')
            """
        )
    )
    session.commit()


# ---------------------------------------------------------------------------
# Tests: package exports
# ---------------------------------------------------------------------------


class TestPublishPackageExports:
    """``app.resolve.publish`` must export all required symbols."""

    def test_build_resolved_views_exported(self):
        from app.resolve.publish import build_resolved_views

        assert callable(build_resolved_views)

    def test_build_address_occupancy_view_exported(self):
        from app.resolve.publish import build_address_occupancy_view

        assert callable(build_address_occupancy_view)

    def test_colocation_helpers_exported(self):
        from app.resolve.publish import (
            SelfColocationError,
            assert_colocation,
            find_colocated,
            suggest_colocations,
        )

        assert callable(find_colocated)
        assert callable(assert_colocation)
        assert callable(suggest_colocations)
        assert issubclass(SelfColocationError, ValueError)

    def test_crossstate_helpers_exported(self):
        from app.resolve.publish import (
            entities_for_master,
            get_master_entity,
            link_to_master,
        )

        assert callable(get_master_entity)
        assert callable(entities_for_master)
        assert callable(link_to_master)


# ---------------------------------------------------------------------------
# Tests: build_resolved_views + resolved_contributions return correct rows
# ---------------------------------------------------------------------------


class TestResolvedViewsIntegration:
    """After publish, resolved_contributions joins to canonical entities."""

    @pytest.fixture()
    def session(self):
        engine = _make_engine()
        with Session(engine) as db:
            _create_minimum_schema(db)
            yield db
        engine.dispose()

    def test_publish_creates_all_three_resolved_views(self, session):
        from app.resolve.publish import build_resolved_views

        _seed_contribution_with_entities(session)
        view_names = build_resolved_views(session)

        assert set(view_names) == {
            "resolved_transactions",
            "resolved_contributions",
            "resolved_expenditures",
            "resolved_reports",
            "cross_role_entities",
        }

    def test_resolved_contributions_joins_canonical_entities(self, session):
        from app.resolve.publish import build_resolved_views

        _seed_contribution_with_entities(session)
        build_resolved_views(session)

        row = (
            session.execute(
                text(
                    """
                    SELECT
                        contributor_canonical_entity_id,
                        contributor_canonical_name,
                        recipient_canonical_entity_id,
                        recipient_canonical_name
                    FROM resolved_contributions
                    WHERE id = 1
                    """
                )
            )
            .mappings()
            .one()
        )
        assert row["contributor_canonical_entity_id"] == 101
        assert row["contributor_canonical_name"] == "Alice Canonical"
        assert row["recipient_canonical_entity_id"] == 102
        assert row["recipient_canonical_name"] == "Committee Canonical"

    def test_resolved_contributions_returns_rows(self, session):
        from app.resolve.publish import build_resolved_views

        _seed_contribution_with_entities(session)
        build_resolved_views(session)

        count = session.execute(text("SELECT COUNT(*) FROM resolved_contributions")).scalar()
        assert count >= 1

    def test_publish_views_are_idempotent(self, session):
        from app.resolve.publish import build_resolved_views

        _seed_contribution_with_entities(session)
        first = build_resolved_views(session)
        second = build_resolved_views(session)

        assert set(first) == set(second)


# ---------------------------------------------------------------------------
# Tests: build_address_occupancy_view returns correct rows
# ---------------------------------------------------------------------------


class TestAddressOccupancyIntegration:
    """After publish, address_occupancy joins entities to their canonical address."""

    @pytest.fixture()
    def session(self):
        engine = _make_engine()
        with Session(engine) as db:
            _create_minimum_schema(db)
            yield db
        engine.dispose()

    def test_address_occupancy_view_created(self, session):
        from app.resolve.publish import build_address_occupancy_view

        _seed_contribution_with_entities(session)
        name = build_address_occupancy_view(session)

        assert name == "address_occupancy"

    def test_address_occupancy_returns_rows_for_seeded_entities(self, session):
        from app.resolve.publish import build_address_occupancy_view

        _seed_contribution_with_entities(session)
        build_address_occupancy_view(session)

        count = session.execute(text("SELECT COUNT(*) FROM address_occupancy")).scalar()
        assert count == 2

    def test_address_occupancy_row_columns(self, session):
        from app.resolve.publish import build_address_occupancy_view

        _seed_contribution_with_entities(session)
        build_address_occupancy_view(session)

        row = (
            session.execute(
                text(
                    """
                    SELECT
                        canonical_address_id,
                        canonical_entity_id,
                        entity_name,
                        entity_type,
                        role
                    FROM address_occupancy
                    WHERE canonical_entity_id = 101
                    """
                )
            )
            .mappings()
            .one()
        )
        assert row["canonical_address_id"] == 10
        assert row["canonical_entity_id"] == 101
        assert row["entity_name"] == "Alice Canonical"
        assert row["entity_type"] == "person"
        assert row["role"] == "resident"


# ---------------------------------------------------------------------------
# Tests: publish + occupancy together (combined publish call)
# ---------------------------------------------------------------------------


class TestPublishAll:
    """Both view builders run in sequence without error."""

    @pytest.fixture()
    def session(self):
        engine = _make_engine()
        with Session(engine) as db:
            _create_minimum_schema(db)
            yield db
        engine.dispose()

    def test_full_publish_creates_four_views(self, session):
        from app.resolve.publish import build_address_occupancy_view, build_resolved_views

        _seed_contribution_with_entities(session)
        resolved = build_resolved_views(session)
        occupancy = build_address_occupancy_view(session)

        all_views = set(resolved) | {occupancy}
        assert all_views == {
            "resolved_transactions",
            "resolved_contributions",
            "resolved_expenditures",
            "resolved_reports",
            "cross_role_entities",
            "address_occupancy",
        }


# ---------------------------------------------------------------------------
# Tests: CLI `publish` subcommand
# ---------------------------------------------------------------------------


class TestPublishCliSubcommand:
    """``python -m app.resolve publish --state texas --sqlite`` wiring."""

    def test_publish_help_exits_zero(self):
        from app.resolve.cli import main

        with pytest.raises(SystemExit) as exc_info:
            main(["publish", "--help"])
        assert exc_info.value.code == 0

    def test_publish_sqlite_exits_zero(self, capsys):
        from app.resolve.cli import main

        exit_code = main(["publish", "--state", "texas", "--sqlite"])
        assert exit_code == 0

    def test_publish_prints_view_names(self, capsys):
        from app.resolve.cli import main

        main(["publish", "--state", "texas", "--sqlite"])
        captured = capsys.readouterr()
        for name in (
            "resolved_transactions",
            "resolved_contributions",
            "resolved_expenditures",
            "address_occupancy",
        ):
            assert name in captured.out, f"expected {name!r} in stdout"

    def test_publish_invalid_state_returns_nonzero(self, capsys):
        from app.resolve.cli import main

        exit_code = main(["publish", "--state", "ZZZZZ", "--sqlite"])
        assert exit_code != 0

    def test_publish_materialized_rejected_for_sqlite(self, capsys):
        """--materialized raises ValueError for SQLite and exits non-zero."""
        from app.resolve.cli import main

        exit_code = main(["publish", "--state", "TX", "--sqlite", "--materialized"])
        assert exit_code != 0
