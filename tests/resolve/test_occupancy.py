"""Tests for the address_occupancy publish view (Task 4b)."""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session, create_engine

from app.resolve.publish.occupancy import build_address_occupancy_view


def _create_minimum_schema(session: Session) -> None:
    session.exec(
        text(
            """
            CREATE TABLE canonical_address (
                id INTEGER PRIMARY KEY,
                standardized_line_1 TEXT,
                standardized_line_2 TEXT,
                city TEXT,
                state TEXT,
                zip5 TEXT,
                zip4 TEXT
            )
            """
        )
    )
    session.exec(
        text(
            """
            CREATE TABLE canonical_entity (
                id INTEGER PRIMARY KEY,
                canonical_address_id INTEGER,
                canonical_name TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                first_seen_date TEXT,
                last_seen_date TEXT
            )
            """
        )
    )
    session.exec(
        text(
            """
            CREATE TABLE entity_crosswalk (
                id INTEGER PRIMARY KEY,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                canonical_entity_id INTEGER NOT NULL
            )
            """
        )
    )
    session.exec(
        text(
            """
            CREATE TABLE unified_contributions (
                id INTEGER PRIMARY KEY,
                transaction_id INTEGER NOT NULL,
                contributor_entity_id INTEGER NOT NULL,
                recipient_entity_id INTEGER NOT NULL
            )
            """
        )
    )
    session.exec(
        text(
            """
            CREATE TABLE unified_loans (
                id INTEGER PRIMARY KEY,
                transaction_id INTEGER NOT NULL,
                lender_entity_id INTEGER NOT NULL,
                borrower_entity_id INTEGER NOT NULL
            )
            """
        )
    )
    session.exec(
        text(
            """
            CREATE TABLE unified_debts (
                id INTEGER PRIMARY KEY,
                transaction_id INTEGER NOT NULL,
                creditor_entity_id INTEGER NOT NULL,
                debtor_entity_id INTEGER NOT NULL
            )
            """
        )
    )
    session.exec(
        text(
            """
            CREATE TABLE unified_credits (
                id INTEGER PRIMARY KEY,
                transaction_id INTEGER NOT NULL,
                payor_entity_id INTEGER NOT NULL,
                recipient_entity_id INTEGER NOT NULL
            )
            """
        )
    )
    session.commit()


def _seed_shared_address_data(session: Session) -> int:
    address_id = 10
    session.exec(
        text(
            """
            INSERT INTO canonical_address (
                id, standardized_line_1, city, state, zip5
            )
            VALUES
                (10, '100 MAIN ST', 'AUSTIN', 'TX', '78701')
            """
        )
    )
    session.exec(
        text(
            """
            INSERT INTO canonical_entity (
                id, canonical_address_id, canonical_name, entity_type,
                first_seen_date, last_seen_date
            )
            VALUES
                (101, 10, 'Alice Example', 'person', '2024-01-01', '2024-03-01'),
                (102, 10, 'Example Consulting LLC', 'organization', '2024-01-15', '2024-04-01'),
                (103, 10, 'Friends of Example PAC', 'committee', '2024-02-01', '2024-04-15')
            """
        )
    )
    session.exec(
        text(
            """
            INSERT INTO entity_crosswalk (
                id, source_type, source_id, canonical_entity_id
            )
            VALUES
                (1, 'unified_entity', '1001', 101),
                (2, 'unified_entity', '1002', 102),
                (3, 'unified_entity', '1003', 103)
            """
        )
    )
    session.exec(
        text(
            """
            INSERT INTO unified_contributions (
                id, transaction_id, contributor_entity_id, recipient_entity_id
            )
            VALUES
                (1, 501, 1001, 1002),
                (2, 502, 1001, 1003),
                (3, 503, 1001, 1002),
                (4, 504, 1002, 1003)
            """
        )
    )
    session.commit()
    return address_id


def test_build_address_occupancy_view_lists_distinct_entities_with_transaction_counts() -> None:
    engine = create_engine("sqlite://")
    with Session(engine) as session:
        _create_minimum_schema(session)
        address_id = _seed_shared_address_data(session)

        view_name = build_address_occupancy_view(session)
        assert view_name == "address_occupancy"

        rows = session.execute(
            text(
                """
                SELECT canonical_entity_id, transaction_count
                FROM address_occupancy
                WHERE canonical_address_id = :address_id
                ORDER BY canonical_entity_id
                """
            ),
            {"address_id": address_id},
        ).all()

        assert len(rows) == 3
        assert [row[0] for row in rows] == [101, 102, 103]
        assert {row[0]: row[1] for row in rows} == {101: 3, 102: 3, 103: 2}

        roles = session.execute(
            text(
                """
                SELECT canonical_entity_id, role
                FROM address_occupancy
                WHERE canonical_address_id = :address_id
                ORDER BY canonical_entity_id
                """
            ),
            {"address_id": address_id},
        ).all()
        assert roles == [(101, "resident"), (102, "registered"), (103, "registered")]


def test_address_occupancy_supports_counting_entities_at_an_address() -> None:
    engine = create_engine("sqlite://")
    with Session(engine) as session:
        _create_minimum_schema(session)
        address_id = _seed_shared_address_data(session)
        build_address_occupancy_view(session)

        count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM address_occupancy
                WHERE canonical_address_id = :address_id
                """
            ),
            {"address_id": address_id},
        ).scalar_one()

        assert count == 3


def test_build_address_occupancy_view_is_idempotent() -> None:
    engine = create_engine("sqlite://")
    with Session(engine) as session:
        _create_minimum_schema(session)
        address_id = _seed_shared_address_data(session)

        first = build_address_occupancy_view(session)
        second = build_address_occupancy_view(session)

        assert first == "address_occupancy"
        assert second == "address_occupancy"

        count = session.execute(
            text(
                """
                SELECT COUNT(*)
                FROM address_occupancy
                WHERE canonical_address_id = :address_id
                """
            ),
            {"address_id": address_id},
        ).scalar_one()
        assert count == 3
