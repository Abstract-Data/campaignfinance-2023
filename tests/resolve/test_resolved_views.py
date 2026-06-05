"""Tests for Phase 4 publish resolved views (task-4a)."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlmodel import Session, create_engine

from app.resolve.publish.views import build_resolved_views


@pytest.fixture()
def session() -> Session:
    engine = create_engine("sqlite:///:memory:", echo=False)
    with Session(engine) as db:
        _create_base_tables(db)
        yield db
    engine.dispose()


def _create_base_tables(session: Session) -> None:
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
            CREATE TABLE unified_transactions (
                id INTEGER PRIMARY KEY,
                transaction_id TEXT,
                amount NUMERIC,
                transaction_type TEXT,
                transaction_date DATE,
                campaign_id INTEGER,
                report_id INTEGER,
                report_ident TEXT
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE unified_campaigns (
                id INTEGER PRIMARY KEY,
                primary_committee_id TEXT,
                election_year INTEGER,
                office_sought TEXT
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE unified_reports (
                id INTEGER PRIMARY KEY,
                committee_id TEXT,
                period_end DATE
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE canonical_campaign (
                id INTEGER PRIMARY KEY,
                committee_entity_id INTEGER,
                office_normalized TEXT,
                election_cycle INTEGER,
                candidate_entity_id INTEGER,
                canonical_name TEXT
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE canonical_name_history (
                id INTEGER PRIMARY KEY,
                subject_type TEXT NOT NULL,
                subject_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                normalized_name TEXT,
                first_seen_date DATE,
                last_seen_date DATE,
                occurrence_count INTEGER
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE unified_contributions (
                id INTEGER PRIMARY KEY,
                transaction_id INTEGER NOT NULL,
                contributor_entity_id INTEGER NOT NULL,
                recipient_entity_id INTEGER NOT NULL,
                amount NUMERIC
            )
            """
        )
    )
    session.execute(
        text(
            """
            CREATE TABLE unified_transaction_persons (
                id INTEGER PRIMARY KEY,
                transaction_id INTEGER NOT NULL,
                entity_id INTEGER,
                role TEXT NOT NULL
            )
            """
        )
    )
    session.commit()


def _seed_resolved_contribution(session: Session) -> None:
    session.execute(
        text(
            """
            INSERT INTO canonical_entity (id, canonical_name)
            VALUES
                (101, 'Alice Canonical'),
                (102, 'Committee Canonical')
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
                (1, 'TXN-1', :amount, 'contribution', 11, 'RPT-11')
            """
        ),
        {"amount": 250.00},
    )
    session.execute(
        text(
            """
            INSERT INTO unified_contributions
                (id, transaction_id, contributor_entity_id, recipient_entity_id, amount)
            VALUES
                (1, 1, 201, 202, :amount)
            """
        ),
        {"amount": 250.00},
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


def test_build_resolved_views_creates_views_and_resolves_contributions(session: Session) -> None:
    _seed_resolved_contribution(session)

    view_names = build_resolved_views(session)

    assert set(view_names) == {
        "resolved_transactions",
        "resolved_contributions",
        "resolved_expenditures",
        "resolved_reports",
    }

    row = session.execute(
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
    ).mappings().one()
    assert row["contributor_canonical_entity_id"] == 101
    assert row["contributor_canonical_name"] == "Alice Canonical"
    assert row["recipient_canonical_entity_id"] == 102
    assert row["recipient_canonical_name"] == "Committee Canonical"


def test_uncrosswalked_party_is_preserved_with_null_canonical_columns(session: Session) -> None:
    session.execute(
        text(
            """
            INSERT INTO unified_transactions
                (id, transaction_id, amount, transaction_type, report_id, report_ident)
            VALUES
                (2, 'TXN-MISSING-XWALK', 99.00, 'expenditure', 22, 'RPT-22')
            """
        )
    )
    session.execute(
        text(
            """
            INSERT INTO unified_transaction_persons (id, transaction_id, entity_id, role)
            VALUES
                (3, 2, 9999, 'payee')
            """
        )
    )
    session.commit()

    build_resolved_views(session)

    row = session.execute(
        text(
            """
            SELECT
                id,
                payee_source_entity_id,
                payee_canonical_entity_id,
                payee_canonical_name
            FROM resolved_transactions
            WHERE id = 2
            """
        )
    ).mappings().one()
    assert row["id"] == 2
    assert row["payee_source_entity_id"] == 9999
    assert row["payee_canonical_entity_id"] is None
    assert row["payee_canonical_name"] is None


def test_build_resolved_views_materialized_requires_postgresql(
    session: Session,
) -> None:
    with pytest.raises(ValueError, match="materialized=True is only supported"):
        build_resolved_views(session, materialized=True)


def test_build_resolved_views_is_idempotent(session: Session) -> None:
    _seed_resolved_contribution(session)

    first = build_resolved_views(session)
    second = build_resolved_views(session)

    assert first == second

    views = session.execute(
        text(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'view'
              AND name IN (
                'resolved_transactions',
                'resolved_contributions',
                'resolved_expenditures'
              )
            ORDER BY name
            """
        )
    ).scalars().all()
    assert views == [
        "resolved_contributions",
        "resolved_expenditures",
        "resolved_transactions",
    ]
