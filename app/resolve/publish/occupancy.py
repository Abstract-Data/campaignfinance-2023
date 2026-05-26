"""Publish builder for the ``address_occupancy`` analytics view."""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session

ADDRESS_OCCUPANCY_VIEW_NAME = "address_occupancy"

_ADDRESS_OCCUPANCY_SELECT = """
WITH entity_transactions AS (
    SELECT
        ec.canonical_entity_id,
        uc.transaction_id
    FROM unified_contributions AS uc
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(uc.contributor_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        uc.transaction_id
    FROM unified_contributions AS uc
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(uc.recipient_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        ul.transaction_id
    FROM unified_loans AS ul
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(ul.lender_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        ul.transaction_id
    FROM unified_loans AS ul
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(ul.borrower_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        ud.transaction_id
    FROM unified_debts AS ud
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(ud.creditor_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        ud.transaction_id
    FROM unified_debts AS ud
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(ud.debtor_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        ucr.transaction_id
    FROM unified_credits AS ucr
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(ucr.payor_entity_id AS TEXT)

    UNION ALL

    SELECT
        ec.canonical_entity_id,
        ucr.transaction_id
    FROM unified_credits AS ucr
    JOIN entity_crosswalk AS ec
      ON ec.source_type = 'unified_entity'
     AND ec.source_id = CAST(ucr.recipient_entity_id AS TEXT)
),
entity_transaction_rollup AS (
    SELECT
        canonical_entity_id,
        COUNT(DISTINCT transaction_id) AS transaction_count
    FROM entity_transactions
    GROUP BY canonical_entity_id
)
SELECT
    ca.id AS canonical_address_id,
    ca.standardized_line_1,
    ca.standardized_line_2,
    ca.city,
    ca.state,
    ca.zip5,
    ca.zip4,
    ce.id AS canonical_entity_id,
    ce.canonical_name AS entity_name,
    ce.entity_type,
    CASE
        WHEN ce.entity_type = 'person' THEN 'resident'
        ELSE 'registered'
    END AS role,
    COALESCE(etr.transaction_count, 0) AS transaction_count,
    ce.first_seen_date,
    ce.last_seen_date
FROM canonical_entity AS ce
JOIN canonical_address AS ca
  ON ca.id = ce.canonical_address_id
LEFT JOIN entity_transaction_rollup AS etr
  ON etr.canonical_entity_id = ce.id
"""

_DROP_SQLITE_VIEW_SQL = "DROP VIEW IF EXISTS address_occupancy"

_CREATE_SQLITE_VIEW_SQL = (
    "CREATE VIEW address_occupancy AS " + _ADDRESS_OCCUPANCY_SELECT
)

_CREATE_POSTGRES_VIEW_SQL = (
    "CREATE OR REPLACE VIEW address_occupancy AS " + _ADDRESS_OCCUPANCY_SELECT
)


def build_address_occupancy_view(session: Session) -> str:
    """Create (or replace) the ``address_occupancy`` view and return its name.

    Commits the current transaction on success.
    """
    bind = session.get_bind()
    if bind is None:
        raise RuntimeError("Session is not bound to an engine/connection.")

    if bind.dialect.name == "postgresql":
        session.execute(text(_CREATE_POSTGRES_VIEW_SQL))
    else:
        session.execute(text(_DROP_SQLITE_VIEW_SQL))
        session.execute(text(_CREATE_SQLITE_VIEW_SQL))

    session.commit()
    return ADDRESS_OCCUPANCY_VIEW_NAME
