"""Publish builder for the ``address_occupancy`` analytics view."""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session

ADDRESS_OCCUPANCY_VIEW_NAME = "address_occupancy"

_ROLE_WHEN_PERSON_SQLITE = "WHEN lower(ce.entity_type) = 'person' THEN 'resident'"
_ROLE_WHEN_PERSON_POSTGRES = "WHEN lower(ce.entity_type::text) = 'person' THEN 'resident'"

_ADDRESS_OCCUPANCY_SELECT_BODY = """
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
        __ROLE_WHEN_PERSON__
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


def _occupancy_select_sql(dialect_name: str) -> str:
    role_case = (
        _ROLE_WHEN_PERSON_POSTGRES if dialect_name == "postgresql" else _ROLE_WHEN_PERSON_SQLITE
    )
    return _ADDRESS_OCCUPANCY_SELECT_BODY.replace("__ROLE_WHEN_PERSON__", role_case)


_DROP_SQLITE_VIEW_SQL = "DROP VIEW IF EXISTS address_occupancy"


def build_address_occupancy_view(session: Session) -> str:
    """Create (or replace) the ``address_occupancy`` view and return its name.

    Commits the current transaction on success.
    """
    bind = session.get_bind()
    if bind is None:
        raise RuntimeError("Session is not bound to an engine/connection.")

    dialect_name = bind.dialect.name
    select_sql = _occupancy_select_sql(dialect_name)
    if dialect_name == "postgresql":
        session.execute(text("CREATE OR REPLACE VIEW address_occupancy AS " + select_sql))
    else:
        session.execute(text(_DROP_SQLITE_VIEW_SQL))
        session.execute(text("CREATE VIEW address_occupancy AS " + select_sql))

    session.commit()
    return ADDRESS_OCCUPANCY_VIEW_NAME
