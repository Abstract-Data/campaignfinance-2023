"""Published resolved views for canonical-entity analysis."""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session

_RESOLVED_TRANSACTIONS_SELECT = """
SELECT
    ut.*,
    contributor_role.entity_id AS contributor_source_entity_id,
    contributor_ce.id AS contributor_canonical_entity_id,
    contributor_ce.canonical_name AS contributor_canonical_name,
    recipient_role.entity_id AS recipient_source_entity_id,
    recipient_ce.id AS recipient_canonical_entity_id,
    recipient_ce.canonical_name AS recipient_canonical_name,
    payee_role.entity_id AS payee_source_entity_id,
    payee_ce.id AS payee_canonical_entity_id,
    payee_ce.canonical_name AS payee_canonical_name
FROM unified_transactions ut
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE role = 'contributor'
    GROUP BY transaction_id
) AS contributor_role ON contributor_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk contributor_xw
    ON contributor_xw.source_type = 'unified_entity'
    AND contributor_xw.source_id = CAST(contributor_role.entity_id AS TEXT)
LEFT JOIN canonical_entity contributor_ce
    ON contributor_ce.id = contributor_xw.canonical_entity_id
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE role = 'recipient'
    GROUP BY transaction_id
) AS recipient_role ON recipient_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk recipient_xw
    ON recipient_xw.source_type = 'unified_entity'
    AND recipient_xw.source_id = CAST(recipient_role.entity_id AS TEXT)
LEFT JOIN canonical_entity recipient_ce
    ON recipient_ce.id = recipient_xw.canonical_entity_id
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE role = 'payee'
    GROUP BY transaction_id
) AS payee_role ON payee_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk payee_xw
    ON payee_xw.source_type = 'unified_entity'
    AND payee_xw.source_id = CAST(payee_role.entity_id AS TEXT)
LEFT JOIN canonical_entity payee_ce
    ON payee_ce.id = payee_xw.canonical_entity_id
"""

_RESOLVED_CONTRIBUTIONS_SELECT = """
SELECT
    uc.*,
    contributor_ce.id AS contributor_canonical_entity_id,
    contributor_ce.canonical_name AS contributor_canonical_name,
    recipient_ce.id AS recipient_canonical_entity_id,
    recipient_ce.canonical_name AS recipient_canonical_name
FROM unified_contributions uc
LEFT JOIN entity_crosswalk contributor_xw
    ON contributor_xw.source_type = 'unified_entity'
    AND contributor_xw.source_id = CAST(uc.contributor_entity_id AS TEXT)
LEFT JOIN canonical_entity contributor_ce
    ON contributor_ce.id = contributor_xw.canonical_entity_id
LEFT JOIN entity_crosswalk recipient_xw
    ON recipient_xw.source_type = 'unified_entity'
    AND recipient_xw.source_id = CAST(uc.recipient_entity_id AS TEXT)
LEFT JOIN canonical_entity recipient_ce
    ON recipient_ce.id = recipient_xw.canonical_entity_id
"""

_RESOLVED_EXPENDITURES_SELECT = """
SELECT
    ut.*,
    payee_role.entity_id AS payee_source_entity_id,
    payee_ce.id AS payee_canonical_entity_id,
    payee_ce.canonical_name AS payee_canonical_name
FROM unified_transactions ut
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE role = 'payee'
    GROUP BY transaction_id
) AS payee_role ON payee_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk payee_xw
    ON payee_xw.source_type = 'unified_entity'
    AND payee_xw.source_id = CAST(payee_role.entity_id AS TEXT)
LEFT JOIN canonical_entity payee_ce
    ON payee_ce.id = payee_xw.canonical_entity_id
WHERE ut.transaction_type = 'expenditure'
"""

_VIEW_SELECTS: tuple[tuple[str, str], ...] = (
    ("resolved_transactions", _RESOLVED_TRANSACTIONS_SELECT),
    ("resolved_contributions", _RESOLVED_CONTRIBUTIONS_SELECT),
    ("resolved_expenditures", _RESOLVED_EXPENDITURES_SELECT),
)

_DROP_SQLITE_VIEW_SQL = {
    "resolved_transactions": "DROP VIEW IF EXISTS resolved_transactions",
    "resolved_contributions": "DROP VIEW IF EXISTS resolved_contributions",
    "resolved_expenditures": "DROP VIEW IF EXISTS resolved_expenditures",
}

_CREATE_SQLITE_VIEW_SQL = {
    "resolved_transactions": f"CREATE VIEW resolved_transactions AS {_RESOLVED_TRANSACTIONS_SELECT}",
    "resolved_contributions": f"CREATE VIEW resolved_contributions AS {_RESOLVED_CONTRIBUTIONS_SELECT}",
    "resolved_expenditures": f"CREATE VIEW resolved_expenditures AS {_RESOLVED_EXPENDITURES_SELECT}",
}

_CREATE_POSTGRES_VIEW_SQL = {
    "resolved_transactions": (
        f"CREATE OR REPLACE VIEW resolved_transactions AS {_RESOLVED_TRANSACTIONS_SELECT}"
    ),
    "resolved_contributions": (
        f"CREATE OR REPLACE VIEW resolved_contributions AS {_RESOLVED_CONTRIBUTIONS_SELECT}"
    ),
    "resolved_expenditures": (
        f"CREATE OR REPLACE VIEW resolved_expenditures AS {_RESOLVED_EXPENDITURES_SELECT}"
    ),
}

_DROP_POSTGRES_MAT_VIEW_SQL = {
    "resolved_transactions": "DROP MATERIALIZED VIEW IF EXISTS resolved_transactions",
    "resolved_contributions": "DROP MATERIALIZED VIEW IF EXISTS resolved_contributions",
    "resolved_expenditures": "DROP MATERIALIZED VIEW IF EXISTS resolved_expenditures",
}

_CREATE_POSTGRES_MAT_VIEW_SQL = {
    "resolved_transactions": (
        f"CREATE MATERIALIZED VIEW resolved_transactions AS {_RESOLVED_TRANSACTIONS_SELECT}"
    ),
    "resolved_contributions": (
        f"CREATE MATERIALIZED VIEW resolved_contributions AS {_RESOLVED_CONTRIBUTIONS_SELECT}"
    ),
    "resolved_expenditures": (
        f"CREATE MATERIALIZED VIEW resolved_expenditures AS {_RESOLVED_EXPENDITURES_SELECT}"
    ),
}


def build_resolved_views(session: Session, materialized: bool = False) -> list[str]:
    """Create the resolved publish views and return their names.

    Commits the current transaction on success.
    """
    bind = session.get_bind()
    if bind is None:
        raise RuntimeError("Session is not bound to an engine/connection.")

    dialect_name = bind.dialect.name
    if materialized and dialect_name != "postgresql":
        raise ValueError("materialized=True is only supported for PostgreSQL.")

    for view_name, _ in _VIEW_SELECTS:
        if materialized:
            session.execute(text(_DROP_POSTGRES_MAT_VIEW_SQL[view_name]))
            session.execute(text(_CREATE_POSTGRES_MAT_VIEW_SQL[view_name]))
            continue
        if dialect_name == "postgresql":
            session.execute(text(_CREATE_POSTGRES_VIEW_SQL[view_name]))
            continue
        session.execute(text(_DROP_SQLITE_VIEW_SQL[view_name]))
        session.execute(text(_CREATE_SQLITE_VIEW_SQL[view_name]))

    session.commit()
    return [view_name for view_name, _ in _VIEW_SELECTS]
