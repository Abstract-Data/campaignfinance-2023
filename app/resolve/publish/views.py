"""Published resolved views for canonical-entity analysis."""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session

# The crosswalk is append-only: every resolution run inserts a fresh set of
# rows tagged with its ``run_id``.  Joining it unfiltered fans every resolved
# row out by the number of runs, so each join below restricts to the most
# recent run via ``run_id = (SELECT MAX(run_id) FROM entity_crosswalk)``.  This
# keeps the views 1:1 (``(source_type, source_id)`` is unique within a run) and
# resilient to re-resolution.

_RESOLVED_TRANSACTIONS_SELECT = """
SELECT
    ut.*,
    contributor_role.entity_id AS contributor_source_entity_id,
    contributor_ce.id AS contributor_canonical_entity_id,
    contributor_ce.canonical_name AS contributor_canonical_name,
    COALESCE(
        (
            SELECT nh.name
            FROM canonical_name_history nh
            WHERE nh.subject_type = 'entity'
              AND nh.subject_id = contributor_ce.id
              AND (nh.first_seen_date IS NULL OR nh.first_seen_date <= ut.transaction_date)
              AND (nh.last_seen_date IS NULL OR nh.last_seen_date >= ut.transaction_date)
            ORDER BY nh.occurrence_count DESC
            LIMIT 1
        ),
        contributor_ce.canonical_name
    ) AS contributor_name_as_of,
    recipient_role.entity_id AS recipient_source_entity_id,
    recipient_ce.id AS recipient_canonical_entity_id,
    recipient_ce.canonical_name AS recipient_canonical_name,
    payee_role.entity_id AS payee_source_entity_id,
    payee_ce.id AS payee_canonical_entity_id,
    payee_ce.canonical_name AS payee_canonical_name,
    COALESCE(
        (
            SELECT nh.name
            FROM canonical_name_history nh
            WHERE nh.subject_type = 'entity'
              AND nh.subject_id = payee_ce.id
              AND (nh.first_seen_date IS NULL OR nh.first_seen_date <= ut.transaction_date)
              AND (nh.last_seen_date IS NULL OR nh.last_seen_date >= ut.transaction_date)
            ORDER BY nh.occurrence_count DESC
            LIMIT 1
        ),
        payee_ce.canonical_name
    ) AS payee_name_as_of,
    camp_cm_xw.canonical_entity_id AS campaign_committee_canonical_id,
    cc.id AS canonical_campaign_id,
    cc.canonical_name AS campaign_canonical_name,
    cc.candidate_entity_id AS campaign_candidate_canonical_id
FROM unified_transactions ut
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE lower(CAST(role AS VARCHAR)) = 'contributor'
    GROUP BY transaction_id
) AS contributor_role ON contributor_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk contributor_xw
    ON contributor_xw.source_type = 'unified_entity'
    AND contributor_xw.source_id = CAST(contributor_role.entity_id AS TEXT)
    AND contributor_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
LEFT JOIN canonical_entity contributor_ce
    ON contributor_ce.id = contributor_xw.canonical_entity_id
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE lower(CAST(role AS VARCHAR)) = 'recipient'
    GROUP BY transaction_id
) AS recipient_role ON recipient_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk recipient_xw
    ON recipient_xw.source_type = 'unified_entity'
    AND recipient_xw.source_id = CAST(recipient_role.entity_id AS TEXT)
    AND recipient_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
LEFT JOIN canonical_entity recipient_ce
    ON recipient_ce.id = recipient_xw.canonical_entity_id
LEFT JOIN (
    SELECT transaction_id, MIN(entity_id) AS entity_id
    FROM unified_transaction_persons
    WHERE lower(CAST(role AS VARCHAR)) = 'payee'
    GROUP BY transaction_id
) AS payee_role ON payee_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk payee_xw
    ON payee_xw.source_type = 'unified_entity'
    AND payee_xw.source_id = CAST(payee_role.entity_id AS TEXT)
    AND payee_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
LEFT JOIN canonical_entity payee_ce
    ON payee_ce.id = payee_xw.canonical_entity_id
LEFT JOIN unified_campaigns ucamp
    ON ucamp.id = ut.campaign_id
LEFT JOIN entity_crosswalk camp_cm_xw
    ON camp_cm_xw.source_type = 'unified_committee'
    AND camp_cm_xw.source_id = ucamp.primary_committee_id
    AND camp_cm_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
LEFT JOIN canonical_campaign cc
    ON cc.committee_entity_id = camp_cm_xw.canonical_entity_id
    AND cc.election_cycle = ucamp.election_year
    AND COALESCE(cc.office_normalized, '') =
        COALESCE(NULLIF(lower(trim(ucamp.office_sought)), ''), '')
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
    AND contributor_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
LEFT JOIN canonical_entity contributor_ce
    ON contributor_ce.id = contributor_xw.canonical_entity_id
LEFT JOIN entity_crosswalk recipient_xw
    ON recipient_xw.source_type = 'unified_entity'
    AND recipient_xw.source_id = CAST(uc.recipient_entity_id AS TEXT)
    AND recipient_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
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
    WHERE lower(CAST(role AS VARCHAR)) = 'payee'
    GROUP BY transaction_id
) AS payee_role ON payee_role.transaction_id = ut.id
LEFT JOIN entity_crosswalk payee_xw
    ON payee_xw.source_type = 'unified_entity'
    AND payee_xw.source_id = CAST(payee_role.entity_id AS TEXT)
    AND payee_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
LEFT JOIN canonical_entity payee_ce
    ON payee_ce.id = payee_xw.canonical_entity_id
WHERE lower(CAST(ut.transaction_type AS VARCHAR)) = 'expenditure'
"""

# Report -> committee canonical entity + canonical campaign.  The campaign cycle
# is the year of the report's period_end; extracting it via substr on the
# date-as-text keeps the SQL portable across PostgreSQL and SQLite.
_RESOLVED_REPORTS_SELECT = """
SELECT
    r.*,
    cm_xw.canonical_entity_id AS committee_canonical_entity_id,
    cc.id AS canonical_campaign_id,
    cc.canonical_name AS campaign_canonical_name,
    cc.candidate_entity_id AS campaign_candidate_canonical_id
FROM unified_reports r
LEFT JOIN entity_crosswalk cm_xw
    ON cm_xw.source_type = 'unified_committee'
    AND cm_xw.source_id = r.committee_id
    AND cm_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
-- A report carries no office, so a committee that ran for >1 office in a cycle
-- would fan the report out across each campaign.  Collapse to one campaign per
-- (committee, cycle) deterministically (lowest id) to keep the view 1:1.
LEFT JOIN (
    SELECT committee_entity_id, election_cycle, MIN(id) AS id
    FROM canonical_campaign
    GROUP BY committee_entity_id, election_cycle
) cc_pick
    ON cc_pick.committee_entity_id = cm_xw.canonical_entity_id
    AND cc_pick.election_cycle = CAST(substr(CAST(r.period_end AS VARCHAR), 1, 4) AS INTEGER)
LEFT JOIN canonical_campaign cc ON cc.id = cc_pick.id
"""

_CROSS_ROLE_ENTITIES_SELECT = """
SELECT
    ce.id AS canonical_entity_id,
    ce.canonical_name,
    COALESCE(contrib.contribution_count, 0) AS contribution_count,
    COALESCE(contrib.contribution_total, 0.0) AS contribution_total,
    COALESCE(expend.expenditure_count, 0) AS expenditure_count,
    COALESCE(expend.expenditure_total, 0.0) AS expenditure_total,
    (COALESCE(contrib.contribution_total, 0.0) + COALESCE(expend.expenditure_total, 0.0))
        AS total_activity
FROM canonical_entity ce
INNER JOIN (
    SELECT
        contributor_xw.canonical_entity_id,
        COUNT(*) AS contribution_count,
        COALESCE(SUM(uc.amount), 0.0) AS contribution_total
    FROM unified_contributions uc
    LEFT JOIN entity_crosswalk contributor_xw
        ON contributor_xw.source_type = 'unified_entity'
        AND contributor_xw.source_id = CAST(uc.contributor_entity_id AS TEXT)
        AND contributor_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
    WHERE contributor_xw.canonical_entity_id IS NOT NULL
    GROUP BY contributor_xw.canonical_entity_id
) contrib ON contrib.canonical_entity_id = ce.id
INNER JOIN (
    SELECT
        payee_xw.canonical_entity_id,
        COUNT(*) AS expenditure_count,
        COALESCE(SUM(ut.amount), 0.0) AS expenditure_total
    FROM unified_transactions ut
    LEFT JOIN (
        SELECT transaction_id, MIN(entity_id) AS entity_id
        FROM unified_transaction_persons
        WHERE lower(CAST(role AS VARCHAR)) = 'payee'
        GROUP BY transaction_id
    ) payee_role ON payee_role.transaction_id = ut.id
    LEFT JOIN entity_crosswalk payee_xw
        ON payee_xw.source_type = 'unified_entity'
        AND payee_xw.source_id = CAST(payee_role.entity_id AS TEXT)
        AND payee_xw.run_id = (SELECT MAX(run_id) FROM entity_crosswalk)
    WHERE lower(CAST(ut.transaction_type AS VARCHAR)) = 'expenditure'
      AND payee_xw.canonical_entity_id IS NOT NULL
    GROUP BY payee_xw.canonical_entity_id
) expend ON expend.canonical_entity_id = ce.id
ORDER BY total_activity DESC
"""

_VIEW_SELECTS: tuple[tuple[str, str], ...] = (
    ("resolved_transactions", _RESOLVED_TRANSACTIONS_SELECT),
    ("resolved_contributions", _RESOLVED_CONTRIBUTIONS_SELECT),
    ("resolved_expenditures", _RESOLVED_EXPENDITURES_SELECT),
    ("resolved_reports", _RESOLVED_REPORTS_SELECT),
    ("cross_role_entities", _CROSS_ROLE_ENTITIES_SELECT),
)

_DROP_SQLITE_VIEW_SQL = {
    "resolved_transactions": "DROP VIEW IF EXISTS resolved_transactions",
    "resolved_contributions": "DROP VIEW IF EXISTS resolved_contributions",
    "resolved_expenditures": "DROP VIEW IF EXISTS resolved_expenditures",
    "resolved_reports": "DROP VIEW IF EXISTS resolved_reports",
    "cross_role_entities": "DROP VIEW IF EXISTS cross_role_entities",
}

_CREATE_SQLITE_VIEW_SQL = {
    "resolved_transactions": f"CREATE VIEW resolved_transactions AS {_RESOLVED_TRANSACTIONS_SELECT}",
    "resolved_contributions": f"CREATE VIEW resolved_contributions AS {_RESOLVED_CONTRIBUTIONS_SELECT}",
    "resolved_expenditures": f"CREATE VIEW resolved_expenditures AS {_RESOLVED_EXPENDITURES_SELECT}",
    "resolved_reports": f"CREATE VIEW resolved_reports AS {_RESOLVED_REPORTS_SELECT}",
    "cross_role_entities": f"CREATE VIEW cross_role_entities AS {_CROSS_ROLE_ENTITIES_SELECT}",
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
    "resolved_reports": (
        f"CREATE OR REPLACE VIEW resolved_reports AS {_RESOLVED_REPORTS_SELECT}"
    ),
    "cross_role_entities": (
        f"CREATE OR REPLACE VIEW cross_role_entities AS {_CROSS_ROLE_ENTITIES_SELECT}"
    ),
}

_DROP_POSTGRES_MAT_VIEW_SQL = {
    "resolved_transactions": "DROP MATERIALIZED VIEW IF EXISTS resolved_transactions",
    "resolved_contributions": "DROP MATERIALIZED VIEW IF EXISTS resolved_contributions",
    "resolved_expenditures": "DROP MATERIALIZED VIEW IF EXISTS resolved_expenditures",
    "resolved_reports": "DROP MATERIALIZED VIEW IF EXISTS resolved_reports",
    "cross_role_entities": "DROP MATERIALIZED VIEW IF EXISTS cross_role_entities",
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
    "resolved_reports": (
        f"CREATE MATERIALIZED VIEW resolved_reports AS {_RESOLVED_REPORTS_SELECT}"
    ),
    "cross_role_entities": (
        f"CREATE MATERIALIZED VIEW cross_role_entities AS {_CROSS_ROLE_ENTITIES_SELECT}"
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
