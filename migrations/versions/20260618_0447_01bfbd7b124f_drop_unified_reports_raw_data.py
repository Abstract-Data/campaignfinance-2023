"""drop unified_reports.raw_data

Revision ID: 01bfbd7b124f
Revises: 1e9900f11701
Create Date: 2026-06-18 04:47:00.000000+00:00

DB Bloat Remediation Wave 2 (tasks 2a + 2b):

upgrade():
  1. Drop resolved_reports view (references raw_data).
  2. Backfill committee_name_at_filing and treasurer_name_at_filing for any
     legacy rows that have raw_data but NULL at-filing columns.
  3. Stop writing raw_data (done in code: both report writers updated).
  4. DROP unified_reports.raw_data.
  5. Recreate resolved_reports view without raw_data.

downgrade() is LOSSY: re-adds raw_data as NULL-filled nullable Text.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "01bfbd7b124f"
down_revision: str | None = "1e9900f11701"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_CREATE_RESOLVED_REPORTS = """
CREATE VIEW resolved_reports AS
 SELECT r.id,
    r.uuid,
    r.state_id,
    r.committee_id,
    r.committee_name_at_filing,
    r.treasurer_name_at_filing,
    r.report_ident,
    r.form_type,
    r.filed_date,
    r.period_start,
    r.period_end,
    r.is_final,
    r.total_contributions,
    r.total_unitemized_contributions,
    r.total_expenditures,
    r.total_unitemized_expenditures,
    r.loan_balance,
    r.contributions_maintained,
    r.cash_on_hand,
    r.file_origin_id,
    r.created_at,
    r.updated_at,
    cm_xw.canonical_entity_id AS committee_canonical_entity_id,
    cc.id AS canonical_campaign_id,
    cc.canonical_name AS campaign_canonical_name,
    cc.candidate_entity_id AS campaign_candidate_canonical_id
   FROM (((unified_reports r
     LEFT JOIN entity_crosswalk cm_xw ON (((cm_xw.source_type = 'unified_committee'::sourcetype)
       AND ((cm_xw.source_id)::text = (r.committee_id)::text)
       AND (cm_xw.run_id = ( SELECT max(entity_crosswalk.run_id) AS max FROM entity_crosswalk)))))
     LEFT JOIN ( SELECT canonical_campaign.committee_entity_id,
            canonical_campaign.election_cycle,
            min(canonical_campaign.id) AS id
           FROM canonical_campaign
          GROUP BY canonical_campaign.committee_entity_id, canonical_campaign.election_cycle
        ) cc_pick ON (((cc_pick.committee_entity_id = cm_xw.canonical_entity_id)
          AND (cc_pick.election_cycle = (substr(((r.period_end)::character varying)::text, 1, 4))::integer))))
     LEFT JOIN canonical_campaign cc ON ((cc.id = cc_pick.id)));
"""

_CREATE_RESOLVED_REPORTS_WITH_RAW = _CREATE_RESOLVED_REPORTS.replace(
    "    r.file_origin_id,\n    r.created_at,",
    "    r.file_origin_id,\n    r.raw_data,\n    r.created_at,",
)


def _column_exists(conn: sa.engine.Connection, table: str, column: str) -> bool:
    result = conn.execute(sa.text(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_name = :t AND column_name = :c"
    ), {"t": table, "c": column})
    return result.fetchone() is not None


def upgrade() -> None:
    conn = op.get_bind()

    # Drop dependent view
    conn.execute(sa.text("DROP VIEW IF EXISTS resolved_reports"))

    # Backfill at-filing columns from raw_data for any legacy rows with NULLs.
    # Only executed when the column still exists (idempotent on fresh DBs).
    if conn.dialect.name == "postgresql" and _column_exists(conn, "unified_reports", "raw_data"):
        conn.execute(sa.text(
            """
            UPDATE unified_reports
            SET committee_name_at_filing = (raw_data::jsonb ->> 'filerName')
            WHERE raw_data IS NOT NULL
              AND committee_name_at_filing IS NULL
            """
        ))
        conn.execute(sa.text(
            """
            UPDATE unified_reports
            SET treasurer_name_at_filing = TRIM(
                COALESCE(raw_data::jsonb ->> 'treasNameFirst', '') || ' ' ||
                COALESCE(raw_data::jsonb ->> 'treasNameLast', '')
            )
            WHERE raw_data IS NOT NULL
              AND treasurer_name_at_filing IS NULL
            """
        ))

    # Drop the column (idempotent — fresh DBs may not have raw_data)
    conn.execute(sa.text(
        "ALTER TABLE unified_reports DROP COLUMN IF EXISTS raw_data"
    ))

    # Recreate view without raw_data
    conn.execute(sa.text(_CREATE_RESOLVED_REPORTS))


def downgrade() -> None:
    conn = op.get_bind()

    conn.execute(sa.text("DROP VIEW IF EXISTS resolved_reports"))

    # WARNING: LOSSY — re-adds raw_data as NULL-filled nullable Text.
    conn.execute(sa.text(
        "ALTER TABLE unified_reports ADD COLUMN IF NOT EXISTS raw_data TEXT"
    ))

    conn.execute(sa.text(_CREATE_RESOLVED_REPORTS_WITH_RAW))
