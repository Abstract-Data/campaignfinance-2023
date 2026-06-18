"""drop unified_transactions.raw_data + add campaign source cols

Revision ID: 1e9900f11701
Revises: 0002_dedup_legacy_transactions
Create Date: 2026-06-18 04:33:21.939402+00:00

DB Bloat Remediation Wave 1 (tasks 1a + 1b):

upgrade():
  1. Drop views that SELECT raw_data (resolved_transactions, resolved_expenditures).
  2. ADD three narrow campaign source columns (Wave 1a). These replace the need to
     read the full raw_data JSON blob in finalize_campaigns().
  3. DROP unified_transactions.raw_data — the only consumer was campaigns.py,
     which has been rewired off it. IngestError.raw_data is intentionally kept.
  4. Recreate the two views without the raw_data column.

downgrade() is LOSSY: re-adds raw_data as a NULL-filled nullable Text column.
The original JSON blob data cannot be reconstructed from the source columns.
This is documented and accepted — source parquet files preserve provenance.
"""
from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "1e9900f11701"
down_revision: str | None = "0002_dedup_legacy_transactions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Views recreated without raw_data.
# Full definitions captured from the DB before the migration.
_CREATE_RESOLVED_TRANSACTIONS = """
CREATE VIEW resolved_transactions AS
 SELECT ut.id,
    ut.uuid,
    ut.transaction_id,
    ut.amount,
    ut.transaction_date,
    ut.description,
    ut.transaction_type,
    ut.committee_id,
    ut.campaign_id,
    ut.state_id,
    ut.file_origin_id,
    ut.report_id,
    ut.report_ident,
    ut.filed_date,
    ut.amended,
    ut.download_date,
    ut.last_modified_at,
    ut.last_modified_by,
    ut.change_reason,
    ut.amendment_details,
    ut.created_at,
    ut.updated_at,
    contributor_role.entity_id AS contributor_source_entity_id,
    contributor_ce.id AS contributor_canonical_entity_id,
    contributor_ce.canonical_name AS contributor_canonical_name,
    COALESCE(( SELECT nh.name
           FROM canonical_name_history nh
          WHERE ((nh.subject_type = 'entity'::namehistorysubjecttype) AND (nh.subject_id = contributor_ce.id)
            AND ((nh.first_seen_date IS NULL) OR (nh.first_seen_date <= ut.transaction_date))
            AND ((nh.last_seen_date IS NULL) OR (nh.last_seen_date >= ut.transaction_date)))
          ORDER BY nh.occurrence_count DESC
         LIMIT 1), contributor_ce.canonical_name) AS contributor_name_as_of,
    recipient_role.entity_id AS recipient_source_entity_id,
    recipient_ce.id AS recipient_canonical_entity_id,
    recipient_ce.canonical_name AS recipient_canonical_name,
    payee_role.entity_id AS payee_source_entity_id,
    payee_ce.id AS payee_canonical_entity_id,
    payee_ce.canonical_name AS payee_canonical_name,
    COALESCE(( SELECT nh.name
           FROM canonical_name_history nh
          WHERE ((nh.subject_type = 'entity'::namehistorysubjecttype) AND (nh.subject_id = payee_ce.id)
            AND ((nh.first_seen_date IS NULL) OR (nh.first_seen_date <= ut.transaction_date))
            AND ((nh.last_seen_date IS NULL) OR (nh.last_seen_date >= ut.transaction_date)))
          ORDER BY nh.occurrence_count DESC
         LIMIT 1), payee_ce.canonical_name) AS payee_name_as_of,
    camp_cm_xw.canonical_entity_id AS campaign_committee_canonical_id,
    cc.id AS canonical_campaign_id,
    cc.canonical_name AS campaign_canonical_name,
    cc.candidate_entity_id AS campaign_candidate_canonical_id
   FROM ((((((((((((unified_transactions ut
     LEFT JOIN ( SELECT unified_transaction_persons.transaction_id,
            min(unified_transaction_persons.entity_id) AS entity_id
           FROM unified_transaction_persons
          WHERE (lower(((unified_transaction_persons.role)::character varying)::text) = 'contributor'::text)
          GROUP BY unified_transaction_persons.transaction_id) contributor_role ON ((contributor_role.transaction_id = ut.id)))
     LEFT JOIN entity_crosswalk contributor_xw ON (((contributor_xw.source_type = 'unified_entity'::sourcetype)
       AND ((contributor_xw.source_id)::text = (contributor_role.entity_id)::text)
       AND (contributor_xw.run_id = ( SELECT max(entity_crosswalk.run_id) AS max FROM entity_crosswalk)))))
     LEFT JOIN canonical_entity contributor_ce ON ((contributor_ce.id = contributor_xw.canonical_entity_id)))
     LEFT JOIN ( SELECT unified_transaction_persons.transaction_id,
            min(unified_transaction_persons.entity_id) AS entity_id
           FROM unified_transaction_persons
          WHERE (lower(((unified_transaction_persons.role)::character varying)::text) = 'recipient'::text)
          GROUP BY unified_transaction_persons.transaction_id) recipient_role ON ((recipient_role.transaction_id = ut.id)))
     LEFT JOIN entity_crosswalk recipient_xw ON (((recipient_xw.source_type = 'unified_entity'::sourcetype)
       AND ((recipient_xw.source_id)::text = (recipient_role.entity_id)::text)
       AND (recipient_xw.run_id = ( SELECT max(entity_crosswalk.run_id) AS max FROM entity_crosswalk)))))
     LEFT JOIN canonical_entity recipient_ce ON ((recipient_ce.id = recipient_xw.canonical_entity_id)))
     LEFT JOIN ( SELECT unified_transaction_persons.transaction_id,
            min(unified_transaction_persons.entity_id) AS entity_id
           FROM unified_transaction_persons
          WHERE (lower(((unified_transaction_persons.role)::character varying)::text) = 'payee'::text)
          GROUP BY unified_transaction_persons.transaction_id) payee_role ON ((payee_role.transaction_id = ut.id)))
     LEFT JOIN entity_crosswalk payee_xw ON (((payee_xw.source_type = 'unified_entity'::sourcetype)
       AND ((payee_xw.source_id)::text = (payee_role.entity_id)::text)
       AND (payee_xw.run_id = ( SELECT max(entity_crosswalk.run_id) AS max FROM entity_crosswalk)))))
     LEFT JOIN canonical_entity payee_ce ON ((payee_ce.id = payee_xw.canonical_entity_id)))
     LEFT JOIN unified_campaigns ucamp ON ((ucamp.id = ut.campaign_id)))
     LEFT JOIN entity_crosswalk camp_cm_xw ON (((camp_cm_xw.source_type = 'unified_committee'::sourcetype)
       AND ((camp_cm_xw.source_id)::text = (ucamp.primary_committee_id)::text)
       AND (camp_cm_xw.run_id = ( SELECT max(entity_crosswalk.run_id) AS max FROM entity_crosswalk)))))
     LEFT JOIN canonical_campaign cc ON (((cc.committee_entity_id = camp_cm_xw.canonical_entity_id)
       AND (cc.election_cycle = ucamp.election_year)
       AND ((COALESCE(cc.office_normalized, ''::character varying))::text
            = COALESCE(NULLIF(lower(TRIM(BOTH FROM ucamp.office_sought)), ''::text), ''::text)))));
"""

_CREATE_RESOLVED_EXPENDITURES = """
CREATE VIEW resolved_expenditures AS
 SELECT ut.id,
    ut.uuid,
    ut.transaction_id,
    ut.amount,
    ut.transaction_date,
    ut.description,
    ut.transaction_type,
    ut.committee_id,
    ut.campaign_id,
    ut.state_id,
    ut.file_origin_id,
    ut.report_id,
    ut.report_ident,
    ut.filed_date,
    ut.amended,
    ut.download_date,
    ut.last_modified_at,
    ut.last_modified_by,
    ut.change_reason,
    ut.amendment_details,
    ut.created_at,
    ut.updated_at,
    payee_role.entity_id AS payee_source_entity_id,
    payee_ce.id AS payee_canonical_entity_id,
    payee_ce.canonical_name AS payee_canonical_name
   FROM (((unified_transactions ut
     LEFT JOIN ( SELECT unified_transaction_persons.transaction_id,
            min(unified_transaction_persons.entity_id) AS entity_id
           FROM unified_transaction_persons
          WHERE (lower(((unified_transaction_persons.role)::character varying)::text) = 'payee'::text)
          GROUP BY unified_transaction_persons.transaction_id) payee_role ON ((payee_role.transaction_id = ut.id)))
     LEFT JOIN entity_crosswalk payee_xw ON (((payee_xw.source_type = 'unified_entity'::sourcetype)
       AND ((payee_xw.source_id)::text = (payee_role.entity_id)::text)
       AND (payee_xw.run_id = ( SELECT max(entity_crosswalk.run_id) AS max FROM entity_crosswalk)))))
     LEFT JOIN canonical_entity payee_ce ON ((payee_ce.id = payee_xw.canonical_entity_id)))
  WHERE (lower(((ut.transaction_type)::character varying)::text) = 'expenditure'::text);
"""

# Views restored in downgrade with raw_data included.
_CREATE_RESOLVED_TRANSACTIONS_WITH_RAW = _CREATE_RESOLVED_TRANSACTIONS.replace(
    "    ut.download_date,\n    ut.last_modified_at,",
    "    ut.download_date,\n    ut.raw_data,\n    ut.last_modified_at,",
)
_CREATE_RESOLVED_EXPENDITURES_WITH_RAW = _CREATE_RESOLVED_EXPENDITURES.replace(
    "    ut.download_date,\n    ut.last_modified_at,",
    "    ut.download_date,\n    ut.raw_data,\n    ut.last_modified_at,",
)


def upgrade() -> None:
    conn = op.get_bind()

    # Drop dependent views before altering the table (idempotent)
    conn.execute(sa.text("DROP VIEW IF EXISTS resolved_transactions"))
    conn.execute(sa.text("DROP VIEW IF EXISTS resolved_expenditures"))

    # Add the three narrow campaign source columns (idempotent — 0001_baseline
    # creates_all from current SQLModel which may already include these cols).
    conn.execute(sa.text(
        "ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS "
        "campaign_office_src VARCHAR(200)"
    ))
    conn.execute(sa.text(
        "ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS "
        "campaign_district_src VARCHAR(200)"
    ))
    conn.execute(sa.text(
        "ALTER TABLE unified_transactions ADD COLUMN IF NOT EXISTS "
        "campaign_name_src VARCHAR(200)"
    ))

    # Drop the raw_data column (idempotent — may already be absent on fresh DBs
    # where 0001_baseline used the current SQLModel schema without raw_data).
    conn.execute(sa.text(
        "ALTER TABLE unified_transactions DROP COLUMN IF EXISTS raw_data"
    ))

    # Recreate views without raw_data (idempotent DROP IF EXISTS above handles both cases)
    conn.execute(sa.text(_CREATE_RESOLVED_TRANSACTIONS))
    conn.execute(sa.text(_CREATE_RESOLVED_EXPENDITURES))


def downgrade() -> None:
    conn = op.get_bind()

    # Drop views that reference campaign source columns (they don't, but safer)
    conn.execute(sa.text("DROP VIEW IF EXISTS resolved_transactions"))
    conn.execute(sa.text("DROP VIEW IF EXISTS resolved_expenditures"))

    # WARNING: LOSSY downgrade — re-adds raw_data as a NULL-filled nullable Text column.
    # The original JSON blob data is permanently lost; source parquet files preserve provenance.
    op.add_column(
        "unified_transactions",
        sa.Column("raw_data", sa.Text(), nullable=True),
    )

    # Drop campaign source columns
    op.drop_column("unified_transactions", "campaign_name_src")
    op.drop_column("unified_transactions", "campaign_district_src")
    op.drop_column("unified_transactions", "campaign_office_src")

    # Recreate views with raw_data restored
    conn.execute(sa.text(_CREATE_RESOLVED_TRANSACTIONS_WITH_RAW))
    conn.execute(sa.text(_CREATE_RESOLVED_EXPENDITURES_WITH_RAW))
