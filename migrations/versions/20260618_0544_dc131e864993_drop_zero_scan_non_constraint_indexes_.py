"""drop zero-scan non-constraint indexes (Wave 5a)

Revision ID: dc131e864993
Revises: 01bfbd7b124f
Create Date: 2026-06-18 05:44:07.216145+00:00

Wave 5a — Index Diet

Drops 16 indexes with idx_scan = 0 (per docs/db-bloat-baseline-2026-06-17.md)
that are NOT:
  - backing a unique/dedup constraint (uix_*, uq_*, *_key)
  - primary key indexes
  - foreign key support indexes
  - the 7 partial-unique dedup indexes

12 are exact column-level duplicates where an ix_unified_* version already
exists on the same column; 4 are standalone analytical indexes on non-FK columns
with no production use signal.

Preserved (never dropped):
  - All uix_* dedup indexes (7 partial-unique + others)
  - All *_pkey, *_key, uq_* constraint indexes
  - All ix_*_run_id, ix_*_entity_id, ix_*_canonical_*, ix_*_committee_id,
    ix_*_state_id, ix_*_file_origin_id, ix_*_transaction_id indexes (FK-backing)
  - ix_resolution_input_run_* blocking indexes (resolve pipeline)
  - All indexes with idx_scan > 0
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "dc131e864993"
down_revision: str | None = "01bfbd7b124f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Indexes being dropped — grouped by table for readability
# ---------------------------------------------------------------------------
#
# unified_assets (3 drops):
#   idx_assets_acquisition_date  — duplicate of ix_unified_assets_acquisition_date
#   idx_assets_is_disposed       — duplicate of ix_unified_assets_is_disposed
#   idx_assets_type              — analytical, no FK, no ix_ duplicate
#
# unified_campaign_entities (1 drop):
#   idx_campaign_entity_role     — duplicate of ix_unified_campaign_entities_role
#
# unified_contributions (2 drops):
#   idx_contributions_date       — duplicate of ix_unified_contributions_receipt_date
#   idx_contributions_amount     — analytical, no FK, no ix_ duplicate
#
# unified_credits (1 drop):
#   idx_credits_date             — duplicate of ix_unified_credits_credit_date
#
# unified_debts (2 drops):
#   idx_debts_date               — duplicate of ix_unified_debts_debt_date
#   idx_debts_due_date           — duplicate of ix_unified_debts_due_date
#
# unified_loans (2 drops):
#   idx_loans_date               — duplicate of ix_unified_loans_loan_date
#   idx_loans_due_date           — duplicate of ix_unified_loans_due_date
#
# unified_transactions (2 drops):
#   idx_transactions_date        — duplicate of ix_unified_transactions_transaction_date
#   idx_transactions_amount      — analytical, no FK, no ix_ duplicate
#
# unified_travel (3 drops):
#   idx_travel_date              — duplicate of ix_unified_travel_travel_date
#   idx_travel_departure         — duplicate of ix_unified_travel_departure_date
#   idx_travel_arrival_city      — analytical, no FK, no ix_ duplicate
# ---------------------------------------------------------------------------


def upgrade() -> None:
    # unified_assets
    op.drop_index("idx_assets_acquisition_date", table_name="unified_assets", if_exists=True)
    op.drop_index("idx_assets_is_disposed", table_name="unified_assets", if_exists=True)
    op.drop_index("idx_assets_type", table_name="unified_assets", if_exists=True)

    # unified_campaign_entities
    op.drop_index(
        "idx_campaign_entity_role", table_name="unified_campaign_entities", if_exists=True
    )

    # unified_contributions
    op.drop_index("idx_contributions_date", table_name="unified_contributions", if_exists=True)
    op.drop_index("idx_contributions_amount", table_name="unified_contributions", if_exists=True)

    # unified_credits
    op.drop_index("idx_credits_date", table_name="unified_credits", if_exists=True)

    # unified_debts
    op.drop_index("idx_debts_date", table_name="unified_debts", if_exists=True)
    op.drop_index("idx_debts_due_date", table_name="unified_debts", if_exists=True)

    # unified_loans
    op.drop_index("idx_loans_date", table_name="unified_loans", if_exists=True)
    op.drop_index("idx_loans_due_date", table_name="unified_loans", if_exists=True)

    # unified_transactions
    op.drop_index("idx_transactions_date", table_name="unified_transactions", if_exists=True)
    op.drop_index("idx_transactions_amount", table_name="unified_transactions", if_exists=True)

    # unified_travel
    op.drop_index("idx_travel_date", table_name="unified_travel", if_exists=True)
    op.drop_index("idx_travel_departure", table_name="unified_travel", if_exists=True)
    op.drop_index("idx_travel_arrival_city", table_name="unified_travel", if_exists=True)


def downgrade() -> None:
    # unified_travel
    op.create_index("idx_travel_arrival_city", "unified_travel", ["arrival_city"])
    op.create_index("idx_travel_departure", "unified_travel", ["departure_date"])
    op.create_index("idx_travel_date", "unified_travel", ["travel_date"])

    # unified_transactions
    op.create_index("idx_transactions_amount", "unified_transactions", ["amount"])
    op.create_index("idx_transactions_date", "unified_transactions", ["transaction_date"])

    # unified_loans
    op.create_index("idx_loans_due_date", "unified_loans", ["due_date"])
    op.create_index("idx_loans_date", "unified_loans", ["loan_date"])

    # unified_debts
    op.create_index("idx_debts_due_date", "unified_debts", ["due_date"])
    op.create_index("idx_debts_date", "unified_debts", ["debt_date"])

    # unified_credits
    op.create_index("idx_credits_date", "unified_credits", ["credit_date"])

    # unified_contributions
    op.create_index("idx_contributions_amount", "unified_contributions", ["amount"])
    op.create_index("idx_contributions_date", "unified_contributions", ["receipt_date"])

    # unified_campaign_entities
    op.create_index("idx_campaign_entity_role", "unified_campaign_entities", ["role"])

    # unified_assets
    op.create_index("idx_assets_type", "unified_assets", ["asset_type"])
    op.create_index("idx_assets_is_disposed", "unified_assets", ["is_disposed"])
    op.create_index("idx_assets_acquisition_date", "unified_assets", ["acquisition_date"])
