"""Source model modules for resolution pipeline Phase 0.

This package registers all new source-layer SQLModel tables in the shared
``SQLModel.metadata`` and exposes the ``RECORD_TYPE_BUILDERS`` registry so
the production loader can route raw records to the correct ingest builder
without a chain of ``if record_type == ...`` statements.

Usage
-----
::

    from app.core.source_models import RECORD_TYPE_BUILDERS

    builder = RECORD_TYPE_BUILDERS.get(record_type)
    if builder:
        obj = builder(raw, state_id=state_id)
        session.add(obj)
"""

from __future__ import annotations

from app.core.source_models.lookups import CommitteePurpose, ExpenditureCategory
from app.core.source_models.lookups_ingest import (
    build_committee_purpose,
    build_expenditure_category,
)
from app.core.source_models.notices import UnifiedNotice
from app.core.source_models.notices_ingest import build_notice
from app.core.source_models.pledges import UnifiedPledge
from app.core.source_models.pledges_ingest import build_pledge
from app.core.source_models.reports import UnifiedReport
from app.core.source_models.reports_ingest import build_report, link_transactions_to_reports
from app.core.source_models.spac import SpacLink
from app.core.source_models.spac_ingest import build_spac_link

# ---------------------------------------------------------------------------
# Record-type builder registry
#
# Maps a TEC ``recordType`` string → a callable with signature
#   ``(raw: dict, *, state_id: int) -> SQLModel instance``
#
# Note: PLDG (pledge) rows are *not* in this registry because
# ``build_pledge`` also requires pre-resolved entity/transaction objects.
# The loader handles PLDG as a special case after inserting the transaction.
# ---------------------------------------------------------------------------
RECORD_TYPE_BUILDERS: dict[str, object] = {
    "CVR1": build_report,
    "CVR2": build_notice,
    "CVR3": build_committee_purpose,
    "EXCAT": build_expenditure_category,
    "SPAC": build_spac_link,
}

__all__ = [
    "CommitteePurpose",
    "ExpenditureCategory",
    "UnifiedNotice",
    "UnifiedPledge",
    "UnifiedReport",
    "SpacLink",
    "build_committee_purpose",
    "build_expenditure_category",
    "build_notice",
    "build_pledge",
    "build_report",
    "build_spac_link",
    "link_transactions_to_reports",
    "RECORD_TYPE_BUILDERS",
]
