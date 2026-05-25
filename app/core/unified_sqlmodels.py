"""Backward-compatible re-exports for unified SQLModel symbols (TASK-3a shim)."""

from app.core.builders import UnifiedSQLModelBuilder
from app.core.constants import (
    AMOUNT_BUCKETS,
    DEFAULT_STATE,
    MONEY_TYPE,
    PLACEHOLDER_NAMES,
    RECORD_TYPE_CODES,
    RECORD_TYPE_TO_TRANSACTION,
)
from app.core.enums import (
    AssociationType,
    CampaignRole,
    CommitteeRole,
    EntityType,
    PersonRole,
    PersonType,
    TransactionType,
)
from app.core.models import *
from app.core.models.tables import UnifiedTransactionIndexes
from app.core.processor import UnifiedSQLDataProcessor, unified_sql_processor

__all__ = [
    "AMOUNT_BUCKETS",
    "AssociationType",
    "CampaignRole",
    "CommitteeRole",
    "DEFAULT_STATE",
    "EntityType",
    "MONEY_TYPE",
    "PLACEHOLDER_NAMES",
    "PersonRole",
    "PersonType",
    "RECORD_TYPE_CODES",
    "RECORD_TYPE_TO_TRANSACTION",
    "TransactionType",
    "UnifiedSQLDataProcessor",
    "UnifiedSQLModelBuilder",
    "UnifiedTransactionIndexes",
    "unified_sql_processor",
]
