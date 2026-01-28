"""
Campaign Finance Core Module

This module contains unified models, processors, and utilities for
cross-state campaign finance data processing.
"""

from .unified_field_library import (
    field_library,
    FieldCategory,
    FieldType,
    FieldDefinition,
    StateFieldMapping,
    UnifiedFieldLibrary,
)

from .unified_models import (
    unified_processor,
    UnifiedTransaction as BaseUnifiedTransaction,
    TransactionType as BaseTransactionType,
    PersonType as BasePersonType,
    UnifiedPerson as BaseUnifiedPerson,
    UnifiedAddress as BaseUnifiedAddress,
    UnifiedDataProcessor,
)

from .unified_sqlmodels import (
    unified_sql_processor,
    UnifiedTransaction,
    UnifiedPerson,
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedTransactionPerson,
    UnifiedCommitteePerson,
    UnifiedTransactionVersion,
    UnifiedPersonVersion,
    UnifiedAddressVersion,
    UnifiedCommitteeVersion,
    UnifiedCommitteePersonVersion,
    TransactionType,
    PersonType,
    PersonRole,
    CommitteeRole,
    State,
)

from .unified_database import (
    db_manager,
    UnifiedDatabaseManager,
)

from .unified_state_loader import (
    UnifiedStateLoader,
)

from .unified_integration import (
    UnifiedStateProcessor,
)

__all__ = [
    # Field Library
    "field_library",
    "FieldCategory",
    "FieldType",
    "FieldDefinition",
    "StateFieldMapping",
    "UnifiedFieldLibrary",
    # Base Models (dataclass-based)
    "unified_processor",
    "BaseUnifiedTransaction",
    "BaseTransactionType",
    "BasePersonType",
    "BaseUnifiedPerson",
    "BaseUnifiedAddress",
    "UnifiedDataProcessor",
    # SQLModels
    "unified_sql_processor",
    "UnifiedTransaction",
    "UnifiedPerson",
    "UnifiedAddress",
    "UnifiedCommittee",
    "UnifiedTransactionPerson",
    "UnifiedCommitteePerson",
    "UnifiedTransactionVersion",
    "UnifiedPersonVersion",
    "UnifiedAddressVersion",
    "UnifiedCommitteeVersion",
    "UnifiedCommitteePersonVersion",
    "TransactionType",
    "PersonType",
    "PersonRole",
    "CommitteeRole",
    "State",
    # Database
    "db_manager",
    "UnifiedDatabaseManager",
    # Loaders
    "UnifiedStateLoader",
    "UnifiedStateProcessor",
]
