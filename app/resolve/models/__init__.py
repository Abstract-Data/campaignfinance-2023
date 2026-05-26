"""Resolution-layer SQLModel schemas."""

# UnifiedTransaction (unified_sqlmodels) references UnifiedReport via Relationship.
# SQLAlchemy configures all mappers globally on first ORM use; importing reports
# here ensures the class is registered before any resolution code opens a Session.
import app.core.source_models.reports  # noqa: F401
from app.resolve.models.canonical import (
    CanonicalAddress,
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalNameHistory,
    EntityType,
    UnmappedEntityTypeError,
    map_unified_to_canonical_entity_type,
)
from app.resolve.models.resolution import (
    SOURCE_ID_MAX_LENGTH,
    AddressCrosswalk,
    CampaignCrosswalk,
    EntityCrosswalk,
    MatchDecision,
    MatchRun,
    MergeReview,
)

__all__ = [
    "SOURCE_ID_MAX_LENGTH",
    "AddressCrosswalk",
    "CampaignCrosswalk",
    "CanonicalAddress",
    "CanonicalCampaign",
    "CanonicalEntity",
    "CanonicalNameHistory",
    "EntityCrosswalk",
    "EntityType",
    "MatchDecision",
    "MatchRun",
    "MergeReview",
    "UnmappedEntityTypeError",
    "map_unified_to_canonical_entity_type",
]
