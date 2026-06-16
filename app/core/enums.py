"""Campaign finance domain enumerations."""

from enum import Enum


class TransactionType(str, Enum):
    """Types of campaign finance transactions"""

    CONTRIBUTION = "contribution"
    EXPENDITURE = "expenditure"
    LOAN = "loan"
    PLEDGE = "pledge"
    DEBT = "debt"
    CREDIT = "credit"
    TRAVEL = "travel"
    ASSET = "asset"
    REFUND = "refund"
    TRANSFER = "transfer"
    OTHER = "other"


class PersonType(str, Enum):
    """Types of persons in campaign finance data"""

    INDIVIDUAL = "individual"
    ORGANIZATION = "organization"
    COMMITTEE = "committee"
    CANDIDATE = "candidate"
    UNKNOWN = "unknown"


class PersonRole(str, Enum):
    """Roles of persons in transactions"""

    CONTRIBUTOR = "contributor"
    RECIPIENT = "recipient"
    PAYEE = "payee"
    CANDIDATE = "candidate"
    TREASURER = "treasurer"
    CHAIR = "chair"


class CommitteeRole(str, Enum):
    """Roles that people can have within committees"""

    TREASURER = "treasurer"
    ASSISTANT_TREASURER = "assistant_treasurer"
    CHAIR = "chair"
    VICE_CHAIR = "vice_chair"
    SECRETARY = "secretary"
    ASSISTANT_SECRETARY = "assistant_secretary"
    CANDIDATE = "candidate"
    DEPUTY_TREASURER = "deputy_treasurer"
    OTHER = "other"


class EntityType(str, Enum):
    """Types of unified entities used for deduplication"""

    PERSON = "person"
    ORGANIZATION = "organization"
    COMMITTEE = "committee"
    CAMPAIGN = "campaign"
    VENDOR = "vendor"
    OTHER = "other"


class AssociationType(str, Enum):
    """Association types between unified entities"""

    TREASURER_OF = "treasurer_of"
    DONOR_TO = "donor_to"
    VENDOR_FOR = "vendor_for"
    OFFICER_OF = "officer_of"
    AFFILIATED_WITH = "affiliated_with"
    EMPLOYED_BY = "employed_by"
    CO_LOCATED_WITH = "co_located_with"
    OTHER = "other"


class CampaignRole(str, Enum):
    """Roles that entities can have within a campaign context"""

    CANDIDATE = "candidate"
    TREASURER = "treasurer"
    CHAIR = "chair"
    DONOR = "donor"
    VENDOR = "vendor"
    CONSULTANT = "consultant"
    STAFF = "staff"
    SUPPORTER = "supporter"
    COMMITTEE = "committee"
    OTHER = "other"


