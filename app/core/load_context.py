"""Pipeline state for a single UnifiedStateLoader.load_state_data() run."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.core.models import UnifiedAddress, UnifiedCommittee, UnifiedPerson

AddressCacheKey = tuple[str, str, str, str]
PersonCacheKey = str


@dataclass
class LoadStats:
    files_processed: int = 0
    transactions_created: int = 0
    transactions_failed: int = 0
    persons_created: int = 0
    committees_created: int = 0
    addresses_created: int = 0
    committee_relationships_created: int = 0
    transaction_links_created: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total_attempted(self) -> int:
        return self.transactions_created + self.transactions_failed

    def as_dict(self) -> dict[str, Any]:
        return {
            "files_processed": self.files_processed,
            "transactions_created": self.transactions_created,
            "persons_created": self.persons_created,
            "committees_created": self.committees_created,
            "addresses_created": self.addresses_created,
            "committee_relationships_created": self.committee_relationships_created,
            "transaction_links_created": self.transaction_links_created,
            "errors": list(self.errors),
        }


@dataclass
class LoadContext:
    """All mutable state for one load_state_data() invocation."""

    state: str
    state_id: int | None = None
    state_code: str | None = None

    person_cache: dict[PersonCacheKey, UnifiedPerson] = field(default_factory=dict)
    committee_cache: dict[str, UnifiedCommittee] = field(default_factory=dict)
    address_cache: dict[AddressCacheKey, int] = field(default_factory=dict)
    committee_officers: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    data_files: list[Path] = field(default_factory=list)
    stats: LoadStats = field(default_factory=LoadStats)
