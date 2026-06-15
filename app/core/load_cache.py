"""In-memory dedup cache shared across a single load run.

The per-row builder previously issued ~4-8 synchronous ``SELECT``s per record
(`_find_person_by_name_state`, `_find_committee_by_filer_id`, `_find_entity`,
`_find_address_by_fields`, `_find_campaign`).  Across hundreds of thousands of
rows that is millions of round-trips and the dominant cost of a full load.

``BuilderCache`` turns repeated lookups of the same person / committee / entity
/ address / campaign into dict hits.  Keys are computed to mirror the database
partial-unique indexes (see ``UnifiedDatabaseManager._DEDUP_INDEXES``) so a cache
hit and a DB row stay consistent.

Modes
-----
``authoritative=False`` (default, used for incremental loads and the
truncate+reload path): **read-through**.  On a cache miss the builder still
queries the DB and caches the result.  This is safe even when another code path
(e.g. ``filer_ingest``) writes the same person/committee/address rows — those are
discovered via the DB query and then cached.

``authoritative=True``: skip the DB entirely on a cache miss.  Only safe when
this run is the *sole* writer of every cached table (no ``filer_ingest`` officer
persons/addresses, no concurrent writers).  Left opt-in; not used by the default
loader path.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# Key shapes (all lower-cased / normalized at call sites):
#   person    -> ("org", org, state_id) | ("name", first, last, state_id, addr_key)
#   committee -> filer_id
#   entity    -> (entity_type, normalized_name, state_id)
#   address   -> (street_1|None, city|None, state|None, zip|None)
#   campaign  -> (normalized_name, committee_filer_id|None, candidate_id|None, year|None)
PersonKey = tuple
EntityKey = tuple
AddressKey = tuple
CampaignKey = tuple


@dataclass
class BuilderCache:
    """Shared dedup maps for one load run.  Holds live ORM object references."""

    authoritative: bool = False
    committees: dict[str, Any] = field(default_factory=dict)
    persons: dict[PersonKey, Any] = field(default_factory=dict)
    entities: dict[EntityKey, Any] = field(default_factory=dict)
    addresses: dict[AddressKey, Any] = field(default_factory=dict)
    campaigns: dict[CampaignKey, Any] = field(default_factory=dict)

    # Per-run memoized builder (set by the processor).  Tying builder reuse to the
    # cache's lifetime — rather than the module-singleton processor — avoids
    # leaking a builder bound to a disposed engine across unrelated callers.
    builder: Any = field(default=None, compare=False, repr=False)
    builder_key: Any = field(default=None, compare=False, repr=False)

    # ── key builders ────────────────────────────────────────────────────────
    @staticmethod
    def person_key(
        first_name: str | None,
        last_name: str | None,
        organization: str | None,
        state_id: int | None,
        address_key: str | None = None,
    ) -> PersonKey | None:
        """Dedup key mirroring ``uix_persons_*``.

        Org-persons key on ``(lower(org), state)`` alone (``address_key`` ignored).
        Individuals key on ``(lower(first), lower(last), state, address_key)`` so two
        same-name people at distinct locations stay separate.  ``address_key`` is the
        denormalized ``dedup_addr_key`` string (see ``address_key_str``) or ``None``
        when the address has too few fields — then the key degrades to name-only,
        preserving today's behavior for addressless persons.
        """
        if organization:
            return ("org", organization.lower(), state_id)
        if first_name and last_name:
            return ("name", first_name.lower(), last_name.lower(), state_id, address_key)
        return None

    @staticmethod
    def address_key_str(address_data: dict[str, Any]) -> str | None:
        """Denormalized ``dedup_addr_key`` — the ``address_key`` tuple joined by ``|``.

        Returns ``"street|city|state|zip"`` (lowercased street/city/state, zip as-is,
        empty for missing components) when ≥2 of the four fields are populated, else
        ``None``.  Stored on ``UnifiedPerson.dedup_addr_key`` so a single-table unique
        index can enforce the (name + address) dedup key.
        """
        key = BuilderCache.address_key(address_data)
        if key is None:
            return None
        return "|".join("" if part is None else str(part) for part in key)

    @staticmethod
    def entity_key(
        entity_type: Any, normalized_name: str | None, state_id: int | None
    ) -> EntityKey | None:
        if not normalized_name:
            return None
        return (entity_type, normalized_name, state_id)

    @staticmethod
    def address_key(address_data: dict[str, Any]) -> AddressKey | None:
        street_1 = address_data.get("street_1")
        city = address_data.get("city")
        state = address_data.get("state")
        zip_code = address_data.get("zip_code")
        populated = sum(1 for v in (street_1, city, state, zip_code) if v is not None)
        if populated < 2:
            return None
        return (
            street_1.lower() if street_1 else None,
            city.lower() if city else None,
            state.lower() if state else None,
            zip_code,
        )

    @staticmethod
    def campaign_key(
        normalized_name: str | None,
        committee_filer_id: str | None,
        candidate_id: int | None,
        election_year: int | None,
    ) -> CampaignKey | None:
        if not normalized_name:
            return None
        return (normalized_name, committee_filer_id, candidate_id, election_year)
