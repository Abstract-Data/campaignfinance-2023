"""Build unified SQLModel rows from state-specific records."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import selectinload
from sqlalchemy.sql.expression import func as sa_func
from sqlmodel import Session, select

from app.core.constants import PLACEHOLDER_NAMES, RECORD_TYPE_TO_TRANSACTION
from app.core.enums import CampaignRole, EntityType, PersonRole, PersonType, TransactionType
from app.core.load_cache import BuilderCache
from app.core.models import (
    UnifiedAddress,
    UnifiedCampaign,
    UnifiedCampaignEntity,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
    UnifiedTransaction,
)
from app.core.unified_field_library import field_library
from app.core.value_objects import AddressParts, PersonName, normalize_entity_name
from app.logger import Logger


class UnifiedSQLModelBuilder:
    """
    Builder class that creates SQLModel instances from state-specific data
    by automatically mapping fields using the field library.

    Notes
    -----
    RF-SMELL-005 — the builder no longer reaches for a module-level
    ``db_manager`` via in-method imports.  Callers that want the builder's
    ``_find_*`` helpers to dedupe against the database **must inject a
    ``Session``** via the ``session`` kwarg.  When ``session`` is ``None``,
    the lookups short-circuit to ``None`` and the caller is responsible for
    persisting whatever the builder creates.  This breaks the previous
    circular-import smell on :mod:`app.core.unified_database` and makes the
    builder unit-testable against an in-memory SQLite session.
    """

    def __init__(
        self,
        state: str,
        state_id: int | None,
        state_code: str | None = None,
        *,
        session: Session | None = None,
        strict_field_resolution: bool = False,
        cache: "BuilderCache | None" = None,
    ):
        self.state_slug = state
        self.state_id = state_id
        self.state_code = state_code
        self.session = session
        self.strict_field_resolution = strict_field_resolution
        self._cache = cache
        self.logger = Logger(self.__class__.__name__)
        self.field_mappings = {
            mapping.state_field: mapping.unified_field
            for mapping in field_library.get_state_mappings(state)
        }

    def build_transaction(self, raw_data: dict[str, Any]) -> UnifiedTransaction:
        """
        Build a unified transaction from raw state-specific data.

        Args:
            raw_data: Dictionary containing state-specific field data

        Returns:
            UnifiedTransaction object with normalized data
        """
        # Initialize the transaction
        transaction = UnifiedTransaction(
            state_id=self.state_id, raw_data=json.dumps(raw_data.copy(), default=self._json_default)
        )

        # Map core transaction fields
        transaction.transaction_id = self._get_field_value(raw_data, "transaction_id")
        transaction.amount = self._parse_amount(self._get_field_value(raw_data, "amount"))
        if transaction.amount is None:
            # Travel (TRVL) rows carry the value on parentAmount → parent_amount;
            # use it as the base-transaction amount when no direct amount exists.
            transaction.amount = self._parse_amount(
                self._get_field_value(raw_data, "parent_amount")
            )
        transaction.transaction_date = self._parse_date(
            self._get_field_value(raw_data, "transaction_date")
        )
        if transaction.transaction_date is None:
            # Fallback to the filing's received date for record types that carry
            # no transaction-specific date (TEC debt/asset rows only have
            # receivedDt) — better than a null date for downstream date logic.
            transaction.transaction_date = self._parse_date(raw_data.get("receivedDt"))
        transaction.description = self._get_field_value(raw_data, "description")
        transaction.transaction_type = self._determine_transaction_type(raw_data)

        # Map administrative fields
        transaction.filed_date = self._parse_date(self._get_field_value(raw_data, "filed_date"))
        transaction.amended = self._parse_boolean(self._get_field_value(raw_data, "amended"))

        # Report linkage — TEC populates reportInfoIdent on every row; storing it
        # here enables link_transactions_to_reports() to run without re-reading raw_data.
        report_ident_raw = self._get_field_value(raw_data, "report_ident")
        if report_ident_raw is not None:
            transaction.report_ident = str(report_ident_raw).strip() or None

        # Map metadata fields
        transaction.download_date = raw_data.get("download_date")

        return transaction

    def _parse_person_name(
        self, raw_data: dict[str, Any], field_prefix: str = "person"
    ) -> PersonName:
        """Extract normalized person name parts from raw state data.

        Args:
            raw_data: State-specific record dict.
            field_prefix: Unified-field prefix for name lookups.  Defaults to
                ``"person"`` (generic path).  Pass a role-specific prefix such
                as ``"contributor"`` or ``"payee"`` so that role-scoped column
                mappings are used instead of the shared generic ones (Fix 1c).
        """

        # Role-scoped key first (contributor_first_name, payee_first_name, …)
        # with a fallback to the generic person_* key.  The fallback keeps the
        # committee/officer path and generically-normalized inputs working even
        # when a role prefix is supplied (Fix 1c).  When field_prefix is already
        # "person" the two keys coincide and the fallback is a no-op.
        def _scoped(suffix: str) -> Any:
            return self._get_field_value(
                raw_data, f"{field_prefix}_{suffix}"
            ) or self._get_field_value(raw_data, f"person_{suffix}")

        return PersonName(
            first=_scoped("first_name"),
            middle=self._get_field_value(raw_data, "person_middle_name"),
            last=_scoped("last_name"),
            suffix=_scoped("suffix"),
            organization=_scoped("organization"),
        )

    def _parse_address_parts(
        self, raw_data: dict[str, Any], field_prefix: str = "address"
    ) -> AddressParts:
        """Extract normalized address components from raw state data.

        Args:
            raw_data: State-specific record dict.
            field_prefix: Unified-field prefix for address lookups.  Defaults
                to ``"address"`` (used for committees / generic path).  Pass a
                role prefix (e.g. ``"contributor"``, ``"payee"``) for
                role-scoped column resolution (Fix 1c).
        """

        # Role-scoped key first, falling back to the generic address_* key
        # (mirrors _parse_person_name) so generically-normalized inputs and the
        # committee/officer path still resolve under a role prefix (Fix 1c).
        def _scoped(suffix: str) -> Any:
            return self._get_field_value(
                raw_data, f"{field_prefix}_{suffix}"
            ) or self._get_field_value(raw_data, f"address_{suffix}")

        return AddressParts(
            street_1=_scoped("street_1"),
            street_2=_scoped("street_2"),
            city=_scoped("city"),
            state=_scoped("state"),
            zip_code=_scoped("zip"),
        ).normalized()

    def build_person(
        self, raw_data: dict[str, Any], role: PersonRole, field_prefix: str = "contributor"
    ) -> UnifiedPerson | None:
        """Build a unified person from raw data.

        Args:
            raw_data: State-specific record dict.
            role: The ``PersonRole`` this person occupies in the transaction.
            field_prefix: Unified-field prefix for name and address lookups.
                Defaults to ``"contributor"``.  Pass the role-specific prefix
                from ``RECORD_TYPE_ROLE_MAP`` (e.g. ``"payee"``, ``"lender"``)
                so that each role resolves its own TEC source columns (Fix 1c).
        """
        name = self._parse_person_name(raw_data, field_prefix=field_prefix)
        if not name.full_name:
            return None

        # Build the address BEFORE the find-or-create: the individual dedup key now
        # includes the address (so two same-name people at distinct locations stay
        # separate, matching uix_persons_name_state).  ``address_key`` is the
        # denormalized dedup_addr_key string (None when too few address fields ->
        # name-only key, preserving today's behavior for addressless persons).
        address = self.build_address(raw_data, role.value, field_prefix=field_prefix)
        address_key = self._person_dedup_addr_key(address)

        # ── Find-or-create: check the DB before building a new row ────────────
        # The DB enforces case-insensitive unique indexes on
        # (lower(first_name), lower(last_name), state_id, dedup_addr_key) and
        # (lower(organization), state_id).  Without this check every second
        # occurrence of the same person across multiple contribution files
        # raises a UniqueViolation.
        existing_person = self._find_person_by_name_state(
            first_name=name.first,
            last_name=name.last,
            organization=name.organization,
            address_key=address_key,
        )

        # Per-occurrence fields that may be absent on the first time a person is
        # seen (e.g. first as a committee officer / payee, later as a contributor
        # carrying employer + occupation).  Collected here so they can backfill
        # an existing (deduped) person as well as populate a new one — otherwise
        # the first-occurrence-wins dedup silently drops them.
        extra_values: dict[str, Any] = {}
        for extra_field in ("employer", "occupation"):
            value = self._get_field_value(raw_data, f"person_{extra_field}")
            if value:
                extra_values[extra_field] = value
        if name.middle:
            extra_values["middle_name"] = name.middle
        if name.suffix:
            extra_values["suffix"] = name.suffix

        if existing_person is not None:
            for field_name, field_value in extra_values.items():
                if not getattr(existing_person, field_name, None):
                    setattr(existing_person, field_name, field_value)
            return existing_person

        person_data: dict[str, Any] = {
            "first_name": name.first,
            "last_name": name.last,
            "middle_name": name.middle,
            "suffix": name.suffix,
            "organization": name.organization,
        }
        for extra_field in ("employer", "occupation"):
            if extra_field in extra_values:
                person_data[extra_field] = extra_values[extra_field]

        # Determine person type
        person_type = PersonType.UNKNOWN
        last_name = (name.last or "").strip()
        first_name = (name.first or "").strip()

        if last_name.upper() in PLACEHOLDER_NAMES:
            person_type = PersonType.UNKNOWN
        elif name.organization:
            person_type = PersonType.ORGANIZATION
        elif first_name and last_name:
            person_type = PersonType.INDIVIDUAL
        elif last_name and not first_name:
            person_type = PersonType.UNKNOWN

        person = UnifiedPerson(
            **person_data,
            person_type=person_type,
            state_id=self.state_id,
            address=address,
            # NULL for org-persons (key on lower(org), state alone) and for addressless
            # individuals; otherwise the denormalized address key the unique index uses.
            dedup_addr_key=None if name.organization else address_key,
        )

        # Write-through: subsequent rows naming the same person reuse this object
        # instead of re-querying (or creating a duplicate that violates the
        # uix_persons_* unique indexes).
        if self._cache is not None:
            key = BuilderCache.person_key(
                name.first, name.last, name.organization, self.state_id, address_key
            )
            if key is not None:
                self._cache.persons[key] = person

        entity_type = (
            EntityType.ORGANIZATION if person_type == PersonType.ORGANIZATION else EntityType.PERSON
        )
        entity_name = person.organization if person.organization else person.full_name
        entity = self._get_or_create_entity(
            entity_type=entity_type, name=entity_name, address=address, person=person
        )
        if entity:
            person.entity = entity

        return person

    def build_address(
        self, raw_data: dict[str, Any], entity_role: str, field_prefix: str = "address"
    ) -> UnifiedAddress | None:
        """Build a unified address from raw data.

        Args:
            raw_data: State-specific record dict.
            entity_role: Unused legacy parameter kept for API compat.
            field_prefix: Unified-field prefix for address lookups.  Defaults
                to ``"address"`` (generic path / committees).  Pass a role
                prefix (e.g. ``"contributor"``, ``"payee"``) when building a
                role-scoped address (Fix 1c).
        """
        _ = entity_role
        parts = self._parse_address_parts(raw_data, field_prefix=field_prefix)
        address_data: dict[str, Any] = {
            "street_1": parts.street_1,
            "street_2": parts.street_2,
            "city": parts.city,
            "state": parts.state,
            "zip_code": parts.zip_code,
        }
        for extra_field, unified_field in (
            ("country", "address_country"),
            ("county", "address_county"),
        ):
            value = self._get_field_value(raw_data, unified_field)
            if value:
                address_data[extra_field] = value

        if any(address_data.get(field) for field in ("street_1", "city", "state", "zip_code")):
            # Check if address already exists
            existing_address = self._find_address_by_fields(address_data)
            if existing_address:
                return existing_address

            # Create new address (write-through so repeat addresses reuse it).
            new_address = UnifiedAddress(**address_data)
            if self._cache is not None:
                key = BuilderCache.address_key(address_data)
                if key is not None:
                    self._cache.addresses[key] = new_address
            return new_address

        return None

    def build_committee(self, raw_data: dict[str, Any]) -> UnifiedCommittee | None:
        """Build a unified committee from raw data"""
        committee_data = {}

        # Map committee fields
        committee_fields = {
            "name": "committee_name",
            "committee_type": "committee_type",
            "filer_id": "committee_filer_id",
        }

        for unified_field, field_name in committee_fields.items():
            value = self._get_field_value(raw_data, field_name)
            if value:
                committee_data[unified_field] = value

        # Handle missing committee name - for candidate committees, use candidate name
        if not committee_data.get("name"):
            committee_type = committee_data.get("committee_type", "").lower()
            candidate_name = raw_data.get("Candidate Name", "")

            if "candidate" in committee_type and candidate_name:
                # For candidate committees, use candidate name as committee name
                committee_data["name"] = f"Candidate Committee - {candidate_name}"
            else:
                # Use filer_id as fallback name if no committee name is available
                filer_id = committee_data.get("filer_id", "UNKNOWN")
                committee_data["name"] = f"Committee {filer_id}"

        committee_address = self.build_address(raw_data, "committee")

        # If we found any committee data, create or find the committee
        if committee_data:
            # Check if committee already exists by filer_id
            existing_committee = self._find_committee_by_filer_id(committee_data.get("filer_id"))
            if existing_committee:
                if self.state_id and not existing_committee.state_id:
                    existing_committee.state_id = self.state_id
                if committee_address and not existing_committee.address:
                    existing_committee.address = committee_address
                if not existing_committee.entity:
                    entity = self._get_or_create_entity(
                        entity_type=EntityType.COMMITTEE,
                        name=existing_committee.name,
                        address=existing_committee.address,
                        committee=existing_committee,
                    )
                    if entity:
                        existing_committee.entity = entity
                return existing_committee

            committee = UnifiedCommittee(**committee_data)
            committee.state_id = self.state_id
            if committee.filer_id and self._cache is not None:
                self._cache.committees[committee.filer_id] = committee
            if committee_address:
                committee.address = committee_address
            entity = self._get_or_create_entity(
                entity_type=EntityType.COMMITTEE,
                name=committee.name,
                address=committee.address,
                committee=committee,
            )
            if entity:
                committee.entity = entity

            # Persist the new committee independently of the transaction.  The
            # loader severs ``transaction.committee`` (keeping the committee_id
            # FK) to dodge a relationship→FK sync race, which would otherwise
            # leave a freshly-built committee uninserted and break the
            # transaction's committee_id foreign key.  Adding it here lets
            # SQLAlchemy insert it (and its address/entity, via cascade) ahead of
            # the referring transaction.
            if self.session is not None:
                self.session.add(committee)

            return committee

        return None

    def _get_field_value(self, raw_data: dict[str, Any], unified_field: str) -> Any | None:
        """Get the value for a unified field from raw data"""
        # Handle None unified_field
        if unified_field is None:
            return None

        # First check if the unified field name is directly in raw_data
        # This handles cases where GenericFileReader already normalized the field names
        if unified_field in raw_data:
            return raw_data[unified_field]

        # Then try direct mapping from state-specific field names
        for state_field, mapped_field in self.field_mappings.items():
            if mapped_field == unified_field and state_field in raw_data:
                return raw_data[state_field]

        if self.strict_field_resolution:
            raise ValueError(
                f"No explicit mapping found for field '{unified_field}' in state "
                f"'{self.state_slug}'. Add a mapping to UnifiedFieldLibrary."
            )

        fuzzy_result = self._fuzzy_field_match(raw_data, unified_field)
        if fuzzy_result is not None:
            self.logger.debug(
                "Field '%s' resolved via fuzzy match for state '%s' — "
                "consider adding an explicit mapping to UnifiedFieldLibrary",
                unified_field,
                self.state_slug,
            )
            return fuzzy_result

        return None

    def _fuzzy_field_match(self, raw_data: dict[str, Any], unified_field: str) -> Any | None:
        """Word-overlap fallback when no explicit mapping exists."""
        for field_name, value in raw_data.items():
            if field_name is None:
                continue
            # Skip fields that look like unified field names (already normalized).
            if field_name.startswith(("person_", "address_", "committee_", "transaction_")):
                continue
            if self._fuzzy_match(field_name, unified_field):
                return value
        return None

    def _fuzzy_match(self, state_field: str, unified_field: str) -> bool:
        """Check if a state field roughly matches a unified field"""
        try:
            state_normalized = self._normalize_field_name(state_field)
            unified_normalized = self._normalize_field_name(unified_field)

            # Exact match after normalization
            if state_normalized == unified_normalized:
                return True

            # Check for exact word matches (more precise)
            state_words = set(state_normalized.split("_"))
            unified_words = set(unified_normalized.split("_"))

            if state_words and unified_words:
                # Require at least 2 words to match for better precision
                overlap = len(state_words.intersection(unified_words))
                return overlap >= 2

            return False
        except (TypeError, ValueError, AttributeError):
            return False

    def _find_committee_by_filer_id(self, filer_id: str) -> UnifiedCommittee | None:
        """Find an existing committee by ``filer_id`` using the injected session.

        Returns ``None`` if no session was injected (lookup-free build path)
        or if the query raises :class:`SQLAlchemyError`.  Other exceptions
        propagate so genuine bugs are not swallowed (P2-MNT-001).
        """
        if not filer_id:
            return None
        if self._cache is not None and filer_id in self._cache.committees:
            return self._cache.committees[filer_id]
        # Committees are always read-through (never authoritative-skip): filer_ingest
        # creates committee rows the builder cache does not author, so the DB is the
        # source of truth on a miss.
        if self.session is None:
            return None
        try:
            stmt = (
                select(UnifiedCommittee)
                .options(
                    selectinload(UnifiedCommittee.address),
                    selectinload(UnifiedCommittee.entity).selectinload(UnifiedEntity.address),
                )
                .where(UnifiedCommittee.filer_id == filer_id)
            )
            found = self.session.exec(stmt).first()
        except SQLAlchemyError:
            return None
        if found is not None and self._cache is not None:
            self._cache.committees[filer_id] = found
        return found

    def _normalize_entity_name(self, value: str | None) -> str:
        return normalize_entity_name(value)

    def _find_entity(
        self, entity_type: EntityType, normalized_name: str, address: UnifiedAddress | None
    ) -> UnifiedEntity | None:
        """Find an existing entity by ``(entity_type, normalized_name[, address])``.

        Returns ``None`` if no session was injected or the query raises a
        SQLAlchemy error.  See :meth:`_find_committee_by_filer_id` for the
        rationale (RF-SMELL-005, P2-MNT-001).
        """
        if not normalized_name:
            return None
        key = BuilderCache.entity_key(entity_type, normalized_name, self.state_id)
        if self._cache is not None and key is not None:
            if key in self._cache.entities:
                return self._cache.entities[key]
            if self._cache.authoritative:
                return None
        if self.session is None:
            return None
        try:
            # Key MUST match the uix_entities_type_name_state unique index exactly
            # — (entity_type, normalized_name, state_id), no address.  Narrowing by
            # address_id here caused the lookup to miss an existing same-name entity
            # with a different address (e.g. one created by filer_ingest for an
            # officer), then insert a duplicate that violates the index.
            query = select(UnifiedEntity).where(
                UnifiedEntity.entity_type == entity_type,
                UnifiedEntity.normalized_name == normalized_name,
                UnifiedEntity.state_id == self.state_id,  # Fix 4: state-scope dedup
            )
            found = self.session.exec(query).first()
        except SQLAlchemyError:
            return None
        if found is not None and self._cache is not None and key is not None:
            self._cache.entities[key] = found
        return found

    def _get_or_create_entity(
        self,
        entity_type: EntityType,
        name: str | None,
        address: UnifiedAddress | None,
        person: UnifiedPerson | None = None,
        committee: UnifiedCommittee | None = None,
    ) -> UnifiedEntity | None:
        normalized_name = self._normalize_entity_name(name)
        existing = self._find_entity(entity_type, normalized_name, address)
        if existing:
            if person and not existing.person:
                existing.person = person
            if committee and not existing.committee:
                existing.committee = committee
            if address and not existing.address:
                existing.address = address
            if self.state_id and not existing.state_id:
                existing.state_id = self.state_id
            return existing
        if not normalized_name and not name:
            return None
        entity = UnifiedEntity(
            entity_type=entity_type,
            name=name or normalized_name or None,
            normalized_name=normalized_name or None,
            address=address,
            person=person,
            committee=committee,
            state_id=self.state_id,
        )
        if self._cache is not None:
            key = BuilderCache.entity_key(entity_type, normalized_name, self.state_id)
            if key is not None:
                self._cache.entities[key] = entity
        return entity

    def _find_campaign(
        self,
        normalized_name: str,
        committee: UnifiedCommittee | None,
        candidate: UnifiedPerson | None,
        election_year: int | None,
    ) -> UnifiedCampaign | None:
        """Find an existing campaign by ``normalized_name`` (+ optional context).

        Returns ``None`` if no session was injected or the query raises a
        SQLAlchemy error.  See :meth:`_find_committee_by_filer_id` for the
        rationale (RF-SMELL-005, P2-MNT-001).
        """
        if not normalized_name:
            return None
        committee_filer_id = committee.filer_id if committee else None
        candidate_id = getattr(candidate, "id", None) if candidate else None
        key = BuilderCache.campaign_key(
            normalized_name, committee_filer_id, candidate_id, election_year
        )
        if self._cache is not None and key is not None:
            if key in self._cache.campaigns:
                return self._cache.campaigns[key]
            if self._cache.authoritative:
                return None
        if self.session is None:
            return None
        try:
            # Identify a campaign by (normalized_name, committee, election_year)
            # only — NOT by candidate.  A committee runs one campaign per cycle;
            # filtering on candidate_person_id would miss a campaign first created
            # by a non-CAND transaction (candidate NULL) and spawn a duplicate.
            # build_campaign backfills the candidate onto the reused campaign.
            query = select(UnifiedCampaign).where(
                UnifiedCampaign.normalized_name == normalized_name
            )
            if committee and committee.filer_id:
                query = query.where(UnifiedCampaign.primary_committee_id == committee.filer_id)
            if election_year:
                query = query.where(UnifiedCampaign.election_year == election_year)
            found = self.session.exec(query).first()
        except SQLAlchemyError:
            return None
        if found is not None and self._cache is not None and key is not None:
            self._cache.campaigns[key] = found
        return found

    def build_campaign(
        self,
        raw_data: dict[str, Any],
        committee: UnifiedCommittee | None,
        candidate: UnifiedPerson | None,
        transaction: UnifiedTransaction | None,
    ) -> UnifiedCampaign | None:
        campaign_name = self._get_field_value(raw_data, "campaign_name")
        if not campaign_name:
            campaign_name = candidate.full_name if candidate else None
        if not campaign_name and committee:
            campaign_name = committee.name
        normalized_name = self._normalize_entity_name(campaign_name)
        if not normalized_name:
            return None
        transaction_date = transaction.transaction_date if transaction else None
        if not transaction_date:
            transaction_date = self._parse_date(self._get_field_value(raw_data, "transaction_date"))
        election_year = transaction_date.year if isinstance(transaction_date, date) else None
        campaign = self._find_campaign(normalized_name, committee, candidate, election_year)
        if campaign:
            # Backfill the candidate when this record carries one but the existing
            # campaign (often first created by a non-CAND transaction) has none —
            # so the campaign, and downstream canonical_campaign, gets a candidate.
            if (
                candidate is not None
                and campaign.candidate_person_id is None
                and campaign.candidate is None
            ):
                campaign.candidate = candidate
                if candidate.entity is not None:
                    campaign.entities.append(
                        UnifiedCampaignEntity(
                            campaign=campaign,
                            entity=candidate.entity,
                            state_id=self.state_id,
                            role=CampaignRole.CANDIDATE,
                            is_primary=True,
                        )
                    )
            return campaign
        campaign = UnifiedCampaign(
            name=campaign_name,
            normalized_name=normalized_name,
            election_year=election_year,
            office_sought=self._get_field_value(raw_data, "office_sought"),
            district=self._get_field_value(raw_data, "district_info"),
            candidate=candidate,
            primary_committee=committee,
            state_id=self.state_id,
        )
        if self._cache is not None:
            key = BuilderCache.campaign_key(
                normalized_name,
                committee.filer_id if committee else None,
                getattr(candidate, "id", None) if candidate else None,
                election_year,
            )
            if key is not None:
                self._cache.campaigns[key] = campaign
        if candidate and candidate.entity:
            campaign.entities.append(
                UnifiedCampaignEntity(
                    campaign=campaign,
                    entity=candidate.entity,
                    state_id=self.state_id,
                    role=CampaignRole.CANDIDATE,
                    is_primary=True,
                )
            )
        if committee and committee.entity:
            campaign.entities.append(
                UnifiedCampaignEntity(
                    campaign=campaign,
                    entity=committee.entity,
                    state_id=self.state_id,
                    role=CampaignRole.COMMITTEE,
                    is_primary=True,
                )
            )
        return campaign

    @staticmethod
    def _person_dedup_addr_key(address: "UnifiedAddress | None") -> str | None:
        """Denormalized ``dedup_addr_key`` for an individual person's address.

        Mirrors ``BuilderCache.address_key_str``: requires ≥2 populated address
        fields, else ``None`` (name-only key).  ``address`` is the ``UnifiedAddress``
        the person carries (built per-occurrence by ``build_address``).
        """
        if address is None:
            return None
        return BuilderCache.address_key_str(
            {
                "street_1": address.street_1,
                "city": address.city,
                "state": address.state,
                "zip_code": address.zip_code,
            }
        )

    def _find_person_by_name_state(
        self,
        first_name: str | None,
        last_name: str | None,
        organization: str | None,
        address_key: str | None = None,
    ) -> UnifiedPerson | None:
        """Find an existing person using the same key as the DB unique indexes.

        Mirrors ``uix_persons_name_state`` — now keyed on
        ``(lower(first), lower(last), state_id, dedup_addr_key)`` — and
        ``uix_persons_org_state`` (organization); both use ``lower()`` so the
        lookup must also be case-insensitive.  For individuals the stored
        ``dedup_addr_key`` column is matched NULL-aware against ``address_key`` so
        two same-name people at distinct locations resolve to separate rows.

        Returns ``None`` if no session is available or no anchor values exist.
        """
        key = BuilderCache.person_key(
            first_name, last_name, organization, self.state_id, address_key
        )
        if self._cache is not None and key is not None:
            if key in self._cache.persons:
                return self._cache.persons[key]
            if self._cache.authoritative:
                return None
        if self.session is None:
            return None
        try:
            if organization:
                stmt = select(UnifiedPerson).where(
                    sa_func.lower(UnifiedPerson.organization) == organization.lower(),
                    UnifiedPerson.state_id == self.state_id,
                    UnifiedPerson.organization.is_not(None),
                )
            elif first_name and last_name:
                # NULL-aware equality on dedup_addr_key: ``is_(None)`` when the address
                # key is absent (matches the partial index's NULL rows), else ``==``.
                addr_match = (
                    UnifiedPerson.dedup_addr_key.is_(None)
                    if address_key is None
                    else UnifiedPerson.dedup_addr_key == address_key
                )
                stmt = select(UnifiedPerson).where(
                    sa_func.lower(UnifiedPerson.first_name) == first_name.lower(),
                    sa_func.lower(UnifiedPerson.last_name) == last_name.lower(),
                    UnifiedPerson.state_id == self.state_id,
                    UnifiedPerson.organization.is_(None),
                    addr_match,
                )
            else:
                return None
            found = self.session.exec(stmt).first()
        except SQLAlchemyError:
            return None
        if found is not None and self._cache is not None and key is not None:
            self._cache.persons[key] = found
        return found

    def _find_address_by_fields(self, address_data: dict[str, Any]) -> UnifiedAddress | None:
        """Find an existing address by key fields using the injected session.

        Returns ``None`` if ``address_data`` is empty, no session was injected,
        too few fields are populated to identify a row, or the query raises a
        SQLAlchemy error (RF-SMELL-005, P2-MNT-001).
        """
        if not address_data:
            return None
        key = BuilderCache.address_key(address_data)
        if self._cache is not None and key is not None:
            if key in self._cache.addresses:
                return self._cache.addresses[key]
            if self._cache.authoritative:
                return None
        if self.session is None:
            return None
        try:
            street_1_val = address_data.get("street_1")
            city_val = address_data.get("city")
            state_val = address_data.get("state")
            zip_val = address_data.get("zip_code")

            # Require at least two non-None anchor values to avoid matching
            # all-null rows in the database.
            populated = sum(
                1 for v in (street_1_val, city_val, state_val, zip_val) if v is not None
            )
            if populated < 2:
                return None

            # Case-insensitive match on the fields we actually have.  Absent
            # (None) fields are left unconstrained — a partial lookup (e.g.
            # street+city+state, no zip) should still resolve a fuller stored
            # address rather than forcing those columns to be NULL, which would
            # never match a populated row.  city/state/street_1 use lower() to
            # mirror the DB unique indexes; zip is stored as-is.
            conditions = []
            if street_1_val is not None:
                conditions.append(sa_func.lower(UnifiedAddress.street_1) == street_1_val.lower())
            if city_val is not None:
                conditions.append(sa_func.lower(UnifiedAddress.city) == city_val.lower())
            if state_val is not None:
                conditions.append(sa_func.lower(UnifiedAddress.state) == state_val.lower())
            if zip_val is not None:
                conditions.append(UnifiedAddress.zip_code == zip_val)

            stmt = select(UnifiedAddress).where(*conditions)
            found = self.session.exec(stmt).first()
        except SQLAlchemyError:
            return None
        if found is not None and self._cache is not None and key is not None:
            self._cache.addresses[key] = found
        return found

    def _normalize_field_name(self, field_name: str) -> str:
        """Normalize a field name for comparison"""
        if field_name is None:
            return ""
        try:
            normalized = str(field_name).lower()
            normalized = re.sub(r"[^a-z0-9]", "_", normalized)
            normalized = re.sub(r"_+", "_", normalized)
            return normalized.strip("_")
        except (TypeError, ValueError, AttributeError):
            return ""

    def _parse_amount(self, value: Any) -> Decimal | None:
        """Parse an amount value to Decimal, tolerating non-numeric input.

        Strips currency/percent symbols; returns None (rather than raising) for
        values that don't reduce to a number — e.g. an ``interestRate`` of
        ``"Prime Rate"`` or ``"NONE"`` — so one bad row never fails a whole file.
        """
        if value is None:
            return None

        try:
            if isinstance(value, str):
                value = re.sub(r"[^\d.-]", "", value)
                if value in ("", ".", "-", "-.", "."):
                    return None
            return Decimal(str(value))
        except (ValueError, TypeError, InvalidOperation):
            return None

    def _parse_date(self, value: Any) -> date | None:
        """Parse a date value, rejecting implausible years."""
        if value is None:
            return None

        parsed: date | None = None
        try:
            if isinstance(value, str):
                # Try common date formats.
                # "%Y%m%d" must come BEFORE ambiguous short formats — TEC (Texas) stores
                # dates as compact 8-digit strings like "20120702".
                for fmt in [
                    "%Y%m%d",  # TEC compact: "20120702"
                    "%Y-%m-%d",  # ISO: "2012-07-02"
                    "%m/%d/%Y",  # US: "07/02/2012"
                    "%m-%d-%Y",  # US dashed: "07-02-2012"
                    "%Y/%m/%d",  # ISO slashed: "2012/07/02"
                    "%m/%d/%Y %H:%M:%S",  # US datetime: "07/02/2012 00:00:00"
                    "%Y-%m-%dT%H:%M:%S",  # ISO datetime: "2012-07-02T00:00:00"
                ]:
                    try:
                        parsed = datetime.strptime(value, fmt).date()
                        break
                    except ValueError:
                        continue
            elif isinstance(value, (date, datetime)):
                parsed = value.date() if isinstance(value, datetime) else value
        except (ValueError, TypeError):
            return None

        # Guard against malformed source dates (e.g. a stray "00041110" parsing
        # to year 4) — they corrupt min/max date windows downstream.
        if parsed is not None and not (1900 <= parsed.year <= 2100):
            return None
        return parsed

    def _parse_boolean(self, value: Any) -> bool:
        """Parse a boolean value"""
        if value is None:
            return False

        if isinstance(value, bool):
            return value

        if isinstance(value, str):
            return value.lower() in ["true", "yes", "y", "1", "t"]

        if isinstance(value, (int, float)):
            return bool(value)

        return False

    def _determine_transaction_type(self, raw_data: dict[str, Any]) -> TransactionType:
        """Determine the transaction type from raw data"""
        # Check for explicit transaction type
        type_value = self._get_field_value(raw_data, "transaction_type")
        if type_value:
            type_str = str(type_value).lower()
            type_synonyms = {
                "monetary": TransactionType.CONTRIBUTION,
                "money": TransactionType.CONTRIBUTION,
                "in-kind": TransactionType.CONTRIBUTION,
                "inkind": TransactionType.CONTRIBUTION,
                "loan": TransactionType.LOAN,
                "expenditure": TransactionType.EXPENDITURE,
                "expense": TransactionType.EXPENDITURE,
                "debt": TransactionType.DEBT,
                "obligation": TransactionType.DEBT,
                "credit": TransactionType.CREDIT,
                "refund": TransactionType.CREDIT,
                "return": TransactionType.CREDIT,
                "travel": TransactionType.TRAVEL,
                "trip": TransactionType.TRAVEL,
                "transportation": TransactionType.TRAVEL,
                "asset": TransactionType.ASSET,
                "equipment": TransactionType.ASSET,
                "property": TransactionType.ASSET,
            }
            if type_str in type_synonyms:
                return type_synonyms[type_str]
            for transaction_type in TransactionType:
                if transaction_type.value in type_str:
                    return transaction_type

        # Check for explicit record_type field (Texas uses this).  TEC parquet
        # sources expose it as camelCase ``recordType``; accept both spellings.
        record_type = (raw_data.get("record_type") or raw_data.get("recordType") or "").upper()
        if record_type in RECORD_TYPE_TO_TRANSACTION:
            return RECORD_TYPE_TO_TRANSACTION[record_type]

        # Infer from field names
        field_names = [k.lower() for k in raw_data.keys()]
        field_type_rules: list[tuple[tuple[str, ...], TransactionType]] = [
            (("contribution",), TransactionType.CONTRIBUTION),
            (("expenditure", "expend"), TransactionType.EXPENDITURE),
            (("loan",), TransactionType.LOAN),
            (("pledge",), TransactionType.PLEDGE),
            (("debt",), TransactionType.DEBT),
            (("credit",), TransactionType.CREDIT),
            (("travel",), TransactionType.TRAVEL),
            (("asset",), TransactionType.ASSET),
            (("refund",), TransactionType.REFUND),
            (("transfer",), TransactionType.TRANSFER),
        ]
        for keywords, transaction_type in field_type_rules:
            if any(any(keyword in name for keyword in keywords) for name in field_names):
                return transaction_type

        return TransactionType.OTHER

    def _json_default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return str(obj)
