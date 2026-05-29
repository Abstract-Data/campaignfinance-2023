"""Build unified SQLModel rows from state-specific records."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from app.core.constants import PLACEHOLDER_NAMES, RECORD_TYPE_TO_TRANSACTION
from app.core.enums import CampaignRole, EntityType, PersonRole, PersonType, TransactionType
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
from app.core.value_objects import AddressParts, PersonName
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
    ):
        self.state_slug = state
        self.state_id = state_id
        self.state_code = state_code
        self.session = session
        self.strict_field_resolution = strict_field_resolution
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
        transaction.transaction_date = self._parse_date(
            self._get_field_value(raw_data, "transaction_date")
        )
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

    def _parse_person_name(self, raw_data: dict[str, Any], field_prefix: str = "person") -> PersonName:
        """Extract normalized person name parts from raw state data.

        Args:
            raw_data: State-specific record dict.
            field_prefix: Unified-field prefix for name lookups.  Defaults to
                ``"person"`` (generic path).  Pass a role-specific prefix such
                as ``"contributor"`` or ``"payee"`` so that role-scoped column
                mappings are used instead of the shared generic ones (Fix 1c).
        """
        return PersonName(
            first=self._get_field_value(raw_data, f"{field_prefix}_first_name"),
            middle=self._get_field_value(raw_data, "person_middle_name"),
            last=self._get_field_value(raw_data, f"{field_prefix}_last_name"),
            suffix=self._get_field_value(raw_data, "person_suffix"),
            organization=self._get_field_value(raw_data, f"{field_prefix}_organization"),
        )

    def _parse_address_parts(self, raw_data: dict[str, Any], field_prefix: str = "address") -> AddressParts:
        """Extract normalized address components from raw state data.

        Args:
            raw_data: State-specific record dict.
            field_prefix: Unified-field prefix for address lookups.  Defaults
                to ``"address"`` (used for committees / generic path).  Pass a
                role prefix (e.g. ``"contributor"``, ``"payee"``) for
                role-scoped column resolution (Fix 1c).
        """
        return AddressParts(
            street_1=self._get_field_value(raw_data, f"{field_prefix}_street_1"),
            street_2=self._get_field_value(raw_data, f"{field_prefix}_street_2"),
            city=self._get_field_value(raw_data, f"{field_prefix}_city"),
            state=self._get_field_value(raw_data, f"{field_prefix}_state"),
            zip_code=self._get_field_value(raw_data, f"{field_prefix}_zip"),
        ).normalized()

    def build_person(self, raw_data: dict[str, Any], role: PersonRole, field_prefix: str = "contributor") -> UnifiedPerson | None:
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

        person_data: dict[str, Any] = {
            "first_name": name.first,
            "last_name": name.last,
            "middle_name": name.middle,
            "suffix": name.suffix,
            "organization": name.organization,
        }
        for extra_field in ("employer", "occupation"):
            value = self._get_field_value(raw_data, f"person_{extra_field}")
            if value:
                person_data[extra_field] = value

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

        address = self.build_address(raw_data, role.value, field_prefix=field_prefix)

        person = UnifiedPerson(
            **person_data, person_type=person_type, state_id=self.state_id, address=address
        )

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

    def build_address(self, raw_data: dict[str, Any], entity_role: str, field_prefix: str = "address") -> UnifiedAddress | None:
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

            # Create new address
            return UnifiedAddress(**address_data)

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
        if not filer_id or self.session is None:
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
            return self.session.exec(stmt).first()
        except SQLAlchemyError:
            return None

    def _normalize_entity_name(self, value: str | None) -> str:
        if not value:
            return ""
        normalized = value.strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    def _find_entity(
        self, entity_type: EntityType, normalized_name: str, address: UnifiedAddress | None
    ) -> UnifiedEntity | None:
        """Find an existing entity by ``(entity_type, normalized_name[, address])``.

        Returns ``None`` if no session was injected or the query raises a
        SQLAlchemy error.  See :meth:`_find_committee_by_filer_id` for the
        rationale (RF-SMELL-005, P2-MNT-001).
        """
        if not normalized_name or self.session is None:
            return None
        try:
            query = select(UnifiedEntity).where(
                UnifiedEntity.entity_type == entity_type,
                UnifiedEntity.normalized_name == normalized_name,
                UnifiedEntity.state_id == self.state_id,  # Fix 4: state-scope dedup
            )
            if address and getattr(address, "id", None):
                query = query.where(UnifiedEntity.address_id == address.id)
            return self.session.exec(query).first()
        except SQLAlchemyError:
            return None

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
        if not normalized_name or self.session is None:
            return None
        try:
            query = select(UnifiedCampaign).where(
                UnifiedCampaign.normalized_name == normalized_name
            )
            if committee and committee.filer_id:
                query = query.where(UnifiedCampaign.primary_committee_id == committee.filer_id)
            if candidate and getattr(candidate, "id", None):
                query = query.where(UnifiedCampaign.candidate_person_id == candidate.id)
            if election_year:
                query = query.where(UnifiedCampaign.election_year == election_year)
            return self.session.exec(query).first()
        except SQLAlchemyError:
            return None

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

    def _find_address_by_fields(self, address_data: dict[str, Any]) -> UnifiedAddress | None:
        """Find an existing address by key fields using the injected session.

        Returns ``None`` if ``address_data`` is empty, no session was injected,
        too few fields are populated to identify a row, or the query raises a
        SQLAlchemy error (RF-SMELL-005, P2-MNT-001).
        """
        if not address_data or self.session is None:
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

            # Fix 5: NULL-safe comparisons — plain == never matches NULL columns,
            # so we use IS NULL when the candidate value is None.
            def _null_safe(col: Any, val: Any) -> Any:
                return col.is_(None) if val is None else col == val

            stmt = select(UnifiedAddress).where(
                _null_safe(UnifiedAddress.street_1, street_1_val),
                _null_safe(UnifiedAddress.city, city_val),
                _null_safe(UnifiedAddress.state, state_val),
                _null_safe(UnifiedAddress.zip_code, zip_val),
            )
            return self.session.exec(stmt).first()
        except SQLAlchemyError:
            return None

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
        """Parse an amount value to Decimal"""
        if value is None:
            return None

        try:
            # Remove currency symbols and commas
            if isinstance(value, str):
                value = re.sub(r"[^\d.-]", "", value)

            return Decimal(str(value))
        except (ValueError, TypeError):
            return None

    def _parse_date(self, value: Any) -> date | None:
        """Parse a date value"""
        if value is None:
            return None

        try:
            if isinstance(value, str):
                # Try common date formats.
                # "%Y%m%d" must come BEFORE ambiguous short formats — TEC (Texas) stores
                # dates as compact 8-digit strings like "20120702".
                for fmt in [
                    "%Y%m%d",             # TEC compact: "20120702"
                    "%Y-%m-%d",           # ISO: "2012-07-02"
                    "%m/%d/%Y",           # US: "07/02/2012"
                    "%m-%d-%Y",           # US dashed: "07-02-2012"
                    "%Y/%m/%d",           # ISO slashed: "2012/07/02"
                    "%m/%d/%Y %H:%M:%S",  # US datetime: "07/02/2012 00:00:00"
                    "%Y-%m-%dT%H:%M:%S",  # ISO datetime: "2012-07-02T00:00:00"
                ]:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
            elif isinstance(value, (date, datetime)):
                return value.date() if isinstance(value, datetime) else value

            return None
        except (ValueError, TypeError):
            return None

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

        # Check for explicit record_type field (Texas uses this)
        record_type = raw_data.get("record_type", "").upper()
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
