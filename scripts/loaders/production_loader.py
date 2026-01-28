#!/usr/bin/env python3
"""
Production-ready campaign finance data loader with batch processing.
Features:
- Batch processing for memory efficiency
- Comprehensive error handling and logging
- Progress tracking with rich output
- Address and committee deduplication
- Transaction rollback on errors
- Performance metrics
- Configurable batch sizes and limits
"""

import time
import logging
import re
from itertools import islice
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn
from rich.table import Table
from rich.panel import Panel
from rich import print as rprint

from sqlalchemy import text
from sqlmodel import select

from app.ingest import GenericFileReader, SchemaValidationError, build_schema_for_states, build_unified_schema
from app.core.unified_field_library import field_library
from sqlalchemy.exc import InvalidRequestError

from app.core.unified_sqlmodels import (
    unified_sql_processor,
    UnifiedCommittee,
    UnifiedAddress,
    UnifiedEntity,
    UnifiedCampaign,
    UnifiedPerson,
    FileOrigin,
    State,
    EntityType,
)
from app.states.postgres_config import create_postgres_database_manager
from loader_config import get_config, get_file_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('campaign_finance_loader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

US_STATE_METADATA: Dict[str, Tuple[str, str]] = {
    "alabama": ("AL", "Alabama"),
    "alaska": ("AK", "Alaska"),
    "arizona": ("AZ", "Arizona"),
    "arkansas": ("AR", "Arkansas"),
    "california": ("CA", "California"),
    "colorado": ("CO", "Colorado"),
    "connecticut": ("CT", "Connecticut"),
    "delaware": ("DE", "Delaware"),
    "florida": ("FL", "Florida"),
    "georgia": ("GA", "Georgia"),
    "hawaii": ("HI", "Hawaii"),
    "idaho": ("ID", "Idaho"),
    "illinois": ("IL", "Illinois"),
    "indiana": ("IN", "Indiana"),
    "iowa": ("IA", "Iowa"),
    "kansas": ("KS", "Kansas"),
    "kentucky": ("KY", "Kentucky"),
    "louisiana": ("LA", "Louisiana"),
    "maine": ("ME", "Maine"),
    "maryland": ("MD", "Maryland"),
    "massachusetts": ("MA", "Massachusetts"),
    "michigan": ("MI", "Michigan"),
    "minnesota": ("MN", "Minnesota"),
    "mississippi": ("MS", "Mississippi"),
    "missouri": ("MO", "Missouri"),
    "montana": ("MT", "Montana"),
    "nebraska": ("NE", "Nebraska"),
    "nevada": ("NV", "Nevada"),
    "new hampshire": ("NH", "New Hampshire"),
    "new jersey": ("NJ", "New Jersey"),
    "new mexico": ("NM", "New Mexico"),
    "new york": ("NY", "New York"),
    "north carolina": ("NC", "North Carolina"),
    "north dakota": ("ND", "North Dakota"),
    "ohio": ("OH", "Ohio"),
    "oklahoma": ("OK", "Oklahoma"),
    "oregon": ("OR", "Oregon"),
    "pennsylvania": ("PA", "Pennsylvania"),
    "rhode island": ("RI", "Rhode Island"),
    "south carolina": ("SC", "South Carolina"),
    "south dakota": ("SD", "South Dakota"),
    "tennessee": ("TN", "Tennessee"),
    "texas": ("TX", "Texas"),
    "utah": ("UT", "Utah"),
    "vermont": ("VT", "Vermont"),
    "virginia": ("VA", "Virginia"),
    "washington": ("WA", "Washington"),
    "west virginia": ("WV", "West Virginia"),
    "wisconsin": ("WI", "Wisconsin"),
    "wyoming": ("WY", "Wyoming"),
}

@dataclass
class LoaderConfig:
    """Configuration for the production loader"""
    batch_size: int = 100
    max_records: Optional[int] = None
    commit_frequency: int = 50  # Commit every N batches
    enable_progress: bool = True
    enable_logging: bool = True
    retry_failed: bool = True
    max_retries: int = 3

@dataclass
class LoaderStats:
    """Statistics for the loader"""
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    start_time: float = 0
    end_time: float = 0
    
    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return (self.successful_records / self.total_records) * 100
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    @property
    def records_per_second(self) -> float:
        if self.duration == 0:
            return 0.0
        return self.successful_records / self.duration

class ProductionLoader:
    """Production-ready campaign finance data loader"""
    
    def __init__(self, config: LoaderConfig):
        self.config = config
        self.console = Console()
        self.stats = LoaderStats()
        self.db_manager = create_postgres_database_manager()
        
        # Caches for deduplication
        self.address_cache: Dict[Tuple, object] = {}
        self.committee_cache: Dict[str, object] = {}
        self.entity_cache: Dict[Tuple, UnifiedEntity] = {}
        self.campaign_cache: Dict[Tuple[str, Optional[str], Optional[str], Optional[int]], object] = {}
        self.person_cache: Dict[Tuple, UnifiedPerson] = {}
        self.state_cache: Dict[str, State] = {}
        self.state_code_cache: Dict[str, State] = {}
        self.file_origin_cache: Dict[str, FileOrigin] = {}
        self.active_state: Optional[State] = None
        self.active_state_code: Optional[str] = None
        self.active_state_slug: Optional[str] = None
        
        # Reader cache (state -> GenericFileReader)
        self.reader_cache: Dict[str, GenericFileReader] = {}
        
        # Error tracking
        self.errors: List[Dict] = []
        self.failed_records: List[Dict] = []
    
    @staticmethod
    def _normalize_name(value: Optional[str]) -> str:
        if not value:
            return ""
        normalized = value.strip().lower()
        normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()
    
    @staticmethod
    def _address_key(address) -> Tuple[str, str, str, str]:
        if not address:
            return ("", "", "", "")
        return (
            (address.street_1 or "").strip().upper(),
            (address.city or "").strip().upper(),
            (address.state or "").strip().upper(),
            (address.zip_code or "").strip()
        )
    
    def _resolve_state_metadata(self, state_key: str) -> Tuple[str, str]:
        normalized = (state_key or "").strip().lower()
        if normalized in US_STATE_METADATA:
            return US_STATE_METADATA[normalized]
        if len(normalized) == 2:
            for slug, (code, name) in US_STATE_METADATA.items():
                if code.lower() == normalized:
                    return code, name
        raise ValueError(f"Unsupported state identifier '{state_key}'. Unable to resolve metadata.")

    def _ensure_state(self, session, state_key: str) -> State:
        normalized = (state_key or "").strip().lower()
        if normalized in self.state_cache:
            return self.state_cache[normalized]

        code, name = self._resolve_state_metadata(state_key)
        cached_by_code = self.state_code_cache.get(code)
        if cached_by_code:
            self.state_cache[normalized] = cached_by_code
            return cached_by_code

        state = session.exec(
            select(State).where(State.code == code)
        ).first()
        if not state:
            state = State(code=code, name=name)
            session.add(state)
            session.flush()

        self.state_cache[normalized] = state
        self.state_code_cache[code] = state
        return state

    def _ensure_file_origin(self, session, state: State, filename: Optional[str]) -> Optional[FileOrigin]:
        if not filename:
            return None
        key = FileOrigin.build_key(state.id, filename)
        cached = self.file_origin_cache.get(key)
        if cached:
            return cached
        file_origin = session.get(FileOrigin, key)
        if not file_origin:
            file_origin = FileOrigin(id=key, state_id=state.id, filename=filename)
            session.add(file_origin)
        self.file_origin_cache[key] = file_origin
        return file_origin

    def _entity_key(self, entity) -> Optional[Tuple]:
        if not entity:
            return None
        entity_type = getattr(entity, "entity_type", None)
        if not entity_type:
            return None
        if entity_type == EntityType.PERSON:
            person = getattr(entity, "person", None)
            if person and getattr(person, "uuid", None):
                return (entity_type.value, f"person_uuid:{person.uuid}")
            person_id = getattr(entity, "person_id", None)
            if person_id:
                return (entity_type.value, f"person_id:{person_id}")
            return None
        if entity_type == EntityType.COMMITTEE:
            return None
        normalized_name = entity.normalized_name or self._normalize_name(entity.name)
        if not normalized_name:
            return None
        return (entity_type.value, normalized_name, self._address_key(entity.address))
    
    def _campaign_key(self, campaign) -> Optional[Tuple[str, Optional[str], Optional[str], Optional[int]]]:
        if not campaign:
            return None
        normalized_name = campaign.normalized_name or self._normalize_name(campaign.name)
        if not normalized_name:
            return None
        candidate_name = None
        if getattr(campaign, "candidate", None):
            candidate_name = campaign.candidate.full_name
        return (normalized_name, campaign.primary_committee_id, candidate_name, campaign.election_year)

    def _person_key(self, person: Optional[UnifiedPerson]) -> Optional[Tuple]:
        if not person:
            return None
        first = (person.first_name or "").strip().lower()
        last = (person.last_name or "").strip().lower()
        middle = (person.middle_name or "").strip().lower() if person.middle_name else ""
        suffix = (person.suffix or "").strip().lower() if person.suffix else ""
        organization = (person.organization or "").strip().lower() if person.organization else ""
        address_id = getattr(person, "address_id", None)
        if not address_id and getattr(person, "address", None):
            address = person.address
            address_id = getattr(address, "id", None)
        if not any([first, last, organization]):
            return None
        address_component = address_id if address_id is not None else "__no_address__"
        return (first, middle, last, suffix, organization, address_component)

    def _ensure_person(self, session, person: Optional[UnifiedPerson]) -> Optional[UnifiedPerson]:
        if not person:
            return None
        key = self._person_key(person)
        if not key:
            return person
        if self.active_state and getattr(person, "state_id", None) is None:
            person.state_id = self.active_state.id
        cached_person = self.person_cache.get(key)
        if cached_person:
            if self.active_state and getattr(cached_person, "state_id", None) is None:
                cached_person.state_id = self.active_state.id
            return cached_person

        stmt = (
            select(UnifiedPerson)
            .where(
                UnifiedPerson.first_name == person.first_name,
                UnifiedPerson.last_name == person.last_name,
                UnifiedPerson.organization == person.organization,
                UnifiedPerson.address_id == person.address_id,
                UnifiedPerson.middle_name == person.middle_name,
                UnifiedPerson.suffix == person.suffix,
            )
            .limit(1)
        )
        existing_person = session.exec(stmt).first()
        if existing_person:
            if self.active_state and getattr(existing_person, "state_id", None) is None:
                existing_person.state_id = self.active_state.id
            self.person_cache[key] = existing_person
            return existing_person

        self.person_cache[key] = person
        return person

    def _dedupe_addresses(self, session) -> None:
        session.exec(
            text(
                """
                WITH dup AS (
                    SELECT id,
                           MIN(id) OVER (PARTITION BY street_1, city, state, zip_code) AS keep_id
                    FROM unified_addresses
                )
                UPDATE unified_persons up
                SET address_id = dup.keep_id
                FROM dup
                WHERE up.address_id = dup.id
                  AND dup.id <> dup.keep_id
                """
            )
        )
        session.exec(
            text(
                """
                WITH dup AS (
                    SELECT id,
                           MIN(id) OVER (PARTITION BY street_1, city, state, zip_code) AS keep_id
                    FROM unified_addresses
                )
                UPDATE unified_entities ue
                SET address_id = dup.keep_id
                FROM dup
                WHERE ue.address_id = dup.id
                  AND dup.id <> dup.keep_id
                """
            )
        )
        session.exec(
            text(
                """
                WITH dup AS (
                    SELECT id,
                           MIN(id) OVER (PARTITION BY street_1, city, state, zip_code) AS keep_id
                    FROM unified_addresses
                )
                UPDATE unified_committees uc
                SET address_id = dup.keep_id
                FROM dup
                WHERE uc.address_id = dup.id
                  AND dup.id <> dup.keep_id
                """
            )
        )
        session.exec(
            text(
                """
                DELETE FROM unified_addresses ua
                USING (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY street_1, city, state, zip_code ORDER BY id) AS rn
                    FROM unified_addresses
                ) dup
                WHERE ua.id = dup.id
                  AND dup.rn > 1
                """
            )
        )

    def _cleanup_orphan_persons(self, session) -> None:
        session.exec(
            text(
                """
                DELETE FROM unified_entities ue
                WHERE ue.person_id IS NOT NULL
                  AND ue.person_id NOT IN (
                    SELECT id FROM unified_persons
                )
                """
            )
        )
        session.exec(
            text(
                """
                DELETE FROM unified_entities ue
                WHERE ue.person_id IS NOT NULL
                  AND ue.person_id NOT IN (
                    SELECT DISTINCT person_id FROM unified_transaction_persons WHERE person_id IS NOT NULL
                  )
                  AND ue.person_id NOT IN (
                    SELECT DISTINCT candidate_person_id FROM unified_campaigns WHERE candidate_person_id IS NOT NULL
                  )
                  AND ue.person_id NOT IN (
                    SELECT DISTINCT person_id FROM unified_committee_persons WHERE person_id IS NOT NULL
                  )
                """
            )
        )
        session.exec(
            text(
                """
                DELETE FROM unified_persons up
                WHERE up.id NOT IN (
                    SELECT DISTINCT person_id FROM unified_transaction_persons WHERE person_id IS NOT NULL
                )
                AND up.id NOT IN (
                    SELECT DISTINCT candidate_person_id FROM unified_campaigns WHERE candidate_person_id IS NOT NULL
                )
                AND up.id NOT IN (
                    SELECT DISTINCT person_id FROM unified_committee_persons WHERE person_id IS NOT NULL
                )
                AND up.id NOT IN (
                    SELECT DISTINCT person_id FROM unified_entities WHERE person_id IS NOT NULL
                )
                """
            )
        )

    def _update_person_references(self, session, old_id: int, new_id: int) -> None:
        reference_tables = [
            ("unified_transaction_persons", "person_id"),
            ("unified_committee_persons", "person_id"),
            ("unified_campaigns", "candidate_person_id"),
        ]
        for table, column in reference_tables:
            session.exec(
                text(f"UPDATE {table} SET {column} = :new WHERE {column} = :old").bindparams(
                    new=new_id, old=old_id
                )
            )

    def _update_entity_references(self, session, old_id: int, new_id: int) -> None:
        reference_tables = [
            ("unified_transaction_persons", "entity_id"),
            ("unified_contributions", "contributor_entity_id"),
            ("unified_contributions", "recipient_entity_id"),
            ("unified_loans", "lender_entity_id"),
            ("unified_loans", "borrower_entity_id"),
            ("unified_campaign_entities", "entity_id"),
            ("unified_entity_associations", "source_entity_id"),
            ("unified_entity_associations", "target_entity_id"),
        ]
        for table, column in reference_tables:
            session.exec(
                text(f"UPDATE {table} SET {column} = :new WHERE {column} = :old").bindparams(
                    new=new_id, old=old_id
                )
            )

    def _dedupe_persons_and_entities(self, session) -> None:
        duplicate_groups = session.exec(
            text(
                """
                SELECT first_name, middle_name, last_name, suffix, organization, address_id,
                       array_agg(id ORDER BY id) AS person_ids
                FROM unified_persons
                GROUP BY first_name, middle_name, last_name, suffix, organization, address_id
                HAVING COUNT(*) > 1
                """
            )
        ).all()

        for row in duplicate_groups:
            person_ids = list(row.person_ids)
            if not person_ids:
                continue
            keep_person_id = person_ids[0]
            entities = session.exec(
                select(UnifiedEntity).where(UnifiedEntity.person_id.in_(person_ids))
            ).all()
            keep_entity = next((entity for entity in entities if entity.person_id == keep_person_id), None)
            if not keep_entity and entities:
                keep_entity = entities[0]
                session.exec(
                    text("UPDATE unified_entities SET person_id = :keep WHERE id = :entity_id").bindparams(
                        keep=keep_person_id, entity_id=keep_entity.id
                    )
                )
            keep_entity_id = keep_entity.id if keep_entity else None

            for person_id in person_ids[1:]:
                self._update_person_references(session, person_id, keep_person_id)

                duplicate_entities = [entity for entity in entities if entity.person_id == person_id]
                for entity in duplicate_entities:
                    if keep_entity_id is not None:
                        self._update_entity_references(session, entity.id, keep_entity_id)
                        session.exec(
                            text("DELETE FROM unified_entities WHERE id = :entity_id").bindparams(
                                entity_id=entity.id
                            )
                        )
                    else:
                        session.exec(
                            text("UPDATE unified_entities SET person_id = :keep WHERE id = :entity_id").bindparams(
                                keep=keep_person_id, entity_id=entity.id
                            )
                        )
                        keep_entity_id = entity.id

                session.exec(
                    text("DELETE FROM unified_persons WHERE id = :person_id").bindparams(person_id=person_id)
                )

            keep_person = session.get(UnifiedPerson, keep_person_id)
            if keep_person:
                key = self._person_key(keep_person)
                if key:
                    self.person_cache[key] = keep_person

    
    def _cache_entity(self, entity):
        key = self._entity_key(entity)
        if not key:
            return entity
        cached = self.entity_cache.get(key)
        if cached:
            if self.active_state and getattr(cached, "state_id", None) is None:
                cached.state_id = self.active_state.id
            return cached
        if not entity.normalized_name:
            entity.normalized_name = key[1]
        if self.active_state and getattr(entity, "state_id", None) is None:
            entity.state_id = self.active_state.id
        self.entity_cache[key] = entity
        return entity
    
    def _cache_campaign(self, campaign):
        key = self._campaign_key(campaign)
        if not key:
            return campaign
        cached = self.campaign_cache.get(key)
        if cached:
            if self.active_state and getattr(cached, "state_id", None) is None:
                cached.state_id = self.active_state.id
            return cached
        if not campaign.normalized_name:
            campaign.normalized_name = key[0]
        if self.active_state and getattr(campaign, "state_id", None) is None:
            campaign.state_id = self.active_state.id
        self.campaign_cache[key] = campaign
        return campaign
    
    def _ensure_address(self, session, address):
        if not address:
            return None
        key = self._address_key(address)
        if key in self.address_cache:
            return self.address_cache[key]
        session.add(address)
        session.flush()
        self.address_cache[key] = address
        return address
    
    def _ensure_committee(self, session, committee: UnifiedCommittee) -> UnifiedCommittee:
        if not committee or not getattr(committee, "filer_id", None):
            return committee
        filer_id = committee.filer_id
        existing = self.committee_cache.get(filer_id)
        if not existing:
            existing = session.get(UnifiedCommittee, filer_id)
            if existing:
                self.committee_cache[filer_id] = existing
        if existing:
            if self.active_state and getattr(existing, "state_id", None) is None:
                existing.state_id = self.active_state.id
            if committee.address and not existing.address:
                original_address = committee.address
                ensured_address = self._ensure_address(session, original_address)
                existing.address = ensured_address
                existing.address_id = ensured_address.id
                if existing.entity and ensured_address:
                    existing.entity.address = ensured_address
                    existing.entity.address_id = ensured_address.id
                    if self.active_state and getattr(existing.entity, "state_id", None) is None:
                        existing.entity.state_id = self.active_state.id
                if original_address is not ensured_address and original_address is not None:
                    try:
                        session.expunge(original_address)
                    except InvalidRequestError:
                        pass
            if committee.entity and not existing.entity:
                committee.entity.committee = existing
                if committee.address and committee.entity:
                    ensured_address = self._ensure_address(session, committee.address)
                    committee.entity.address = ensured_address
                    committee.entity.address_id = ensured_address.id if ensured_address else None
                if self.active_state and getattr(committee.entity, "state_id", None) is None:
                    committee.entity.state_id = self.active_state.id
                existing.entity = committee.entity
                session.add(existing.entity)
            return existing
        if committee.address:
            original_address = committee.address
            ensured_address = self._ensure_address(session, original_address)
            committee.address = ensured_address
            committee.address_id = ensured_address.id
            if committee.entity and ensured_address:
                committee.entity.address = ensured_address
                committee.entity.address_id = ensured_address.id
                if self.active_state and getattr(committee.entity, "state_id", None) is None:
                    committee.entity.state_id = self.active_state.id
            if original_address is not ensured_address and original_address is not None:
                try:
                    session.expunge(original_address)
                except InvalidRequestError:
                    pass
        if self.active_state and getattr(committee, "state_id", None) is None:
            committee.state_id = self.active_state.id
        session.add(committee)
        session.flush()
        self.committee_cache[filer_id] = committee
        return committee
    
    def load_existing_data(self, session) -> None:
        """Pre-load existing addresses and committees for deduplication"""
        self.console.print("📋 Loading existing data for deduplication...", style="blue")
        
        # Load existing addresses
        existing_addresses = session.exec(text("SELECT id, street_1, city, state, zip_code FROM unified_addresses")).all()
        for addr in existing_addresses:
            full_addr = session.get(UnifiedAddress, addr.id)
            key = self._address_key(full_addr)
            if key[0] or key[1]:
                self.address_cache[key] = full_addr
        
        # Load existing committees
        existing_committees = session.exec(text("SELECT filer_id FROM unified_committees")).all()
        for committee in existing_committees:
            full_committee = session.get(UnifiedCommittee, committee.filer_id)
            self.committee_cache[full_committee.filer_id] = full_committee
        
        # Load existing entities
        existing_entities = session.exec(text("SELECT id FROM unified_entities")).all()
        for entity in existing_entities:
            full_entity = session.get(UnifiedEntity, entity.id)
            key = self._entity_key(full_entity)
            if key:
                self.entity_cache[key] = full_entity

        # Load existing persons
        existing_persons = session.exec(text("SELECT id FROM unified_persons")).all()
        for person_row in existing_persons:
            full_person = session.get(UnifiedPerson, person_row.id)
            key = self._person_key(full_person)
            if key:
                self.person_cache[key] = full_person
        
        # Load existing campaigns
        existing_campaigns = session.exec(text("SELECT id FROM unified_campaigns")).all()
        for campaign in existing_campaigns:
            full_campaign = session.get(UnifiedCampaign, campaign.id)
            key = self._campaign_key(full_campaign)
            if key:
                self.campaign_cache[key] = full_campaign

        # Load existing file origins
        existing_file_origins = session.exec(text("SELECT id FROM file_origins")).all()
        for file_origin in existing_file_origins:
            fo = session.get(FileOrigin, file_origin.id)
            if fo:
                self.file_origin_cache[fo.id] = fo
        
        self.console.print(f"  📍 Loaded {len(self.address_cache)} addresses", style="green")
        self.console.print(f"  🏛️ Loaded {len(self.committee_cache)} committees", style="green")
        self.console.print(f"  🧾 Loaded {len(self.entity_cache)} entities", style="green")
        self.console.print(f"  🎯 Loaded {len(self.campaign_cache)} campaigns", style="green")
    
    def process_batch(self, batch: List[Dict], session, progress_task, state: str) -> Tuple[int, int, int]:
        """Process a batch of records"""
        batch_success = 0
        batch_errors = 0
        batch_skipped = 0
        active_state = self.active_state
        if not active_state:
            raise RuntimeError("Active state is not set before processing batch.")
        
        for record in batch:
            try:
                # Ensure state metadata is present
                record['state'] = state
                
                # Process record
                transaction = unified_sql_processor.process_record(
                    record,
                    state,
                    state_id=active_state.id,
                    state_code=self.active_state_code
                )
                transaction.state_id = active_state.id
                file_origin_value = record.get("file_origin")
                file_origin_obj = self._ensure_file_origin(session, active_state, file_origin_value)
                if file_origin_obj:
                    transaction.file_origin_id = file_origin_obj.id
                    transaction.file_origin = file_origin_obj
                
                if not transaction:
                    batch_skipped += 1
                    continue
                
                final_committee = None
                if transaction.committee:
                    final_committee = self._ensure_committee(session, transaction.committee)
                    if final_committee:
                        transaction.committee = final_committee
                        transaction.committee_id = final_committee.filer_id
                        if active_state and getattr(final_committee, "state_id", None) is None:
                            final_committee.state_id = active_state.id
                
                if final_committee and transaction.contribution and transaction.contribution.recipient:
                    recipient_entity = transaction.contribution.recipient
                    if getattr(recipient_entity, "entity_type", None) == EntityType.COMMITTEE and final_committee.entity:
                        transaction.contribution.recipient = final_committee.entity
                if final_committee and transaction.loan and transaction.loan.borrower:
                    borrower_entity = transaction.loan.borrower
                    if getattr(borrower_entity, "entity_type", None) == EntityType.COMMITTEE and final_committee.entity:
                        transaction.loan.borrower = final_committee.entity
                
                if transaction.campaign:
                    if final_committee and getattr(transaction.campaign, "primary_committee", None):
                        if getattr(transaction.campaign.primary_committee, "filer_id", None) == final_committee.filer_id:
                            transaction.campaign.primary_committee = final_committee
                    if active_state and getattr(transaction.campaign, "state_id", None) is None:
                        transaction.campaign.state_id = active_state.id
                    
                    updated_memberships = []
                    for membership in list(transaction.campaign.entities):
                        membership_entity = membership.entity
                        if final_committee and getattr(membership_entity, "entity_type", None) == EntityType.COMMITTEE and final_committee.entity:
                            membership.entity = final_committee.entity
                            membership.entity_id = final_committee.entity.id
                        else:
                            membership.entity = self._cache_entity(membership_entity)
                        if active_state and getattr(membership, "state_id", None) is None:
                            membership.state_id = active_state.id
                        updated_memberships.append(membership)
                    transaction.campaign.entities = updated_memberships
                    transaction.campaign = self._cache_campaign(transaction.campaign)
                
                # Handle address deduplication for persons
                for tx_person in transaction.persons:
                    if tx_person.person and tx_person.person.address:
                        original_address = tx_person.person.address
                        ensured_address = self._ensure_address(session, original_address)
                        if ensured_address:
                            tx_person.person.address_id = ensured_address.id
                            tx_person.person.address = ensured_address
                            if tx_person.person.entity:
                                tx_person.person.entity.address = ensured_address
                                tx_person.person.entity.address_id = ensured_address.id
                        if original_address is not ensured_address and original_address is not None:
                            try:
                                session.expunge(original_address)
                            except InvalidRequestError:
                                pass
                    if tx_person.person and tx_person.person.entity:
                        cached_entity = self._cache_entity(tx_person.person.entity)
                        tx_person.person.entity = cached_entity
                        tx_person.entity = cached_entity

                    if tx_person.person:
                        original_person = tx_person.person
                        ensured_person = self._ensure_person(session, tx_person.person)
                        tx_person.person = ensured_person
                        if ensured_person and ensured_person.entity:
                            tx_person.entity = ensured_person.entity
                        elif ensured_person and tx_person.entity and ensured_person.entity is None:
                            ensured_person.entity = tx_person.entity
                            if ensured_person.address and ensured_person.entity.address is None:
                                ensured_person.entity.address = ensured_person.address
                                ensured_person.entity.address_id = ensured_person.address_id
                        if original_person is not ensured_person:
                            try:
                                session.expunge(original_person)
                            except InvalidRequestError:
                                pass
                    if active_state and getattr(tx_person, "state_id", None) is None:
                        tx_person.state_id = active_state.id
                
                if transaction.contribution:
                    if transaction.contribution.contributor:
                        transaction.contribution.contributor = self._cache_entity(transaction.contribution.contributor)
                    if transaction.contribution.recipient:
                        transaction.contribution.recipient = self._cache_entity(transaction.contribution.recipient)
                    if active_state and getattr(transaction.contribution, "state_id", None) is None:
                        transaction.contribution.state_id = active_state.id
                
                if transaction.loan:
                    if transaction.loan.lender:
                        transaction.loan.lender = self._cache_entity(transaction.loan.lender)
                    if transaction.loan.borrower:
                        transaction.loan.borrower = self._cache_entity(transaction.loan.borrower)
                    if active_state and getattr(transaction.loan, "state_id", None) is None:
                        transaction.loan.state_id = active_state.id
                
                # Save transaction
                session.add(transaction)
                batch_success += 1
                
            except Exception as e:
                batch_errors += 1
                error_info = {
                    'record': record,
                    'error': str(e),
                    'error_type': type(e).__name__
                }
                self.errors.append(error_info)
                self.failed_records.append(record)
                logger.error(f"Error processing record: {e}")
        
        # Update progress
        if progress_task:
            progress_task.advance(len(batch))
        
        return batch_success, batch_errors, batch_skipped
    
    def _get_reader(self, state_key: str) -> GenericFileReader:
        if state_key not in self.reader_cache:
            if state_key == "generic":
                schema = build_unified_schema()
            elif state_key in field_library.state_mappings:
                schema = build_schema_for_states([state_key])
            else:
                logger.warning("No field mappings registered for state '%s'; using unified schema", state_key)
                schema = build_unified_schema()
            self.reader_cache[state_key] = GenericFileReader(schema=schema, add_metadata=True, strict=False)
        return self.reader_cache[state_key]

    def load_file(self, file_path: Path, *, state: Optional[str] = None) -> LoaderStats:
        """Load data from a file using the generic reader"""
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        state_key = state or "generic"
        reader = self._get_reader(state_key)
        
        self.stats = LoaderStats()
        self.stats.start_time = time.time()
        
        # Display header
        self.console.print(Panel.fit(
            f"[bold blue]Production Campaign Finance Loader[/bold blue]\n"
            f"File: {file_path.name}\n"
            f"Batch Size: {self.config.batch_size}\n"
            f"Max Records: {self.config.max_records or 'All'}\n"
            f"Commit Frequency: Every {self.config.commit_frequency} batches",
            border_style="blue"
        ))
        
        try:
            records_iter = reader.read_records(file_path)
        except SchemaValidationError as exc:
            raise SchemaValidationError(f"Failed to read {file_path.name}: {exc}") from exc
        
        if self.config.max_records:
            records_iter = islice(records_iter, self.config.max_records)
        
        all_records: List[Dict] = []
        for record in records_iter:
            if state:
                record.setdefault("state", state)
            else:
                record.setdefault("state", state_key)
            all_records.append(record)
        
        total_records = len(all_records)
        self.stats.total_records = total_records
        
        if total_records == 0:
            self.console.print("No records found in file", style="yellow")
            return self.stats
        
        # Process in batches
        with self.db_manager.get_session() as session:
            # Load existing data
            self.address_cache.clear()
            self.committee_cache.clear()
            self.entity_cache.clear()
            self.campaign_cache.clear()
            self.person_cache.clear()
            self.file_origin_cache.clear()
            self.load_existing_data(session)
            self.active_state_slug = state_key.lower()
            self.active_state = self._ensure_state(session, state_key)
            self.active_state_code = self.active_state.code
            
            if self.config.enable_progress:
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TaskProgressColumn(),
                    TimeElapsedColumn(),
                    console=self.console
                ) as progress:
                    task = progress.add_task("Processing records...", total=total_records)
                    for i in range(0, total_records, self.config.batch_size):
                        batch = all_records[i:i + self.config.batch_size]
                        batch_num = (i // self.config.batch_size) + 1
                        success, errors, skipped = self.process_batch(batch, session, task, state_key)
                        self.stats.successful_records += success
                        self.stats.failed_records += errors
                        self.stats.skipped_records += skipped
                        if batch_num % self.config.commit_frequency == 0:
                            session.commit()
                            self.console.print(f"✅ Committed batch {batch_num}", style="green")
                        if self.config.enable_logging:
                            logger.info(f"Batch {batch_num}: {success} success, {errors} errors, {skipped} skipped")
                    self._dedupe_addresses(session)
                    self._dedupe_persons_and_entities(session)
                    session.commit()
            else:
                for i in range(0, total_records, self.config.batch_size):
                    batch = all_records[i:i + self.config.batch_size]
                    batch_num = (i // self.config.batch_size) + 1
                    success, errors, skipped = self.process_batch(batch, session, None, state_key)
                    self.stats.successful_records += success
                    self.stats.failed_records += errors
                    self.stats.skipped_records += skipped
                    if batch_num % self.config.commit_frequency == 0:
                        session.commit()
                        self.console.print(f"✅ Committed batch {batch_num}", style="green")
                self._dedupe_addresses(session)
                self._dedupe_persons_and_entities(session)
                session.commit()
            self.active_state = None
            self.active_state_code = None
            self.active_state_slug = None
        
        self.stats.end_time = time.time()
        self.display_results()
        return self.stats
    
    def display_results(self) -> None:
        """Display comprehensive results"""
        # Create results table
        table = Table(title="📊 Loader Results", show_header=True, header_style="bold blue")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="green")
        
        table.add_row("Total Records", str(self.stats.total_records))
        table.add_row("Successful", str(self.stats.successful_records))
        table.add_row("Failed", str(self.stats.failed_records))
        table.add_row("Skipped", str(self.stats.skipped_records))
        table.add_row("Success Rate", f"{self.stats.success_rate:.1f}%")
        table.add_row("Duration", f"{self.stats.duration:.2f}s")
        table.add_row("Records/Second", f"{self.stats.records_per_second:.1f}")
        table.add_row("Address Cache", str(len(self.address_cache)))
        table.add_row("Committee Cache", str(len(self.committee_cache)))
        
        self.console.print(table)
        
        # Display errors if any
        if self.errors:
            error_table = Table(title="❌ Errors Summary", show_header=True, header_style="bold red")
            error_table.add_column("Error Type", style="red")
            error_table.add_column("Count", style="red")
            
            error_counts = {}
            for error in self.errors:
                error_type = error['error_type']
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
            
            for error_type, count in error_counts.items():
                error_table.add_row(error_type, str(count))
            
            self.console.print(error_table)
        
        # Display database summary
        self.display_database_summary()
    
    def display_database_summary(self) -> None:
        """Display current database state"""
        with self.db_manager.get_session() as session:
            from sqlalchemy import text
            
            tx_count = session.exec(text("SELECT COUNT(*) FROM unified_transactions")).first()
            committee_count = session.exec(text("SELECT COUNT(*) FROM unified_committees")).first()
            address_count = session.exec(text("SELECT COUNT(*) FROM unified_addresses")).first()
            
            db_table = Table(title="🗄️ Database Summary", show_header=True, header_style="bold blue")
            db_table.add_column("Table", style="cyan")
            db_table.add_column("Count", style="green")
            
            db_table.add_row("Transactions", str(tx_count))
            db_table.add_row("Committees", str(committee_count))
            db_table.add_row("Addresses", str(address_count))
            
            self.console.print(db_table)

def main():
    """Main function"""
    import sys
    
    # Get preset from command line argument or use default
    preset = sys.argv[1] if len(sys.argv) > 1 else "testing"
    file_key = sys.argv[2] if len(sys.argv) > 2 else "oklahoma_2020"
    
    try:
        # Get configuration
        config = get_config(preset)
        file_config = get_file_config(file_key)
        
        rprint(f"🔧 Using preset: {preset}")
        rprint(f"📁 Loading file: {file_config['description']}")
        
        # Create loader
        loader = ProductionLoader(config)
        
        # Load file
        file_path = file_config['file_path']
        state = file_config.get('state')
        
        stats = loader.load_file(file_path, state=state)
        rprint(f"\n🎉 Load completed successfully!")
        rprint(f"📈 Performance: {stats.records_per_second:.1f} records/second")
        rprint(f"✅ Success Rate: {stats.success_rate:.1f}%")
        
    except Exception as e:
        rprint(f"\n❌ Load failed: {e}")
        logger.error(f"Load failed: {e}", exc_info=True)
        
        # Show usage if argument error
        if "Unknown preset" in str(e) or "Unknown file key" in str(e):
            rprint(f"\n📖 Usage: python production_loader.py [preset] [file_key]")
            rprint(f"📋 Available presets: development, testing, production, high_performance, safe")
            rprint(f"📁 Available files: oklahoma_2020, oklahoma_2021, texas_sample")

if __name__ == "__main__":
    main() 