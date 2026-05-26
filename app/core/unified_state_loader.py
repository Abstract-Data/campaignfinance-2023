#!/usr/bin/env python3
"""
Unified State Loader - Comprehensive pipeline for loading state campaign finance data
into the unified database with automatic relationship linking.
"""

from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, select, update

from app.core.enums import CommitteeRole
from app.core.models import (
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedCommitteePerson,
    UnifiedPerson,
    UnifiedTransaction,
    UnifiedTransactionPerson,
)
from app.core.processor import ProcessStats, unified_sql_processor
from app.funcs.csv_reader import FileReader
from app.logger import Logger

from .unified_database import UnifiedDatabaseManager, get_db_manager

logger = Logger(__name__)

CommitteeIndex = dict[str, str]
PersonIndex = dict[tuple[str, str], int]


def _load_committee_index(session: Session, state_id: int | None) -> CommitteeIndex:
    """Return ``{committee_name_lower: filer_id}`` for committees in the state."""
    if state_id is None:
        return {}
    rows = session.exec(
        select(UnifiedCommittee.name, UnifiedCommittee.filer_id).where(
            UnifiedCommittee.state_id == state_id
        )
    ).all()
    return {(name or "").lower(): filer_id for name, filer_id in rows if name}


def _load_person_index(session: Session, state_id: int | None) -> PersonIndex:
    """Return ``{(first_lower, last_lower): person_id}`` for persons in the state."""
    if state_id is None:
        return {}
    rows = session.exec(
        select(
            UnifiedPerson.first_name,
            UnifiedPerson.last_name,
            UnifiedPerson.id,
        ).where(UnifiedPerson.state_id == state_id)
    ).all()
    index: PersonIndex = {}
    for first, last, person_id in rows:
        if first and last and person_id is not None:
            index[(first.lower(), last.lower())] = person_id
    return index


class UnifiedStateLoader:
    """
    Comprehensive loader for state campaign finance data that handles:
    - File reading and parsing
    - Model creation and database storage
    - Committee-person relationship establishment
    - Transaction-to-officer linking
    - Cross-reference validation
    """

    def __init__(
        self,
        state: str,
        data_directory: Path,
        *,
        db_manager: UnifiedDatabaseManager | None = None,
    ):
        self.state = state.lower()
        self.data_directory = Path(data_directory)
        self.state_data_dir = self.data_directory / self.state
        self._db_manager = db_manager or get_db_manager()

        # Track processing statistics
        self.stats = {
            "files_processed": 0,
            "transactions_created": 0,
            "persons_created": 0,
            "committees_created": 0,
            "addresses_created": 0,
            "committee_relationships_created": 0,
            "transaction_links_created": 0,
            "errors": []
        }

        # Cache for deduplication and relationship building
        self.person_cache = {}  # name -> UnifiedPerson
        self.committee_cache = {}  # filer_id -> UnifiedCommittee
        self.address_cache = {}  # address_key -> UnifiedAddress

        # Committee officer mappings from state data
        self.committee_officers = {}  # committee_id -> List[officer_data]

    def load_state_data(
        self,
        auto_link_officers: bool = True,
        create_relationships: bool = True,
        progress_callback: Callable[..., Any] | None = None,
        max_records: int | None = None,
    ) -> dict[str, Any]:
        """
        Main pipeline to load all state data with automatic relationship linking.

        Args:
            auto_link_officers: Whether to automatically link transactions to committee officers
            create_relationships: Whether to create committee-person relationships
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with processing statistics and results
        """
        logger.info(f"Starting unified data load for {self.state.upper()}")

        try:
            # Step 1: Discover and validate data files
            files = self._discover_data_files()
            if not files:
                raise ValueError(f"No data files found in {self.state_data_dir}")

            logger.info(f"Found {len(files)} data files to process")

            # Step 2: Extract committee officer information first
            if create_relationships:
                self._extract_committee_officers(files)

            # Step 3: Process all data files
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                console=None
            ) as progress:

                main_task = progress.add_task(f"Processing {self.state.upper()} data...", total=len(files))

                for file_path in files:
                    try:
                        progress.update(main_task, description=f"Processing {file_path.name}")

                        # Process the file
                        file_stats = self._process_data_file(file_path, auto_link_officers, max_records)

                        # Update progress
                        progress.advance(main_task)

                        if progress_callback:
                            progress_callback(file_stats)

                    except (OSError, ValueError, SQLAlchemyError) as e:
                        error_msg = f"Error processing {file_path}: {str(e)}"
                        self.stats["errors"].append(error_msg)
                        logger.error(error_msg)
                        continue

            # Step 4: Create committee-person relationships
            if create_relationships:
                self._create_committee_relationships()

            # Step 5: Auto-link transactions to officers
            if auto_link_officers:
                self._auto_link_all_transactions()

            # Step 6: Generate summary report
            summary = self._generate_summary_report()

            logger.info(f"Completed unified data load for {self.state.upper()}")
            logger.info(f"Summary: {summary}")

            return summary

        except (OSError, ValueError, SQLAlchemyError) as e:
            error_msg = f"Fatal error loading {self.state} data: {str(e)}"
            self.stats["errors"].append(error_msg)
            logger.error(error_msg)
            raise

    def _discover_data_files(self) -> list[Path]:
        """Discover all data files for the state."""
        files = []

        # Look for common file patterns
        patterns = [
            "*.parquet",
            "*.csv",
            "contributions*.parquet",
            "expenditures*.parquet",
            "committees*.parquet",
            "candidates*.parquet"
        ]

        for pattern in patterns:
            files.extend(self.state_data_dir.glob(pattern))

        # Remove duplicates and sort
        files = list(set(files))
        files.sort()

        return files

    def _extract_committee_officers(self, files: list[Path]) -> None:
        """Extract committee officer information from state data files."""
        logger.info("Extracting committee officer information...")

        # Look for committee/officer files
        officer_files = [f for f in files if any(keyword in f.name.lower()
                                                for keyword in ['committee', 'officer', 'filer', 'candidate'])]

        for file_path in officer_files:
            try:
                # Read the file
                file_reader = FileReader()
                if file_path.suffix.lower() == '.parquet':
                    data_generator = file_reader.read_parquet(file_path)
                else:
                    data_generator = file_reader.read_csv(file_path)

                # Extract officer information based on state-specific field mappings
                for record in data_generator:
                    officer_data = self._extract_officer_from_record(record)
                    if officer_data:
                        committee_id = officer_data.get('committee_id')
                        if committee_id:
                            if committee_id not in self.committee_officers:
                                self.committee_officers[committee_id] = []
                            self.committee_officers[committee_id].append(officer_data)

            except (OSError, ValueError, KeyError) as e:
                logger.error(f"Error extracting officers from {file_path}: {e}")
                continue

    def _extract_officer_from_record(self, record: dict[str, Any]) -> dict[str, Any] | None:
        """Extract officer information from a data record based on state-specific mappings."""

        # State-specific field mappings for officer extraction
        state_mappings = {
            'texas': {
                'treasurer_name': ['treasurer_name', 'treasurer', 'treasurer_first_name', 'treasurer_last_name'],
                'chair_name': ['chair_name', 'chair', 'chair_first_name', 'chair_last_name'],
                'committee_id': ['filer_id', 'committee_id', 'filer_number'],
                'committee_name': ['committee_name', 'filer_name', 'committee_title']
            },
            'oklahoma': {
                'treasurer_name': ['treasurer_name', 'treasurer'],
                'chair_name': ['chair_name', 'chair'],
                'committee_id': ['committee_id', 'filer_id'],
                'committee_name': ['committee_name', 'committee_title']
            }
        }

        mapping = state_mappings.get(self.state, {})

        # Extract committee information
        committee_id = None
        for field in mapping.get('committee_id', []):
            if field in record and record[field]:
                committee_id = str(record[field])
                break

        if not committee_id:
            return None

        # Extract officer information
        officers = []

        # Check for treasurer
        treasurer_name = None
        for field in mapping.get('treasurer_name', []):
            if field in record and record[field]:
                treasurer_name = str(record[field]).strip()
                break

        if treasurer_name:
            officers.append({
                'name': treasurer_name,
                'role': CommitteeRole.TREASURER,
                'committee_id': committee_id
            })

        # Check for chair
        chair_name = None
        for field in mapping.get('chair_name', []):
            if field in record and record[field]:
                chair_name = str(record[field]).strip()
                break

        if chair_name:
            officers.append({
                'name': chair_name,
                'role': CommitteeRole.CHAIR,
                'committee_id': committee_id
            })

        return {
            'committee_id': committee_id,
            'committee_name': self._extract_field(record, mapping.get('committee_name', [])),
            'officers': officers
        }

    def _extract_field(self, record: dict[str, Any], field_names: list[str]) -> str | None:
        """Extract a field value from a record using multiple possible field names."""
        for field in field_names:
            if field in record and record[field]:
                return str(record[field]).strip()
        return None

    def _process_data_file(
        self,
        file_path: Path,
        auto_link_officers: bool,
        max_records: int | None = None,
    ) -> dict[str, Any]:
        """Process a single data file and create unified models."""
        file_stats: dict[str, Any] = {
            "file": file_path.name,
            "transactions": 0,
            "persons": 0,
            "committees": 0,
            "addresses": 0,
            "errors": [],
        }

        try:
            file_reader = FileReader()
            if file_path.suffix.lower() == ".parquet":
                data_generator = file_reader.read_parquet(file_path)
            else:
                data_generator = file_reader.read_csv(file_path)

            records: list[dict[str, Any]] = []
            for record in data_generator:
                records.append(record)
                if max_records and len(records) >= max_records:
                    break

            batch_stats = self.process_records_batch(
                records,
                file_path=file_path,
                auto_link_officers=auto_link_officers,
            )
            file_stats["transactions"] = batch_stats.success
            if batch_stats.failures or batch_stats.db_errors:
                file_stats["errors"].append(str(batch_stats))

            self.stats["files_processed"] += 1
            self.stats["transactions_created"] += batch_stats.success
            return file_stats

        except (OSError, ValueError, SQLAlchemyError) as e:
            error_msg = f"Error processing file {file_path}: {e}"
            file_stats["errors"].append(error_msg)
            self.stats["errors"].append(error_msg)
            return file_stats

    def _load_batch_indexes(
        self, session: Session
    ) -> tuple[CommitteeIndex, PersonIndex, int | None, str | None]:
        """Resolve state id and pre-load committee/person lookup dicts for a batch."""
        state_record = self._db_manager._resolve_state_record(session, self.state)
        if state_record is None:
            return {}, {}, None, None
        state_id = state_record.id
        return (
            _load_committee_index(session, state_id),
            _load_person_index(session, state_id),
            state_id,
            state_record.code,
        )

    def process_records_batch(
        self,
        records: list[dict[str, Any]],
        *,
        file_path: Path | None = None,
        auto_link_officers: bool = False,
    ) -> ProcessStats:
        """Process records under a single DB session; return per-batch counters."""
        stats = ProcessStats()
        if not records:
            return stats

        file_name = file_path.name if file_path else "batch"

        with self._db_manager.get_session() as session:
            _committees, _persons, state_id, state_code = self._load_batch_indexes(session)
            if state_id is None:
                msg = (
                    f"State '{self.state}' is not present in the states table; "
                    "cannot load records with NULL state_id"
                )
                logger.error(msg)
                raise ValueError(msg)

            try:
                for record in records:
                    record = dict(record)
                    record["state"] = self.state
                    record["file_origin"] = file_name
                    try:
                        transaction = self._persist_transaction_from_record(
                            record,
                            session,
                            state_id=state_id,
                            state_code=state_code,
                        )
                        if transaction is None:
                            stats.skipped += 1
                            continue
                        stats.success += 1
                        if auto_link_officers:
                            self._link_transaction_to_officers(
                                transaction, record, session
                            )
                    except (ValidationError, KeyError, ValueError) as exc:
                        logger.error(f"Record failed: {exc} — {record!r}")
                        stats.failures += 1
                    except SQLAlchemyError as exc:
                        logger.error(f"DB error on record: {exc}")
                        session.rollback()
                        stats.db_errors += 1
                        return stats

                session.commit()
            except SQLAlchemyError as exc:
                logger.error(f"DB error committing batch: {exc}")
                session.rollback()
                stats.db_errors += 1

        return stats

    def _persist_transaction_from_record(
        self,
        record: dict[str, Any],
        session: Session,
        *,
        state_id: int | None,
        state_code: str | None,
    ) -> UnifiedTransaction | None:
        """Build and persist one transaction using the batch session."""
        transaction = unified_sql_processor.process_record(
            record,
            self.state,
            state_id=state_id,
            state_code=state_code,
            session=session,
        )

        if transaction.committee:
            filer_id = transaction.committee.filer_id
            existing_committee = session.get(UnifiedCommittee, filer_id)
            if existing_committee is not None:
                transaction.committee = existing_committee
                transaction.committee_id = filer_id
            else:
                session.add(transaction.committee)

        for tx_person in transaction.persons:
            if tx_person.person and tx_person.person.address:
                existing_address = session.exec(
                    select(UnifiedAddress).where(
                        UnifiedAddress.street_1 == tx_person.person.address.street_1,
                        UnifiedAddress.city == tx_person.person.address.city,
                        UnifiedAddress.state == tx_person.person.address.state,
                        UnifiedAddress.zip_code == tx_person.person.address.zip_code,
                    )
                ).first()

                if existing_address:
                    tx_person.person.address_id = existing_address.id
                    tx_person.person.address = existing_address
                else:
                    session.add(tx_person.person.address)
                    session.flush()

        session.add(transaction)
        session.flush()
        return transaction

    def _link_transaction_to_officers(
        self,
        transaction: UnifiedTransaction,
        record: dict[str, Any],
        session: Session,
    ) -> None:
        """Link a transaction to committee officers if applicable."""
        _ = record
        try:
            committee_id = transaction.committee_id
            if not committee_id:
                return

            committee_officers = self.committee_officers.get(str(committee_id), [])
            tx_persons = session.exec(
                select(UnifiedTransactionPerson).where(
                    UnifiedTransactionPerson.transaction_id == transaction.id
                )
            ).all()

            for tx_person in tx_persons:
                for officer_data in committee_officers:
                    for officer in officer_data.get("officers", []):
                        if self._person_matches_officer(
                            tx_person.person_id, officer, session
                        ):
                            self._create_officer_link(
                                tx_person.id, officer, committee_id, session
                            )
                            self.stats["transaction_links_created"] += 1
                            break

        except (SQLAlchemyError, KeyError) as e:
            logger.error(f"Error linking transaction to officers: {e}")

    def _person_matches_officer(
        self, person_id: int, officer: dict[str, Any], session: Session
    ) -> bool:
        """Check if a person matches an officer based on name."""
        try:
            person = session.get(UnifiedPerson, person_id)
            if not person:
                return False

            person_name = f"{person.first_name} {person.last_name}".lower().strip()
            officer_name = officer["name"].lower().strip()
            return person_name == officer_name

        except (SQLAlchemyError, KeyError) as e:
            logger.error(f"Error matching person to officer: {e}")
            return False

    def _create_officer_link(
        self,
        tx_person_id: int,
        officer: dict[str, Any],
        committee_id: str | int,
        session: Session,
    ) -> None:
        """Create a link between a transaction-person relationship and an officer role."""
        try:
            committee_person = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.committee_id == str(committee_id),
                    UnifiedCommitteePerson.role == officer['role'],
                )
            ).first()

            if committee_person:
                session.exec(
                    update(UnifiedTransactionPerson)
                    .where(UnifiedTransactionPerson.id == tx_person_id)
                    .values(committee_person_id=committee_person.id)
                )
                session.flush()

        except (SQLAlchemyError, KeyError) as e:
            logger.error(f"Error creating officer link: {e}")

    def _create_committee_relationships(self) -> None:
        """Create committee-person relationships from extracted officer data."""
        logger.info("Creating committee-person relationships...")

        with self._db_manager.get_session() as session:
            _, persons, state_id, _state_code = self._load_batch_indexes(session)
            if state_id is None:
                msg = (
                    f"State '{self.state}' is not present in the states table; "
                    "cannot create committee relationships"
                )
                logger.error(msg)
                raise ValueError(msg)

            for committee_id, officers_data in self.committee_officers.items():
                try:
                    for officer_data in officers_data:
                        for officer in officer_data.get("officers", []):
                            person = self._find_or_create_person(
                                officer["name"], session, persons, state_id=state_id
                            )
                            if person:
                                self._db_manager.add_person_to_committee(
                                    person_id=person.id,
                                    committee_id=str(committee_id),
                                    role=officer["role"],
                                    start_date=date.today(),
                                    user="state_loader",
                                    notes=f"Auto-created from {self.state} data",
                                    session=session,
                                )
                                self.stats["committee_relationships_created"] += 1

                except (SQLAlchemyError, KeyError, ValueError) as e:
                    logger.error(
                        f"Error creating committee relationship for {committee_id}: {e}"
                    )
                    continue

            session.commit()

    def _find_or_create_person(
        self,
        name: str,
        session: Session,
        persons: PersonIndex,
        *,
        state_id: int | None = None,
    ) -> UnifiedPerson | None:
        """Find or create a person by name using the batch session and person index."""
        try:
            name_parts = name.strip().split()
            if len(name_parts) < 2:
                return None

            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])
            cache_key = f"{first_name}_{last_name}".lower()
            if cache_key in self.person_cache:
                return self.person_cache[cache_key]

            index_key = (first_name.lower(), last_name.lower())
            person_id = persons.get(index_key)
            person = session.get(UnifiedPerson, person_id) if person_id else None

            if not person:
                person = session.exec(
                    select(UnifiedPerson).where(
                        UnifiedPerson.first_name.ilike(first_name),
                        UnifiedPerson.last_name.ilike(last_name),
                    )
                ).first()

            if not person:
                person = UnifiedPerson(
                    first_name=first_name,
                    last_name=last_name,
                    person_type="individual",
                    state_id=state_id,
                )
                session.add(person)
                session.flush()
                self.stats["persons_created"] += 1
                if person.id is not None:
                    persons[index_key] = person.id

            self.person_cache[cache_key] = person
            return person

        except (SQLAlchemyError, KeyError, ValueError) as e:
            logger.error(f"Error finding/creating person {name}: {e}")
            return None

    def _auto_link_all_transactions(self):
        """Auto-link all transactions to committee officers."""
        logger.info("Auto-linking transactions to committee officers...")

        # Get all committees that have officers
        for committee_id in self.committee_officers.keys():
            try:
                linked_counts = self._db_manager.auto_link_transactions_to_committee_roles(
                    str(committee_id)
                )
                self.stats["transaction_links_created"] += linked_counts["total"]

            except (SQLAlchemyError, KeyError, ValueError) as e:
                logger.error(f"Error auto-linking transactions for committee {committee_id}: {e}")
                continue

    def _generate_summary_report(self) -> dict[str, Any]:
        """Generate a comprehensive summary report."""
        return {
            "state": self.state.upper(),
            "timestamp": datetime.now().isoformat(),
            "statistics": self.stats.copy(),
            "summary": {
                "total_files_processed": self.stats["files_processed"],
                "total_transactions": self.stats["transactions_created"],
                "total_persons": self.stats["persons_created"],
                "total_committees": self.stats["committees_created"],
                "total_relationships": self.stats["committee_relationships_created"],
                "total_links": self.stats["transaction_links_created"],
                "error_count": len(self.stats["errors"])
            },
            "errors": self.stats["errors"][:10]  # First 10 errors
        }


def load_state_data(
    state: str,
    data_directory: Path,
    auto_link_officers: bool = True,
    create_relationships: bool = True,
    progress_callback: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    """
    Convenience function to load state data with a single call.

    Args:
        state: State name (e.g., 'texas', 'oklahoma')
        data_directory: Directory containing state data folders
        auto_link_officers: Whether to automatically link transactions to officers
        create_relationships: Whether to create committee-person relationships
        progress_callback: Optional callback for progress updates

    Returns:
        Summary report of the loading process
    """
    loader = UnifiedStateLoader(state, data_directory)
    return loader.load_state_data(
        auto_link_officers=auto_link_officers,
        create_relationships=create_relationships,
        progress_callback=progress_callback
    )
