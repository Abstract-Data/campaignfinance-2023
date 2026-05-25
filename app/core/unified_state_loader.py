#!/usr/bin/env python3
"""
Unified State Loader - Comprehensive pipeline for loading state campaign finance data
into the unified database with automatic relationship linking.
"""

from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import select, update

from app.funcs.csv_reader import FileReader
from app.logger import Logger

from .unified_database import db_manager
from .unified_sqlmodels import (
    CommitteeRole,
    UnifiedAddress,
    UnifiedCommitteePerson,
    UnifiedPerson,
    UnifiedTransaction,
    UnifiedTransactionPerson,
    unified_sql_processor,
)

logger = Logger(__name__)


class UnifiedStateLoader:
    """
    Comprehensive loader for state campaign finance data that handles:
    - File reading and parsing
    - Model creation and database storage
    - Committee-person relationship establishment
    - Transaction-to-officer linking
    - Cross-reference validation
    """

    def __init__(self, state: str, data_directory: Path):
        self.state = state.lower()
        self.data_directory = Path(data_directory)
        self.state_data_dir = self.data_directory / self.state

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

    def load_state_data(self,
                       auto_link_officers: bool = True,
                       create_relationships: bool = True,
                       progress_callback: Optional[callable] = None,
                       max_records: Optional[int] = None) -> Dict[str, Any]:
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

    def _discover_data_files(self) -> List[Path]:
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

    def _extract_committee_officers(self, files: List[Path]):
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

    def _extract_officer_from_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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

    def _extract_field(self, record: Dict[str, Any], field_names: List[str]) -> Optional[str]:
        """Extract a field value from a record using multiple possible field names."""
        for field in field_names:
            if field in record and record[field]:
                return str(record[field]).strip()
        return None

    def _process_data_file(self, file_path: Path, auto_link_officers: bool, max_records: Optional[int] = None) -> Dict[str, Any]:
        """Process a single data file and create unified models."""
        file_stats = {
            "file": file_path.name,
            "transactions": 0,
            "persons": 0,
            "committees": 0,
            "addresses": 0,
            "errors": []
        }

        try:
            # Read the file
            file_reader = FileReader()
            if file_path.suffix.lower() == '.parquet':
                data_generator = file_reader.read_parquet(file_path)
            else:
                data_generator = file_reader.read_csv(file_path)

            # Process each record
            records_processed = 0
            for record in data_generator:
                try:
                    # Add state and file origin information
                    record['state'] = self.state
                    record['file_origin'] = file_path.name

                    # Create unified models
                    transaction = self._create_transaction_from_record(record)
                    if transaction:
                        file_stats["transactions"] += 1

                        # Auto-link to officers if enabled
                        if auto_link_officers:
                            self._link_transaction_to_officers(transaction, record)

                    records_processed += 1

                    # Check if we've hit the record limit
                    if max_records and records_processed >= max_records:
                        break

                except (ValueError, KeyError, SQLAlchemyError) as e:
                    error_msg = f"Error processing record in {file_path.name}: {str(e)}"
                    file_stats["errors"].append(error_msg)
                    continue

            # Update global stats
            self.stats["files_processed"] += 1
            self.stats["transactions_created"] += file_stats["transactions"]

            return file_stats

        except (OSError, ValueError, SQLAlchemyError) as e:
            error_msg = f"Error processing file {file_path}: {str(e)}"
            file_stats["errors"].append(error_msg)
            self.stats["errors"].append(error_msg)
            return file_stats

    def _create_transaction_from_record(self, record: Dict[str, Any]) -> Optional[UnifiedTransaction]:
        """Create a unified transaction from a state-specific record."""
        try:
            # Use the unified processor to create the transaction
            transaction = unified_sql_processor.process_record(record, self.state)

            # Save everything in one session
            with db_manager.get_session() as session:
                # Save committee first if it exists
                if transaction.committee_id:
                    committee = unified_sql_processor.get_builder(self.state).build_committee(record)
                    if committee:
                        # Use merge to handle existing committees automatically
                        session.merge(committee)
                        session.flush()  # Flush to ensure committee is available

                # Save addresses for all persons in the transaction
                for tx_person in transaction.persons:
                    if tx_person.person and tx_person.person.address:
                        # Check if address already exists in session
                        existing_address = session.exec(
                            select(UnifiedAddress).where(
                                UnifiedAddress.street_1 == tx_person.person.address.street_1,
                                UnifiedAddress.city == tx_person.person.address.city,
                                UnifiedAddress.state == tx_person.person.address.state,
                                UnifiedAddress.zip_code == tx_person.person.address.zip_code
                            )
                        ).first()

                        if existing_address:
                            # Use existing address
                            tx_person.person.address_id = existing_address.id
                            tx_person.person.address = existing_address
                        else:
                            # Save new address
                            session.add(tx_person.person.address)
                            session.flush()  # Flush to get the ID

                # Save the transaction
                session.add(transaction)
                session.commit()
                session.refresh(transaction)

            return transaction

        except (SQLAlchemyError, ValueError, KeyError) as e:
            logger.error(f"Error creating transaction: {e}")
            return None

    def _link_transaction_to_officers(self, transaction: UnifiedTransaction, record: Dict[str, Any]):
        """Link a transaction to committee officers if applicable."""
        try:
            # Get the committee ID from the transaction
            committee_id = transaction.committee_id
            if not committee_id:
                return

            # Check if we have officer information for this committee
            committee_officers = self.committee_officers.get(str(committee_id), [])

            # Get transaction-person relationships for this transaction
            # Parameterized SQLModel query — no f-string interpolation (P1-SEC-001 / RF-ARCH-001).
            with db_manager.get_session() as session:
                tx_persons = session.exec(
                    select(UnifiedTransactionPerson).where(
                        UnifiedTransactionPerson.transaction_id == transaction.id
                    )
                ).all()

                for tx_person in tx_persons:
                    # Check if this person is an officer for this committee
                    for officer_data in committee_officers:
                        for officer in officer_data.get('officers', []):
                            if self._person_matches_officer(tx_person.person_id, officer):
                                # Link the transaction to this officer role
                                self._create_officer_link(tx_person.id, officer, committee_id)
                                self.stats["transaction_links_created"] += 1
                                break

        except (SQLAlchemyError, KeyError) as e:
            logger.error(f"Error linking transaction to officers: {e}")

    def _person_matches_officer(self, person_id: int, officer: Dict[str, Any]) -> bool:
        """Check if a person matches an officer based on name."""
        try:
            with db_manager.get_session() as session:
                person = session.get(UnifiedPerson, person_id)
                if not person:
                    return False

                person_name = f"{person.first_name} {person.last_name}".lower().strip()
                officer_name = officer['name'].lower().strip()

                return person_name == officer_name

        except (SQLAlchemyError, KeyError):
            return False

    def _create_officer_link(self, tx_person_id: int, officer: Dict[str, Any], committee_id: int):
        """Create a link between a transaction-person relationship and an officer role."""
        try:
            # Find the committee-person relationship — parameterized (P1-SEC-001).
            with db_manager.get_session() as session:
                committee_person = session.exec(
                    select(UnifiedCommitteePerson).where(
                        UnifiedCommitteePerson.committee_id == committee_id,
                        UnifiedCommitteePerson.role == officer['role']
                    )
                ).first()

                if committee_person:
                    # Update the transaction-person relationship — parameterized.
                    session.exec(
                        update(UnifiedTransactionPerson)
                        .where(UnifiedTransactionPerson.id == tx_person_id)
                        .values(committee_person_id=committee_person.id)
                    )
                    session.commit()

        except (SQLAlchemyError, KeyError) as e:
            logger.error(f"Error creating officer link: {e}")

    def _create_committee_relationships(self):
        """Create committee-person relationships from extracted officer data."""
        logger.info("Creating committee-person relationships...")

        for committee_id, officers_data in self.committee_officers.items():
            try:
                for officer_data in officers_data:
                    for officer in officer_data.get('officers', []):
                        # Find or create the person
                        person = self._find_or_create_person(officer['name'])
                        if person:
                            # Add person to committee
                            db_manager.add_person_to_committee(
                                person_id=person.id,
                                committee_id=int(committee_id),
                                role=officer['role'],
                                start_date=date.today(),  # Default to today
                                user="state_loader",
                                notes=f"Auto-created from {self.state} data"
                            )
                            self.stats["committee_relationships_created"] += 1

            except (SQLAlchemyError, KeyError, ValueError) as e:
                logger.error(f"Error creating committee relationship for {committee_id}: {e}")
                continue

    def _find_or_create_person(self, name: str) -> Optional[UnifiedPerson]:
        """Find or create a person by name."""
        try:
            # Parse name into first and last
            name_parts = name.strip().split()
            if len(name_parts) < 2:
                return None

            first_name = name_parts[0]
            last_name = " ".join(name_parts[1:])

            # Check cache first
            cache_key = f"{first_name}_{last_name}".lower()
            if cache_key in self.person_cache:
                return self.person_cache[cache_key]

            # Check database — parameterized query, case-insensitive comparison via ilike
            # (replaces line-491 bare-string crash + injection vector — P1-SEC-001).
            with db_manager.get_session() as session:
                person = session.exec(
                    select(UnifiedPerson).where(
                        UnifiedPerson.first_name.ilike(first_name),
                        UnifiedPerson.last_name.ilike(last_name),
                    )
                ).first()

                if not person:
                    # Create new person
                    person = UnifiedPerson(
                        first_name=first_name,
                        last_name=last_name,
                        person_type="individual"
                    )
                    session.add(person)
                    session.commit()
                    session.refresh(person)
                    self.stats["persons_created"] += 1

                # Cache the person
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
                linked_counts = db_manager.auto_link_transactions_to_committee_roles(int(committee_id))
                self.stats["transaction_links_created"] += linked_counts["total"]

            except (SQLAlchemyError, KeyError, ValueError) as e:
                logger.error(f"Error auto-linking transactions for committee {committee_id}: {e}")
                continue

    def _generate_summary_report(self) -> Dict[str, Any]:
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


def load_state_data(state: str,
                   data_directory: Path,
                   auto_link_officers: bool = True,
                   create_relationships: bool = True,
                   progress_callback: Optional[callable] = None) -> Dict[str, Any]:
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
