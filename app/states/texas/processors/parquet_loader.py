"""
Texas Parquet Processor

Orchestrates loading Texas Ethics Commission parquet files using the existing
unified model system, then converting to SQLModels with proper relationships.
"""

from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import date
import pandas as pd
from sqlmodel import Session

from ...unified_database import UnifiedDatabaseManager
from ...postgres_config import create_postgres_database_manager
from ...unified_models import unified_processor, UnifiedTransaction as UnifiedTxModel
from ...unified_sqlmodels import (
    UnifiedCommittee, UnifiedTransaction, UnifiedPerson, UnifiedAddress,
    UnifiedEntity, UnifiedTransactionPerson, PersonRole, TransactionType,
    EntityType, State, FileOrigin, UnifiedSQLModelBuilder, unified_sql_processor
)


class TexasParquetProcessor:
    """
    Processor for loading Texas parquet files using the existing unified model system.

    Handles the complete pipeline:
    1. Load parquet files using UnifiedDataProcessor
    2. Convert unified models to SQLModels with relationships
    3. Save to database with proper deduplication
    """

    def __init__(self, db_manager: Optional[UnifiedDatabaseManager] = None):
        """Initialize processor with database manager"""
        self.db_manager = db_manager or create_postgres_database_manager()
        self.state_code = "TX"
        self.state_id = None  # Will be set when processing

    def process_all_texas_files(self, texas_data_dir: Path) -> Dict[str, int]:
        """
        Process all Texas parquet files using the unified system.

        Args:
            texas_data_dir: Directory containing Texas parquet files

        Returns:
            Dictionary with processing statistics
        """
        stats = {}

        # Get or create Texas state record
        with self.db_manager.get_session() as session:
            state = session.exec(
                session.select(State).where(State.code == self.state_code)
            ).first()

            if not state:
                state = State(code=self.state_code, name="Texas")
                session.add(state)
                session.commit()
                session.refresh(state)

            self.state_id = state.id

        # Process files using unified processor
        transaction_files = [
            ('contributions', 'contribs_*.parquet'),
            ('contributions_ss', 'cont_ss_*.parquet'),
            ('contributions_t', 'cont_t_*.parquet'),
            ('expenditures', 'expend_*.parquet'),
            ('expenditures_t', 'expn_t_*.parquet'),
            ('loans', 'loans.parquet'),
            ('pledges', 'pledges.parquet'),
            ('pledges_t', 'pledges_t.parquet'),
            ('credits', 'credits.parquet'),
        ]

        for file_type, pattern in transaction_files:
            try:
                file_paths = list(texas_data_dir.glob(pattern))
                if not file_paths:
                    print(f"No {file_type} files found matching {pattern}")
                    continue

                print(f"Processing {len(file_paths)} {file_type} files...")
                file_stats = self._process_transaction_files(file_paths)
                stats[file_type] = file_stats
                print(f"Completed {file_type}: {file_stats} records processed")

            except Exception as e:
                print(f"Error processing {file_type}: {e}")
                stats[file_type] = 0

        return stats

    def _process_transaction_files(self, file_paths: List[Path]) -> int:
        """Process transaction files using the unified processor"""
        total_processed = 0

        for file_path in file_paths:
            try:
                print(f"  Processing {file_path.name}...")

                # Use the existing unified processor to handle validation and mapping
                unified_transactions = unified_processor.process_file(file_path, "texas")

                # Convert to SQLModels and save to database
                processed = self._convert_and_save_transactions(unified_transactions)
                total_processed += processed

            except Exception as e:
                print(f"    Error processing {file_path.name}: {e}")
                continue

        return total_processed

    def _convert_and_save_transactions(self, unified_transactions: List[UnifiedTxModel]) -> int:
        """Convert unified models to SQLModels and save to database"""
        processed = 0

        with self.db_manager.get_session() as session:
            sql_builder = unified_sql_processor.get_builder("texas", state_id=self.state_id)

            for unified_tx in unified_transactions:
                try:
                    # Convert unified model to raw data dict for SQLModel builder
                    raw_data = unified_tx.raw_data.copy()
                    raw_data.update({
                        'file_origin': unified_tx.file_origin,
                        'download_date': unified_tx.download_date,
                    })

                    # Use the existing SQLModel builder to create database objects
                    sql_transaction = sql_builder.build_transaction(raw_data)

                    # Set state ID
                    sql_transaction.state_id = self.state_id

                    # Build related entities
                    sql_builder.build_person(raw_data, PersonRole.CONTRIBUTOR)  # Will create if needed
                    sql_builder.build_person(raw_data, PersonRole.PAYEE)  # Will create if needed
                    sql_builder.build_committee(raw_data)  # Will create if needed

                    # Link relationships
                    if sql_builder._find_committee_by_filer_id(sql_transaction.committee_id):
                        sql_transaction.committee_id = sql_transaction.committee_id

                    # Create transaction-person relationships
                    contributor = sql_builder.build_person(raw_data, PersonRole.CONTRIBUTOR)
                    if contributor:
                        tx_person = UnifiedTransactionPerson(
                            transaction=sql_transaction,
                            person=contributor,
                            role=PersonRole.CONTRIBUTOR,
                            amount=sql_transaction.amount,
                            state_id=self.state_id
                        )
                        sql_transaction.persons.append(tx_person)

                    payee = sql_builder.build_person(raw_data, PersonRole.PAYEE)
                    if payee:
                        tx_person = UnifiedTransactionPerson(
                            transaction=sql_transaction,
                            person=payee,
                            role=PersonRole.PAYEE,
                            amount=sql_transaction.amount,
                            state_id=self.state_id
                        )
                        sql_transaction.persons.append(tx_person)

                    # Add to session
                    session.add(sql_transaction)
                    processed += 1

                except Exception as e:
                    print(f"Error converting transaction: {e}")
                    continue

            session.commit()

        return processed
