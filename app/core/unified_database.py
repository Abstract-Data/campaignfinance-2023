"""
Database manager for unified SQLModels backed by PostgreSQL.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import func, or_
from sqlalchemy.orm import selectinload
from sqlmodel import Session, SQLModel, create_engine, select

from app.core.enums import CommitteeRole, PersonRole, TransactionType
from app.core.models import (
    State,
    UnifiedAddress,
    UnifiedAddressVersion,
    UnifiedCommittee,
    UnifiedCommitteePerson,
    UnifiedCommitteePersonVersion,
    UnifiedCommitteeVersion,
    UnifiedContribution,
    UnifiedEntity,
    UnifiedPerson,
    UnifiedPersonVersion,
    UnifiedTransaction,
    UnifiedTransactionPerson,
    UnifiedTransactionVersion,
)
from app.core.processor import unified_sql_processor
from app.logger import Logger
from app.states.postgres_config import PostgresConfig

_logger = Logger(__name__)

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _contributor_display_name(tx: UnifiedTransaction) -> str | None:
    """Display label for a contribution's contributor entity, if present."""
    contribution = tx.contribution
    if contribution is None or contribution.contributor is None or not tx.amount:
        return None
    entity = contribution.contributor
    if entity.person is not None:
        return entity.person.full_name
    return entity.name or entity.normalized_name


def _transaction_analytics_options() -> tuple:
    return (
        selectinload(UnifiedTransaction.state),
        selectinload(UnifiedTransaction.contribution)
        .selectinload(UnifiedContribution.contributor)
        .selectinload(UnifiedEntity.person),
        selectinload(UnifiedTransaction.committee),
    )


def _to_json_safe(value: Any) -> Any:
    """Serialize entity field values for version snapshots (RF-DRY-001)."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    return value


def _entity_snapshot(entity: Any) -> dict[str, Any]:
    field_names = getattr(entity, "model_fields", None) or entity.__fields__
    return {k: _to_json_safe(getattr(entity, k)) for k in field_names.keys()}


def _record_version(
    session: Session,
    *,
    entity: Any,
    version_model: type,
    fk_field: str,
    fk_value: int,
    version_number: int,
    user: str | None,
    reason: str | None,
    amendment_details: str | None,
) -> None:
    version = version_model(
        **{
            fk_field: fk_value,
            "version_number": version_number,
            "data": json.dumps(_entity_snapshot(entity)),
            "changed_at": _utc_now(),
            "changed_by": user,
            "change_reason": reason,
            "amendment_details": amendment_details,
        }
    )
    session.add(version)


class UnifiedDatabaseManager:
    """
    Database manager for unified campaign finance data.
    Uses PostgreSQL configuration by default.
    """

    def __init__(self, database_url: str | None = None, *, echo: bool = False):
        """
        Initialize the database manager.

        Args:
            database_url: SQLAlchemy database URL. If None, use PostgresConfig.
            echo: Enable SQL echo for debugging.

        Notes:
            P2-ARC-002 — ``__init__`` no longer runs ``SQLModel.metadata.create_all``.
            Callers that want tables created must explicitly invoke
            :meth:`bootstrap`.  This keeps construction (and module import) free
            of DDL side effects so the module can be imported in environments
            without a live database.
        """
        self._config: PostgresConfig | None = None

        if database_url is None:
            config = PostgresConfig()
            if not config.validate_connection():
                raise RuntimeError("Failed to validate PostgreSQL connection.")
            database_url = config.database_url
            self._config = config

        self.database_url = database_url
        engine_kwargs: dict[str, Any] = {"echo": echo}

        if self._config:
            engine_kwargs.update(
                {
                    "pool_size": self._config.pool_size,
                    "max_overflow": self._config.max_overflow,
                    "pool_timeout": self._config.pool_timeout,
                    "pool_recycle": self._config.pool_recycle,
                }
            )

        self.engine = create_engine(self.database_url, **engine_kwargs)

    def bootstrap(self) -> None:
        """Run ``SQLModel.metadata.create_all`` on this manager's engine.

        Split out of ``__init__`` so importing :mod:`app.core.unified_database`
        — and constructing a ``UnifiedDatabaseManager`` — never opens a
        connection or runs DDL implicitly (P2-ARC-002).  Callers that need
        tables created must invoke this method explicitly.
        """
        SQLModel.metadata.create_all(self.engine)

    def _resolve_state_record(self, session: Session, state_identifier: str) -> State | None:
        if not state_identifier:
            return None
        code = state_identifier.strip().upper()
        name = state_identifier.strip().lower()
        return session.exec(
            select(State).where(or_(State.code == code, func.lower(State.name) == name))
        ).first()

    def get_session(self) -> Session:
        """Get a database session"""
        return Session(self.engine)

    def load_data_from_file(self, file_path: Path, state: str) -> list[UnifiedTransaction]:
        """
        Load data from a file and convert to SQLModel instances.

        Args:
            file_path: Path to the data file
            state: State identifier

        Returns:
            List of UnifiedTransaction objects
        """
        from ..funcs.csv_reader import FileReader

        reader = FileReader()
        records = []

        if file_path.suffix.lower() == ".parquet":
            records = list(reader.read_parquet(file_path))
        elif file_path.suffix.lower() == ".csv":
            records = list(reader.read_csv(file_path))

        with self.get_session() as session:
            state_record = self._resolve_state_record(session, state)
            if not state_record:
                raise ValueError(f"State '{state}' is not present in the states table.")
            transactions = unified_sql_processor.process_records(
                records, state, state_id=state_record.id, state_code=state_record.code
            )

        _logger.info(f"Loaded {len(transactions)} transactions from {file_path}")
        return transactions

    def save_transactions(self, transactions: list[UnifiedTransaction]) -> int:
        """
        Save transactions to the database.

        Args:
            transactions: List of UnifiedTransaction objects

        Returns:
            Number of transactions saved
        """
        with self.get_session() as session:
            for transaction in transactions:
                session.add(transaction)
            session.commit()

        _logger.info(f"Saved {len(transactions)} transactions to database")
        return len(transactions)

    def load_and_save_file(self, file_path: Path, state: str) -> int:
        """
        Load data from file and save to database in one operation.

        Args:
            file_path: Path to the data file
            state: State identifier

        Returns:
            Number of transactions saved
        """
        transactions = self.load_data_from_file(file_path, state)
        return self.save_transactions(transactions)

    def get_transactions(
        self,
        state: str | None = None,
        transaction_type: TransactionType | None = None,
        limit: int | None = None,
        load_relationships: bool = True,
    ) -> list[UnifiedTransaction]:
        """
        Get transactions from the database with optional filters.

        Args:
            state: Filter by state
            transaction_type: Filter by transaction type
            limit: Limit number of results

        Returns:
            List of UnifiedTransaction objects
        """
        with self.get_session() as session:
            query = select(UnifiedTransaction)

            if state:
                state_record = self._resolve_state_record(session, state)
                if not state_record:
                    return []
                query = query.where(UnifiedTransaction.state_id == state_record.id)

            if transaction_type:
                query = query.where(UnifiedTransaction.transaction_type == transaction_type)

            if limit:
                query = query.limit(limit)

            if load_relationships:
                # Load relationships to avoid lazy loading issues
                query = query.options(
                    selectinload(UnifiedTransaction.persons),
                    selectinload(UnifiedTransaction.committee),
                    selectinload(UnifiedTransaction.state),
                    selectinload(UnifiedTransaction.file_origin),
                )

            results = session.exec(query).all()
            return results

    def get_transaction_by_id(self, transaction_id: str) -> UnifiedTransaction | None:
        """
        Get a specific transaction by its transaction ID.

        Args:
            transaction_id: Transaction ID to search for

        Returns:
            UnifiedTransaction object or None
        """
        with self.get_session() as session:
            query = select(UnifiedTransaction).where(
                UnifiedTransaction.transaction_id == transaction_id
            )
            return session.exec(query).first()

    def get_person_by_name(self, first_name: str, last_name: str) -> list[UnifiedPerson]:
        """
        Get persons by name.

        Args:
            first_name: First name to search for
            last_name: Last name to search for

        Returns:
            List of UnifiedPerson objects
        """
        with self.get_session() as session:
            query = select(UnifiedPerson).where(
                UnifiedPerson.first_name == first_name, UnifiedPerson.last_name == last_name
            )
            return session.exec(query).all()

    def get_committee_by_name(self, name: str) -> list[UnifiedCommittee]:
        """
        Get committees by name.

        Args:
            name: Committee name to search for

        Returns:
            List of UnifiedCommittee objects
        """
        with self.get_session() as session:
            query = select(UnifiedCommittee).where(UnifiedCommittee.name == name)
            return session.exec(query).all()

    def get_transactions_by_amount_range(
        self, min_amount: float, max_amount: float, state: str | None = None
    ) -> list[UnifiedTransaction]:
        """
        Get transactions within an amount range.

        Args:
            min_amount: Minimum amount
            max_amount: Maximum amount
            state: Optional state filter

        Returns:
            List of UnifiedTransaction objects
        """
        with self.get_session() as session:
            query = select(UnifiedTransaction).where(
                UnifiedTransaction.amount >= min_amount, UnifiedTransaction.amount <= max_amount
            )

            if state:
                state_record = self._resolve_state_record(session, state)
                if not state_record:
                    return []
                query = query.where(UnifiedTransaction.state_id == state_record.id)

            return session.exec(query).all()

    def get_transactions_by_date_range(
        self, start_date: str, end_date: str, state: str | None = None
    ) -> list[UnifiedTransaction]:
        """
        Get transactions within a date range.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            state: Optional state filter

        Returns:
            List of UnifiedTransaction objects
        """
        from datetime import datetime

        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        with self.get_session() as session:
            query = select(UnifiedTransaction).where(
                UnifiedTransaction.transaction_date >= start,
                UnifiedTransaction.transaction_date <= end,
            )

            if state:
                state_record = self._resolve_state_record(session, state)
                if not state_record:
                    return []
                query = query.where(UnifiedTransaction.state_id == state_record.id)

            return session.exec(query).all()

    def get_summary_statistics(self) -> dict[str, Any]:
        """
        Get summary statistics for all data in the database.

        Returns:
            Dictionary with summary statistics
        """
        with self.get_session() as session:
            # Total transactions
            total_transactions = session.exec(
                select(UnifiedTransaction).options(*_transaction_analytics_options())
            ).all()

            # Total amount
            total_amount = sum(tx.amount for tx in total_transactions if tx.amount)

            # By state
            states = {}
            for tx in total_transactions:
                state_code = tx.state.code if tx.state else "UNKNOWN"
                if state_code not in states:
                    states[state_code] = {"count": 0, "total_amount": 0}
                states[state_code]["count"] += 1
                if tx.amount:
                    states[state_code]["total_amount"] += tx.amount

            # By transaction type
            types = {}
            for tx in total_transactions:
                tx_type = tx.transaction_type.value
                if tx_type not in types:
                    types[tx_type] = {"count": 0, "total_amount": 0}
                types[tx_type]["count"] += 1
                if tx.amount:
                    types[tx_type]["total_amount"] += tx.amount

            # Top contributors (via contribution → contributor entity)
            contributor_totals = {}
            for tx in total_transactions:
                contributor_name = _contributor_display_name(tx)
                if contributor_name:
                    contributor_totals.setdefault(contributor_name, 0)
                    contributor_totals[contributor_name] += tx.amount

            top_contributors = dict(
                sorted(contributor_totals.items(), key=lambda x: x[1], reverse=True)[:10]
            )

            return {
                "total_transactions": len(total_transactions),
                "total_amount": float(total_amount),
                "by_state": states,
                "by_type": types,
                "top_contributors": top_contributors,
            }

    def get_cross_state_analysis(self) -> dict[str, Any]:
        """
        Get cross-state analysis of the data.

        Returns:
            Dictionary with cross-state analysis
        """
        with self.get_session() as session:
            # Get all transactions with their relationships
            query = select(UnifiedTransaction).options(
                selectinload(UnifiedTransaction.persons),
                *_transaction_analytics_options(),
            )
            transactions = session.exec(query).all()

            analysis = {
                "total_transactions": len(transactions),
                "states": {},
                "transaction_types": {},
                "top_contributors": {},
                "top_committees": {},
                "amount_ranges": {"0-100": 0, "100-1000": 0, "1000-10000": 0, "10000+": 0},
            }

            # Analyze each transaction
            for tx in transactions:
                # State analysis
                state_code = tx.state.code if tx.state else "UNKNOWN"
                if state_code not in analysis["states"]:
                    analysis["states"][state_code] = {"count": 0, "total_amount": 0}
                analysis["states"][state_code]["count"] += 1
                if tx.amount:
                    analysis["states"][state_code]["total_amount"] += tx.amount

                # Transaction type analysis
                tx_type = tx.transaction_type.value
                if tx_type not in analysis["transaction_types"]:
                    analysis["transaction_types"][tx_type] = {"count": 0, "total_amount": 0}
                analysis["transaction_types"][tx_type]["count"] += 1
                if tx.amount:
                    analysis["transaction_types"][tx_type]["total_amount"] += tx.amount

                # Amount range analysis
                if tx.amount:
                    amount = float(tx.amount)
                    if amount <= 100:
                        analysis["amount_ranges"]["0-100"] += 1
                    elif amount <= 1000:
                        analysis["amount_ranges"]["100-1000"] += 1
                    elif amount <= 10000:
                        analysis["amount_ranges"]["1000-10000"] += 1
                    else:
                        analysis["amount_ranges"]["10000+"] += 1

                # Contributor analysis (via contribution → contributor entity)
                contributor_name = _contributor_display_name(tx)
                if contributor_name:
                    analysis["top_contributors"].setdefault(contributor_name, 0)
                    analysis["top_contributors"][contributor_name] += tx.amount

                # Committee analysis
                if tx.committee and tx.amount:
                    committee_name = tx.committee.name
                    if committee_name not in analysis["top_committees"]:
                        analysis["top_committees"][committee_name] = 0
                    analysis["top_committees"][committee_name] += tx.amount

            # Sort top contributors and committees
            analysis["top_contributors"] = dict(
                sorted(analysis["top_contributors"].items(), key=lambda x: x[1], reverse=True)[:10]
            )
            analysis["top_committees"] = dict(
                sorted(analysis["top_committees"].items(), key=lambda x: x[1], reverse=True)[:10]
            )

            return analysis

    def export_to_json(
        self,
        output_path: Path,
        state: str | None = None,
        transaction_type: TransactionType | None = None,
        limit: int | None = None,
    ):
        """
        Export transactions to JSON format.

        Args:
            output_path: Path to save the JSON file
            state: Optional state filter
            transaction_type: Optional transaction type filter
            limit: Optional limit on number of records
        """
        transactions = self.get_transactions(state, transaction_type, limit)

        export_data = []
        for tx in transactions:
            tx_dict = {
                "id": tx.id,
                "uuid": tx.uuid,
                "transaction_id": tx.transaction_id,
                "amount": float(tx.amount) if tx.amount else None,
                "transaction_date": tx.transaction_date.isoformat()
                if tx.transaction_date
                else None,
                "description": tx.description,
                "transaction_type": tx.transaction_type.value,
                "state": tx.state.code if tx.state else None,
                "file_origin": tx.file_origin.filename if tx.file_origin else None,
                "download_date": tx.download_date,
                "filed_date": tx.filed_date.isoformat() if tx.filed_date else None,
                "amended": tx.amended,
                "created_at": tx.created_at.isoformat(),
                "updated_at": tx.updated_at.isoformat(),
                "persons": [],
                "committee": None,
            }

            # Add person relationships
            for tx_person in tx.persons:
                person_dict = {
                    "role": tx_person.role.value,
                    "person": {
                        "id": tx_person.person.id,
                        "uuid": tx_person.person.uuid,
                        "full_name": tx_person.person.full_name,
                        "first_name": tx_person.person.first_name,
                        "last_name": tx_person.person.last_name,
                        "organization": tx_person.person.organization,
                        "employer": tx_person.person.employer,
                        "occupation": tx_person.person.occupation,
                        "person_type": tx_person.person.person_type.value,
                        "address": None,
                    },
                }

                if tx_person.person.address:
                    person_dict["person"]["address"] = {
                        "street_1": tx_person.person.address.street_1,
                        "street_2": tx_person.person.address.street_2,
                        "city": tx_person.person.address.city,
                        "state": tx_person.person.address.state,
                        "zip_code": tx_person.person.address.zip_code,
                        "full_address": tx_person.person.address.full_address,
                    }

                tx_dict["persons"].append(person_dict)

            # Add committee relationship
            if tx.committee:
                tx_dict["committee"] = {
                    "id": tx.committee.id,
                    "uuid": tx.committee.uuid,
                    "name": tx.committee.name,
                    "committee_type": tx.committee.committee_type,
                    "filer_id": tx.committee.filer_id,
                }

            export_data.append(tx_dict)

        with open(output_path, "w") as f:
            json.dump(export_data, f, indent=2)

        _logger.info(f"Exported {len(export_data)} transactions to {output_path}")

    def update_transaction(
        self,
        transaction_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedTransaction | None:
        """
        Update a transaction, saving a version snapshot before updating.
        Args:
            transaction_id: The id of the transaction to update
            updates: Dict of fields to update
            user: Who made the change
            reason: Reason for change
            amendment_details: Details about the amendment
        Returns:
            The updated UnifiedTransaction or None if not found
        """
        with self.get_session() as session:
            tx = session.get(UnifiedTransaction, transaction_id)
            if not tx:
                return None
            # Get current version number
            version_count = len(
                session.exec(
                    select(UnifiedTransactionVersion).where(
                        UnifiedTransactionVersion.transaction_id == tx.id
                    )
                ).all()
            )
            _record_version(
                session,
                entity=tx,
                version_model=UnifiedTransactionVersion,
                fk_field="transaction_id",
                fk_value=tx.id,
                version_number=version_count + 1,
                user=user,
                reason=reason,
                amendment_details=amendment_details,
            )
            # Update fields
            for k, v in updates.items():
                setattr(tx, k, v)
            tx.last_modified_at = _utc_now()
            tx.last_modified_by = user
            tx.change_reason = reason
            tx.amendment_details = amendment_details
            session.add(tx)
            session.commit()
            session.refresh(tx)
            return tx

    def get_transaction_versions(self, transaction_id: int) -> list:
        """
        Get all versions for a transaction.
        Args:
            transaction_id: The id of the transaction
        Returns:
            List of UnifiedTransactionVersion objects
        """
        with self.get_session() as session:
            versions = session.exec(
                select(UnifiedTransactionVersion)
                .where(UnifiedTransactionVersion.transaction_id == transaction_id)
                .order_by(UnifiedTransactionVersion.version_number)
            ).all()
            return versions

    def update_person(
        self,
        person_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedPerson | None:
        """
        Update a person, saving a version snapshot before updating.
        """
        with self.get_session() as session:
            person = session.get(UnifiedPerson, person_id)
            if not person:
                return None
            version_count = len(
                session.exec(
                    select(UnifiedPersonVersion).where(UnifiedPersonVersion.person_id == person.id)
                ).all()
            )
            _record_version(
                session,
                entity=person,
                version_model=UnifiedPersonVersion,
                fk_field="person_id",
                fk_value=person.id,
                version_number=version_count + 1,
                user=user,
                reason=reason,
                amendment_details=amendment_details,
            )
            for k, v in updates.items():
                setattr(person, k, v)
            session.add(person)
            session.commit()
            session.refresh(person)
            return person

    def get_person_versions(self, person_id: int) -> list:
        """
        Get all versions for a person.
        """
        with self.get_session() as session:
            versions = session.exec(
                select(UnifiedPersonVersion)
                .where(UnifiedPersonVersion.person_id == person_id)
                .order_by(UnifiedPersonVersion.version_number)
            ).all()
            return versions

    def update_committee(
        self,
        committee_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedCommittee | None:
        """
        Update a committee, saving a version snapshot before updating.
        """
        with self.get_session() as session:
            committee = session.get(UnifiedCommittee, committee_id)
            if not committee:
                return None
            version_count = len(
                session.exec(
                    select(UnifiedCommitteeVersion).where(
                        UnifiedCommitteeVersion.committee_id == committee.id
                    )
                ).all()
            )
            _record_version(
                session,
                entity=committee,
                version_model=UnifiedCommitteeVersion,
                fk_field="committee_id",
                fk_value=committee.id,
                version_number=version_count + 1,
                user=user,
                reason=reason,
                amendment_details=amendment_details,
            )
            for k, v in updates.items():
                setattr(committee, k, v)
            session.add(committee)
            session.commit()
            session.refresh(committee)
            return committee

    def get_committee_versions(self, committee_id: int) -> list:
        """
        Get all versions for a committee.
        """
        with self.get_session() as session:
            versions = session.exec(
                select(UnifiedCommitteeVersion)
                .where(UnifiedCommitteeVersion.committee_id == committee_id)
                .order_by(UnifiedCommitteeVersion.version_number)
            ).all()
            return versions

    def update_address(
        self,
        address_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedAddress | None:
        """
        Update an address, saving a version snapshot before updating.
        """
        with self.get_session() as session:
            address = session.get(UnifiedAddress, address_id)
            if not address:
                return None
            version_count = len(
                session.exec(
                    select(UnifiedAddressVersion).where(
                        UnifiedAddressVersion.address_id == address.id
                    )
                ).all()
            )
            _record_version(
                session,
                entity=address,
                version_model=UnifiedAddressVersion,
                fk_field="address_id",
                fk_value=address.id,
                version_number=version_count + 1,
                user=user,
                reason=reason,
                amendment_details=amendment_details,
            )
            for k, v in updates.items():
                setattr(address, k, v)
            session.add(address)
            session.commit()
            session.refresh(address)
            return address

    def get_address_versions(self, address_id: int) -> list:
        """
        Get all versions for an address.
        """
        with self.get_session() as session:
            versions = session.exec(
                select(UnifiedAddressVersion)
                .where(UnifiedAddressVersion.address_id == address_id)
                .order_by(UnifiedAddressVersion.version_number)
            ).all()
            return versions

    def add_person_to_committee(
        self,
        person_id: int,
        committee_id: str,
        role: CommitteeRole,
        start_date: date | None = None,
        notes: str | None = None,
        user: str | None = None,
        *,
        session: Session | None = None,
    ) -> UnifiedCommitteePerson:
        """
        Add a person to a committee with a specific role.

        When *session* is provided the caller owns the transaction boundary
        (no commit/refresh here) so batch loaders avoid N+1 session churn.
        """
        committee_person = UnifiedCommitteePerson(
            person_id=person_id,
            committee_id=committee_id,
            role=role,
            start_date=start_date,
            notes=notes,
            last_modified_by=user,
        )
        if session is not None:
            session.add(committee_person)
            session.flush()
            return committee_person

        with self.get_session() as owned_session:
            owned_session.add(committee_person)
            owned_session.commit()
            owned_session.refresh(committee_person)
            return committee_person

    def remove_person_from_committee(
        self,
        person_id: int,
        committee_id: int,
        role: CommitteeRole,
        end_date: date | None = None,
        user: str | None = None,
        reason: str | None = None,
    ) -> bool:
        """
        Remove a person from a committee role (set as inactive).
        """
        with self.get_session() as session:
            committee_person = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.person_id == person_id,
                    UnifiedCommitteePerson.committee_id == committee_id,
                    UnifiedCommitteePerson.role == role,
                    UnifiedCommitteePerson.is_active.is_(True),
                )
            ).first()

            if committee_person:
                committee_person.is_active = False
                committee_person.end_date = end_date or _utc_now().date()
                committee_person.last_modified_at = _utc_now()
                committee_person.last_modified_by = user
                committee_person.change_reason = reason
                session.add(committee_person)
                session.commit()
                return True
            return False

    def get_person_committee_roles(
        self, person_id: int, active_only: bool = True
    ) -> list[UnifiedCommitteePerson]:
        """
        Get all committee roles for a specific person.
        """
        with self.get_session() as session:
            query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.person_id == person_id
            )
            if active_only:
                query = query.where(UnifiedCommitteePerson.is_active.is_(True))
            query = query.options(
                selectinload(UnifiedCommitteePerson.person),
                selectinload(UnifiedCommitteePerson.committee),
            )
            return session.exec(query.order_by(UnifiedCommitteePerson.start_date)).all()

    def get_committee_persons(
        self, committee_id: int, role: CommitteeRole | None = None, active_only: bool = True
    ) -> list[UnifiedCommitteePerson]:
        """
        Get all people for a specific committee, optionally filtered by role.
        """
        with self.get_session() as session:
            query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.committee_id == committee_id
            )
            if role:
                query = query.where(UnifiedCommitteePerson.role == role)
            if active_only:
                query = query.where(UnifiedCommitteePerson.is_active.is_(True))
            query = query.options(
                selectinload(UnifiedCommitteePerson.person),
                selectinload(UnifiedCommitteePerson.committee),
            )
            return session.exec(
                query.order_by(UnifiedCommitteePerson.role, UnifiedCommitteePerson.start_date)
            ).all()

    def update_committee_person(
        self,
        committee_person_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedCommitteePerson | None:
        """
        Update a committee-person relationship, saving a version snapshot before updating.
        """
        with self.get_session() as session:
            cp = session.get(UnifiedCommitteePerson, committee_person_id)
            if not cp:
                return None

            # Save version snapshot
            version_count = len(
                session.exec(
                    select(UnifiedCommitteePersonVersion).where(
                        UnifiedCommitteePersonVersion.committee_person_id == cp.id
                    )
                ).all()
            )

            _record_version(
                session,
                entity=cp,
                version_model=UnifiedCommitteePersonVersion,
                fk_field="committee_person_id",
                fk_value=cp.id,
                version_number=version_count + 1,
                user=user,
                reason=reason,
                amendment_details=amendment_details,
            )

            # Apply updates
            for k, v in updates.items():
                setattr(cp, k, v)
            cp.last_modified_at = _utc_now()
            cp.last_modified_by = user
            cp.change_reason = reason

            session.add(cp)
            session.commit()
            session.refresh(cp)
            return cp

    def get_committee_person_versions(
        self, committee_person_id: int
    ) -> list[UnifiedCommitteePersonVersion]:
        """
        Get all versions for a committee-person relationship.
        """
        with self.get_session() as session:
            versions = session.exec(
                select(UnifiedCommitteePersonVersion)
                .where(UnifiedCommitteePersonVersion.committee_person_id == committee_person_id)
                .order_by(UnifiedCommitteePersonVersion.version_number)
            ).all()
            return versions

    def get_active_treasurers(
        self, committee_id: int | None = None
    ) -> list[UnifiedCommitteePerson]:
        """
        Get all active treasurers, optionally filtered by committee.
        """
        with self.get_session() as session:
            query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.role == CommitteeRole.TREASURER,
                UnifiedCommitteePerson.is_active.is_(True),
            )
            if committee_id:
                query = query.where(UnifiedCommitteePerson.committee_id == committee_id)
            query = query.options(
                selectinload(UnifiedCommitteePerson.person),
                selectinload(UnifiedCommitteePerson.committee),
            )
            return session.exec(query).all()

    def get_committee_officers(
        self, committee_id: int, active_only: bool = True
    ) -> dict[CommitteeRole, list[UnifiedCommitteePerson]]:
        """
        Get all officers for a committee, grouped by role.
        """
        committee_persons = self.get_committee_persons(committee_id, active_only=active_only)
        officers = {}
        for cp in committee_persons:
            if cp.role not in officers:
                officers[cp.role] = []
            officers[cp.role].append(cp)
        return officers

    def link_transaction_to_committee_role(
        self,
        transaction_person_id: int,
        committee_person_id: int,
        user: str | None = None,
        notes: str | None = None,
    ) -> bool:
        """
        Link a transaction-person relationship to a committee role.
        This allows tracking when committee officers make contributions or receive expenditures.
        """
        with self.get_session() as session:
            tx_person = session.get(UnifiedTransactionPerson, transaction_person_id)
            if not tx_person:
                return False

            tx_person.committee_person_id = committee_person_id
            if notes:
                tx_person.notes = notes
            tx_person.updated_at = _utc_now()

            session.add(tx_person)
            session.commit()
            return True

    def get_officer_contributions(self, committee_person_id: int) -> list[UnifiedTransactionPerson]:
        """
        Get all contributions made by a committee officer.
        """
        with self.get_session() as session:
            query = (
                select(UnifiedTransactionPerson)
                .where(
                    UnifiedTransactionPerson.committee_person_id == committee_person_id,
                    UnifiedTransactionPerson.role == PersonRole.CONTRIBUTOR,
                )
                .options(
                    selectinload(UnifiedTransactionPerson.transaction),
                    selectinload(UnifiedTransactionPerson.person),
                    selectinload(UnifiedTransactionPerson.committee_person),
                )
            )
            return session.exec(query).all()

    def get_officer_expenditures(self, committee_person_id: int) -> list[UnifiedTransactionPerson]:
        """
        Get all expenditures received by a committee officer.
        """
        with self.get_session() as session:
            query = (
                select(UnifiedTransactionPerson)
                .where(
                    UnifiedTransactionPerson.committee_person_id == committee_person_id,
                    UnifiedTransactionPerson.role == PersonRole.PAYEE,
                )
                .options(
                    selectinload(UnifiedTransactionPerson.transaction),
                    selectinload(UnifiedTransactionPerson.person),
                    selectinload(UnifiedTransactionPerson.committee_person),
                )
            )
            return session.exec(query).all()

    def get_committee_officer_activities(
        self, committee_id: int, role: CommitteeRole | None = None
    ) -> dict[str, list[UnifiedTransactionPerson]]:
        """
        Get all financial activities (contributions and expenditures) for committee officers.
        """
        with self.get_session() as session:
            # Get committee officers
            committee_persons_query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.committee_id == committee_id,
                UnifiedCommitteePerson.is_active.is_(True),
            )
            if role:
                committee_persons_query = committee_persons_query.where(
                    UnifiedCommitteePerson.role == role
                )

            committee_persons = session.exec(committee_persons_query).all()

            activities = {"contributions": [], "expenditures": []}

            for cp in committee_persons:
                # Get contributions by this officer
                contributions = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.committee_person_id == cp.id,
                        UnifiedTransactionPerson.role == PersonRole.CONTRIBUTOR,
                    )
                    .options(
                        selectinload(UnifiedTransactionPerson.transaction),
                        selectinload(UnifiedTransactionPerson.person),
                        selectinload(UnifiedTransactionPerson.committee_person),
                    )
                ).all()
                activities["contributions"].extend(contributions)

                # Get expenditures to this officer
                expenditures = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.committee_person_id == cp.id,
                        UnifiedTransactionPerson.role == PersonRole.PAYEE,
                    )
                    .options(
                        selectinload(UnifiedTransactionPerson.transaction),
                        selectinload(UnifiedTransactionPerson.person),
                        selectinload(UnifiedTransactionPerson.committee_person),
                    )
                ).all()
                activities["expenditures"].extend(expenditures)

            return activities

    def get_person_committee_financial_summary(self, person_id: int) -> dict[str, Any]:
        """
        Get a financial summary for a person across all their committee roles.
        """
        with self.get_session() as session:
            # Get all committee roles for the person
            committee_roles = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.person_id == person_id,
                    UnifiedCommitteePerson.is_active.is_(True),
                )
            ).all()

            summary = {
                "person_id": person_id,
                "committee_roles": [],
                "total_contributions": 0,
                "total_expenditures": 0,
                "role_breakdown": {},
            }

            for role in committee_roles:
                role_summary = {
                    "committee": role.committee.name,
                    "role": role.role.value,
                    "start_date": role.start_date,
                    "contributions": [],
                    "expenditures": [],
                    "total_contributions": 0,
                    "total_expenditures": 0,
                }

                # Get contributions made while in this role
                contributions = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.committee_person_id == role.id,
                        UnifiedTransactionPerson.role == PersonRole.CONTRIBUTOR,
                    )
                    .options(selectinload(UnifiedTransactionPerson.transaction))
                ).all()

                for contrib in contributions:
                    amount = contrib.transaction.amount or 0
                    role_summary["contributions"].append(
                        {
                            "transaction_id": contrib.transaction.transaction_id,
                            "amount": float(amount),
                            "date": contrib.transaction.transaction_date,
                            "description": contrib.transaction.description,
                        }
                    )
                    role_summary["total_contributions"] += float(amount)
                    summary["total_contributions"] += float(amount)

                # Get expenditures received while in this role
                expenditures = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.committee_person_id == role.id,
                        UnifiedTransactionPerson.role == PersonRole.PAYEE,
                    )
                    .options(selectinload(UnifiedTransactionPerson.transaction))
                ).all()

                for exp in expenditures:
                    amount = exp.transaction.amount or 0
                    role_summary["expenditures"].append(
                        {
                            "transaction_id": exp.transaction.transaction_id,
                            "amount": float(amount),
                            "date": exp.transaction.transaction_date,
                            "description": exp.transaction.description,
                        }
                    )
                    role_summary["total_expenditures"] += float(amount)
                    summary["total_expenditures"] += float(amount)

                summary["committee_roles"].append(role_summary)
                summary["role_breakdown"][f"{role.committee.name} - {role.role.value}"] = {
                    "contributions": role_summary["total_contributions"],
                    "expenditures": role_summary["total_expenditures"],
                }

            return summary

    def auto_link_transactions_to_committee_roles(
        self, committee_id: str, user: str | None = None
    ) -> dict[str, int]:
        """
        Automatically link existing transactions to committee roles based on person and committee matching.
        This is useful when you have existing data and want to retroactively link officer activities.
        """
        with self.get_session() as session:
            # Get all committee-person relationships for this committee
            committee_persons = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.committee_id == committee_id,
                    UnifiedCommitteePerson.is_active.is_(True),
                )
            ).all()

            linked_counts = {"contributions": 0, "expenditures": 0, "total": 0}

            for cp in committee_persons:
                # Find transactions where this person is involved with this committee
                # but not yet linked to a committee role
                unlinked_transactions = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.person_id == cp.person_id,
                        UnifiedTransactionPerson.committee_person_id.is_(None),
                    )
                    .options(selectinload(UnifiedTransactionPerson.transaction))
                ).all()

                for tx_person in unlinked_transactions:
                    # Check if this transaction belongs to the committee
                    if tx_person.transaction.committee_id == committee_id:
                        # Link the transaction to this committee role
                        tx_person.committee_person_id = cp.id
                        tx_person.updated_at = _utc_now()

                        if tx_person.role == PersonRole.CONTRIBUTOR:
                            linked_counts["contributions"] += 1
                        elif tx_person.role == PersonRole.PAYEE:
                            linked_counts["expenditures"] += 1

                        linked_counts["total"] += 1
                        session.add(tx_person)

            session.commit()
            return linked_counts

    def process_transaction_with_officer_linking(
        self, transaction_data: dict, committee_officers: list[dict], user: str | None = None
    ) -> UnifiedTransaction:
        """
        Process a new transaction and automatically link it to committee officers if applicable.

        Args:
            transaction_data: Transaction data to process
            committee_officers: List of dicts with 'person_id', 'committee_id', 'role' keys
            user: User making the change
        """
        # First, create the transaction normally
        transaction = unified_sql_processor.build_transaction(transaction_data)

        with self.get_session() as session:
            session.add(transaction)
            session.commit()
            session.refresh(transaction)

            # Now check if any of the transaction participants are committee officers
            for officer in committee_officers:
                # Find the committee-person relationship
                committee_person = session.exec(
                    select(UnifiedCommitteePerson).where(
                        UnifiedCommitteePerson.person_id == officer["person_id"],
                        UnifiedCommitteePerson.committee_id == officer["committee_id"],
                        UnifiedCommitteePerson.role == officer["role"],
                        UnifiedCommitteePerson.is_active.is_(True),
                    )
                ).first()

                if committee_person:
                    # Find the transaction-person relationship for this person
                    tx_person = session.exec(
                        select(UnifiedTransactionPerson).where(
                            UnifiedTransactionPerson.transaction_id == transaction.id,
                            UnifiedTransactionPerson.person_id == officer["person_id"],
                        )
                    ).first()

                    if tx_person:
                        # Link the transaction to this committee role
                        tx_person.committee_person_id = committee_person.id
                        tx_person.updated_at = _utc_now()
                        session.add(tx_person)

            session.commit()
            session.refresh(transaction)
            return transaction

    def get_unlinked_officer_transactions(
        self, committee_id: int | None = None
    ) -> list[UnifiedTransactionPerson]:
        """
        Find transactions involving committee officers that haven't been linked to their roles yet.
        Useful for identifying transactions that need manual review and linking.
        """
        with self.get_session() as session:
            # Get all committee-person relationships
            committee_persons_query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.is_active.is_(True)
            )
            if committee_id:
                committee_persons_query = committee_persons_query.where(
                    UnifiedCommitteePerson.committee_id == committee_id
                )

            committee_persons = session.exec(committee_persons_query).all()

            unlinked_transactions = []

            for cp in committee_persons:
                # Find transactions by this person that aren't linked to any committee role
                person_transactions = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.person_id == cp.person_id,
                        UnifiedTransactionPerson.committee_person_id.is_(None),
                    )
                    .options(
                        selectinload(UnifiedTransactionPerson.transaction),
                        selectinload(UnifiedTransactionPerson.person),
                    )
                ).all()

                for tx_person in person_transactions:
                    # Add committee role info to help with manual review
                    tx_person._committee_role_info = {
                        "committee_id": cp.committee_id,
                        "role": cp.role,
                        "start_date": cp.start_date,
                    }
                    unlinked_transactions.append(tx_person)

            return unlinked_transactions


# ---------------------------------------------------------------------------
# Module-level factory (P2-ARC-002)
# ---------------------------------------------------------------------------
# Previously this module instantiated ``db_manager = UnifiedDatabaseManager()``
# at import time, which (a) opened a real PostgreSQL connection during
# ``import app.core.unified_database`` and (b) ran ``SQLModel.metadata.create_all``
# as a side effect.  Both made the module impossible to import in environments
# without a database and made the builder impossible to unit-test.
#
# ``get_db_manager()`` is the replacement: it lazily constructs and caches a
# single manager on first call.  Importing the module is now inert.
#
# ``db_manager`` remains as a ``None`` sentinel for downstream code that still
# imports it directly; callers that need an actual manager should call
# ``get_db_manager()`` (which raises ``RuntimeError`` if PostgreSQL is
# unreachable, matching the previous fail-loud semantics for non-RuntimeError
# exceptions — P1-OPS-001).
db_manager: "UnifiedDatabaseManager | None" = None
_db_manager_cached: "UnifiedDatabaseManager | None" = None


def get_db_manager(
    database_url: str | None = None,
    *,
    echo: bool = False,
    bootstrap: bool = True,
) -> UnifiedDatabaseManager:
    """Return a process-wide cached :class:`UnifiedDatabaseManager`.

    The first call constructs the manager (using the supplied ``database_url``
    or :class:`~app.states.postgres_config.PostgresConfig` if ``None``) and
    caches it on the module.  Subsequent calls ignore the arguments and return
    the same instance.

    By default, the cached manager is also bootstrapped (DDL is run) on first
    creation so callers do not have to remember a second step.  Pass
    ``bootstrap=False`` to skip that — useful when the caller wants to control
    schema lifecycle explicitly.

    Raises:
        RuntimeError: if construction fails (e.g. PostgreSQL is unreachable
            and no explicit ``database_url`` was supplied).  Other exceptions
            propagate unchanged per P1-OPS-001.
    """
    global _db_manager_cached
    if _db_manager_cached is None:
        _db_manager_cached = UnifiedDatabaseManager(database_url, echo=echo)
        if bootstrap:
            _db_manager_cached.bootstrap()
    return _db_manager_cached


def reset_db_manager_cache() -> None:
    """Reset the cached manager.  Intended for tests only."""
    global _db_manager_cached
    _db_manager_cached = None
