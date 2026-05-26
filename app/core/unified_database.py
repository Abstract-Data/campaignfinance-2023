"""
Database manager for unified SQLModels backed by PostgreSQL.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, or_
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


def _nonzero_amount_sum(column):
    """SUM amounts matching legacy Python ``if tx.amount`` (excludes NULL and zero)."""
    return func.coalesce(
        func.sum(column).filter(column.isnot(None), column != 0),
        0,
    )


def _transaction_type_key(tx_type: Any) -> str:
    return tx_type.value if hasattr(tx_type, "value") else str(tx_type)


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
    fk_value: int | str,
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

    def _top_contributors_dict(self, session: Session, *, limit: int = 10) -> dict[str, float]:
        """Top contributors by transaction amount (SQL aggregate, no full-table load)."""
        person_name = func.trim(
            func.concat_ws(
                " ",
                UnifiedPerson.first_name,
                UnifiedPerson.middle_name,
                UnifiedPerson.last_name,
                UnifiedPerson.suffix,
            )
        )
        display_name = case(
            (
                UnifiedPerson.id.isnot(None),
                func.coalesce(
                    func.nullif(person_name, ""),
                    UnifiedPerson.organization,
                    "Unknown",
                ),
            ),
            else_=func.coalesce(UnifiedEntity.name, UnifiedEntity.normalized_name),
        )
        amount_sum = _nonzero_amount_sum(UnifiedTransaction.amount)
        rows = session.exec(
            select(display_name, amount_sum)
            .join(
                UnifiedContribution,
                UnifiedContribution.transaction_id == UnifiedTransaction.id,
            )
            .join(
                UnifiedEntity,
                UnifiedContribution.contributor_entity_id == UnifiedEntity.id,
            )
            .outerjoin(UnifiedPerson, UnifiedEntity.person_id == UnifiedPerson.id)
            .where(
                UnifiedTransaction.amount.isnot(None),
                UnifiedTransaction.amount != 0,
            )
            .group_by(display_name)
            .having(display_name.isnot(None))
            .order_by(amount_sum.desc())
            .limit(limit)
        ).all()
        return {name: float(total or 0) for name, total in rows if name}

    def get_summary_statistics(self) -> dict[str, Any]:
        """Get summary statistics for all data in the database."""
        with self.get_session() as session:
            total_transactions, total_amount = session.exec(
                select(
                    func.count(UnifiedTransaction.id),
                    _nonzero_amount_sum(UnifiedTransaction.amount),
                )
            ).one()

            by_state_rows = session.exec(
                select(
                    func.coalesce(State.code, "UNKNOWN"),
                    func.count(UnifiedTransaction.id),
                    _nonzero_amount_sum(UnifiedTransaction.amount),
                )
                .join(State, UnifiedTransaction.state_id == State.id, isouter=True)
                .group_by(State.code)
            ).all()

            by_type_rows = session.exec(
                select(
                    UnifiedTransaction.transaction_type,
                    func.count(UnifiedTransaction.id),
                    _nonzero_amount_sum(UnifiedTransaction.amount),
                ).group_by(UnifiedTransaction.transaction_type)
            ).all()

            return {
                "total_transactions": total_transactions,
                "total_amount": float(total_amount or 0),
                "by_state": {
                    state_code: {
                        "count": count,
                        "total_amount": float(amount or 0),
                    }
                    for state_code, count, amount in by_state_rows
                },
                "by_type": {
                    _transaction_type_key(tx_type): {
                        "count": count,
                        "total_amount": float(amount or 0),
                    }
                    for tx_type, count, amount in by_type_rows
                },
                "top_contributors": self._top_contributors_dict(session, limit=10),
            }

    def get_cross_state_analysis(self) -> dict[str, Any]:
        """Get cross-state analysis of the data."""
        with self.get_session() as session:
            total_transactions = session.exec(
                select(func.count(UnifiedTransaction.id))
            ).one()

            state_rows = session.exec(
                select(
                    State.code,
                    func.count(UnifiedTransaction.id),
                    func.coalesce(func.sum(UnifiedTransaction.amount), 0),
                )
                .join(State, UnifiedTransaction.state_id == State.id, isouter=True)
                .group_by(State.code)
            ).all()

            type_rows = session.exec(
                select(
                    UnifiedTransaction.transaction_type,
                    func.count(UnifiedTransaction.id),
                    func.coalesce(func.sum(UnifiedTransaction.amount), 0),
                ).group_by(UnifiedTransaction.transaction_type)
            ).all()

            amount_range_rows = session.exec(
                select(
                    func.sum(
                        case((UnifiedTransaction.amount <= 100, 1), else_=0)
                    ),
                    func.sum(
                        case(
                            (
                                (UnifiedTransaction.amount > 100)
                                & (UnifiedTransaction.amount <= 1000),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    func.sum(
                        case(
                            (
                                (UnifiedTransaction.amount > 1000)
                                & (UnifiedTransaction.amount <= 10000),
                                1,
                            ),
                            else_=0,
                        )
                    ),
                    func.sum(case((UnifiedTransaction.amount > 10000, 1), else_=0)),
                )
            ).one()

            top_committee_rows = session.exec(
                select(
                    UnifiedCommittee.name,
                    func.coalesce(func.sum(UnifiedTransaction.amount), 0),
                )
                .join(
                    UnifiedTransaction,
                    UnifiedTransaction.committee_id == UnifiedCommittee.filer_id,
                    isouter=True,
                )
                .group_by(UnifiedCommittee.filer_id, UnifiedCommittee.name)
                .order_by(func.coalesce(func.sum(UnifiedTransaction.amount), 0).desc())
                .limit(10)
            ).all()

            return {
                "total_transactions": total_transactions,
                "states": {
                    (row[0] or "UNKNOWN"): {
                        "count": row[1],
                        "total_amount": float(row[2] or 0),
                    }
                    for row in state_rows
                },
                "transaction_types": {
                    row[0].value: {
                        "count": row[1],
                        "total_amount": float(row[2] or 0),
                    }
                    for row in type_rows
                },
                "top_contributors": {},
                "top_committees": {
                    row[0]: float(row[1] or 0)
                    for row in top_committee_rows
                    if row[0] is not None
                },
                "amount_ranges": {
                    "0-100": int(amount_range_rows[0] or 0),
                    "100-1000": int(amount_range_rows[1] or 0),
                    "1000-10000": int(amount_range_rows[2] or 0),
                    "10000+": int(amount_range_rows[3] or 0),
                },
            }

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

    def _update_entity(
        self,
        entity_model: type,
        entity_id: int | str,
        updates: dict,
        *,
        version_model: type,
        fk_field: str,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> object | None:
        """Generic update-with-versioning for any entity."""
        with self.get_session() as session:
            entity = session.get(entity_model, entity_id)
            if entity is None:
                return None
            version_count = session.exec(
                select(func.count()).where(
                    getattr(version_model, fk_field) == entity_id
                )
            ).one()
            _record_version(
                session,
                entity=entity,
                version_model=version_model,
                fk_field=fk_field,
                fk_value=entity_id,
                version_number=version_count + 1,
                user=user,
                reason=reason,
                amendment_details=amendment_details,
            )
            for key, value in updates.items():
                if not hasattr(entity, key):
                    raise AttributeError(
                        f"{entity_model.__name__} has no field '{key}'"
                    )
                setattr(entity, key, value)
            if hasattr(entity, "last_modified_at"):
                entity.last_modified_at = _utc_now()
            if hasattr(entity, "last_modified_by"):
                entity.last_modified_by = user
            if hasattr(entity, "change_reason"):
                entity.change_reason = reason
            if hasattr(entity, "amendment_details"):
                entity.amendment_details = amendment_details
            session.add(entity)
            session.commit()
            session.refresh(entity)
            return entity

    def _get_versions(
        self,
        version_model: type,
        fk_field: str,
        entity_id: int | str,
    ) -> list:
        """Return all version records for an entity, ordered by version_number."""
        with self.get_session() as session:
            return session.exec(
                select(version_model)
                .where(getattr(version_model, fk_field) == entity_id)
                .order_by(version_model.version_number)
            ).all()

    def update_transaction(
        self,
        transaction_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedTransaction | None:
        return self._update_entity(
            UnifiedTransaction,
            transaction_id,
            updates,
            version_model=UnifiedTransactionVersion,
            fk_field="transaction_id",
            user=user,
            reason=reason,
            amendment_details=amendment_details,
        )

    def get_transaction_versions(self, transaction_id: int) -> list:
        return self._get_versions(
            UnifiedTransactionVersion, "transaction_id", transaction_id
        )

    def update_person(
        self,
        person_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedPerson | None:
        return self._update_entity(
            UnifiedPerson,
            person_id,
            updates,
            version_model=UnifiedPersonVersion,
            fk_field="person_id",
            user=user,
            reason=reason,
            amendment_details=amendment_details,
        )

    def get_person_versions(self, person_id: int) -> list:
        return self._get_versions(UnifiedPersonVersion, "person_id", person_id)

    def update_committee(
        self,
        committee_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedCommittee | None:
        return self._update_entity(
            UnifiedCommittee,
            committee_id,
            updates,
            version_model=UnifiedCommitteeVersion,
            fk_field="committee_id",
            user=user,
            reason=reason,
            amendment_details=amendment_details,
        )

    def get_committee_versions(self, committee_id: int) -> list:
        return self._get_versions(
            UnifiedCommitteeVersion, "committee_id", committee_id
        )

    def update_address(
        self,
        address_id: int,
        updates: dict,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> UnifiedAddress | None:
        return self._update_entity(
            UnifiedAddress,
            address_id,
            updates,
            version_model=UnifiedAddressVersion,
            fk_field="address_id",
            user=user,
            reason=reason,
            amendment_details=amendment_details,
        )

    def get_address_versions(self, address_id: int) -> list:
        return self._get_versions(UnifiedAddressVersion, "address_id", address_id)

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
        return self._update_entity(
            UnifiedCommitteePerson,
            committee_person_id,
            updates,
            version_model=UnifiedCommitteePersonVersion,
            fk_field="committee_person_id",
            user=user,
            reason=reason,
            amendment_details=amendment_details,
        )

    def get_committee_person_versions(
        self, committee_person_id: int
    ) -> list[UnifiedCommitteePersonVersion]:
        return self._get_versions(
            UnifiedCommitteePersonVersion, "committee_person_id", committee_person_id
        )

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
