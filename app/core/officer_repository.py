"""
UnifiedOfficerRepository — committee-person relationship management.

Extracted from UnifiedDatabaseManager in Wave 3c (RF-CPLX-001).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any

from sqlalchemy.orm import selectinload
from sqlmodel import Session, select

from app.core.enums import CommitteeRole, PersonRole
from app.core.models import (
    UnifiedCommitteePerson,
    UnifiedTransaction,
    UnifiedTransactionPerson,
)
from app.core.processor import unified_sql_processor
from app.core.repository import UnifiedVersionedRepository, utc_now


class UnifiedOfficerRepository:
    """Committee-person relationship management."""

    def __init__(
        self,
        get_session_fn: Callable[[], Session],
        *,
        repo: UnifiedVersionedRepository | None = None,
    ) -> None:
        self._get_session = get_session_fn
        self._repo = repo

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
        """Add a person to a committee with a specific role."""
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

        with self._get_session() as owned_session:
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
        """Remove a person from a committee role (set as inactive)."""
        with self._get_session() as session:
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
                committee_person.end_date = end_date or utc_now().date()
                committee_person.last_modified_at = utc_now()
                committee_person.last_modified_by = user
                committee_person.change_reason = reason
                session.add(committee_person)
                session.commit()
                return True
            return False

    def get_person_committee_roles(
        self, person_id: int, active_only: bool = True
    ) -> list[UnifiedCommitteePerson]:
        """Get all committee roles for a specific person."""
        with self._get_session() as session:
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
        """Get all people for a committee, optionally filtered by role."""
        with self._get_session() as session:
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

    def get_active_treasurers(
        self, committee_id: int | None = None
    ) -> list[UnifiedCommitteePerson]:
        """Get all active treasurers, optionally filtered by committee."""
        with self._get_session() as session:
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
        """Get all officers for a committee, grouped by role."""
        committee_persons = self.get_committee_persons(committee_id, active_only=active_only)
        officers: dict[CommitteeRole, list[UnifiedCommitteePerson]] = {}
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
        """Link a transaction-person row to a committee role."""
        with self._get_session() as session:
            tx_person = session.get(UnifiedTransactionPerson, transaction_person_id)
            if not tx_person:
                return False

            tx_person.committee_person_id = committee_person_id
            if notes:
                tx_person.notes = notes
            tx_person.updated_at = utc_now()

            session.add(tx_person)
            session.commit()
            return True

    def get_officer_contributions(self, committee_person_id: int) -> list[UnifiedTransactionPerson]:
        """Get all contributions made by a committee officer."""
        with self._get_session() as session:
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
        """Get all expenditures received by a committee officer."""
        with self._get_session() as session:
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
        """Get financial activities for committee officers."""
        with self._get_session() as session:
            committee_persons_query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.committee_id == committee_id,
                UnifiedCommitteePerson.is_active.is_(True),
            )
            if role:
                committee_persons_query = committee_persons_query.where(
                    UnifiedCommitteePerson.role == role
                )

            committee_persons = session.exec(committee_persons_query).all()

            activities: dict[str, list[UnifiedTransactionPerson]] = {
                "contributions": [],
                "expenditures": [],
            }

            for cp in committee_persons:
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
        """Financial summary for a person across committee roles."""
        with self._get_session() as session:
            committee_roles = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.person_id == person_id,
                    UnifiedCommitteePerson.is_active.is_(True),
                )
            ).all()

            summary: dict[str, Any] = {
                "person_id": person_id,
                "committee_roles": [],
                "total_contributions": 0,
                "total_expenditures": 0,
                "role_breakdown": {},
            }

            for role in committee_roles:
                role_summary: dict[str, Any] = {
                    "committee": role.committee.name,
                    "role": role.role.value,
                    "start_date": role.start_date,
                    "contributions": [],
                    "expenditures": [],
                    "total_contributions": 0,
                    "total_expenditures": 0,
                }

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
        """Retroactively link transactions to committee roles."""
        del user  # reserved for audit trail on future writes
        with self._get_session() as session:
            committee_persons = session.exec(
                select(UnifiedCommitteePerson).where(
                    UnifiedCommitteePerson.committee_id == committee_id,
                    UnifiedCommitteePerson.is_active.is_(True),
                )
            ).all()

            linked_counts = {"contributions": 0, "expenditures": 0, "total": 0}

            for cp in committee_persons:
                unlinked_transactions = session.exec(
                    select(UnifiedTransactionPerson)
                    .where(
                        UnifiedTransactionPerson.person_id == cp.person_id,
                        UnifiedTransactionPerson.committee_person_id.is_(None),
                    )
                    .options(selectinload(UnifiedTransactionPerson.transaction))
                ).all()

                for tx_person in unlinked_transactions:
                    if tx_person.transaction.committee_id == committee_id:
                        tx_person.committee_person_id = cp.id
                        tx_person.updated_at = utc_now()

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
        """Process a transaction and link committee officers when applicable."""
        del user
        transaction = unified_sql_processor.build_transaction(transaction_data)

        with self._get_session() as session:
            session.add(transaction)
            session.commit()
            session.refresh(transaction)

            for officer in committee_officers:
                committee_person = session.exec(
                    select(UnifiedCommitteePerson).where(
                        UnifiedCommitteePerson.person_id == officer["person_id"],
                        UnifiedCommitteePerson.committee_id == officer["committee_id"],
                        UnifiedCommitteePerson.role == officer["role"],
                        UnifiedCommitteePerson.is_active.is_(True),
                    )
                ).first()

                if committee_person:
                    tx_person = session.exec(
                        select(UnifiedTransactionPerson).where(
                            UnifiedTransactionPerson.transaction_id == transaction.id,
                            UnifiedTransactionPerson.person_id == officer["person_id"],
                        )
                    ).first()

                    if tx_person:
                        tx_person.committee_person_id = committee_person.id
                        tx_person.updated_at = utc_now()
                        session.add(tx_person)

            session.commit()
            session.refresh(transaction)
            return transaction

    def get_unlinked_officer_transactions(
        self, committee_id: int | None = None
    ) -> list[UnifiedTransactionPerson]:
        """Find officer transactions not yet linked to committee roles."""
        with self._get_session() as session:
            committee_persons_query = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.is_active.is_(True)
            )
            if committee_id:
                committee_persons_query = committee_persons_query.where(
                    UnifiedCommitteePerson.committee_id == committee_id
                )

            committee_persons = session.exec(committee_persons_query).all()

            unlinked_transactions: list[UnifiedTransactionPerson] = []

            for cp in committee_persons:
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
                    tx_person._committee_role_info = {
                        "committee_id": cp.committee_id,
                        "role": cp.role,
                        "start_date": cp.start_date,
                    }
                    unlinked_transactions.append(tx_person)

            return unlinked_transactions
