"""
UnifiedAnalyticsService — SQL aggregate analytics.

Extracted from UnifiedDatabaseManager in Wave 3c (RF-CPLX-001).
All aggregate paths use SQL — no full-table loads for summaries.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from sqlalchemy import case, func
from sqlmodel import Session, select

from app.core.enums import TransactionType
from app.core.models import (
    State,
    UnifiedCommittee,
    UnifiedContribution,
    UnifiedEntity,
    UnifiedPerson,
    UnifiedTransaction,
)
from app.logger import Logger

_logger = Logger(__name__)


def nonzero_amount_sum(column):
    """SUM amounts matching legacy Python ``if tx.amount`` (excludes NULL and zero)."""
    return func.coalesce(
        func.sum(column).filter(column.isnot(None), column != 0),
        0,
    )


def transaction_type_key(tx_type: Any) -> str:
    return tx_type.value if hasattr(tx_type, "value") else str(tx_type)


class UnifiedAnalyticsService:
    """Analytics queries using SQL aggregates."""

    def __init__(
        self,
        get_session_fn: Callable[[], Session],
        *,
        get_transactions_fn: Callable[..., list[UnifiedTransaction]] | None = None,
    ) -> None:
        self._get_session = get_session_fn
        self._get_transactions = get_transactions_fn

    def top_contributors_dict(self, session: Session, *, limit: int = 10) -> dict[str, float]:
        """Top contributors by transaction amount (SQL aggregate)."""
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
        amount_sum = nonzero_amount_sum(UnifiedTransaction.amount)
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
        with self._get_session() as session:
            total_transactions, total_amount = session.exec(
                select(
                    func.count(UnifiedTransaction.id),
                    nonzero_amount_sum(UnifiedTransaction.amount),
                )
            ).one()

            by_state_rows = session.exec(
                select(
                    func.coalesce(State.code, "UNKNOWN"),
                    func.count(UnifiedTransaction.id),
                    nonzero_amount_sum(UnifiedTransaction.amount),
                )
                .join(State, UnifiedTransaction.state_id == State.id, isouter=True)
                .group_by(State.code)
            ).all()

            by_type_rows = session.exec(
                select(
                    UnifiedTransaction.transaction_type,
                    func.count(UnifiedTransaction.id),
                    nonzero_amount_sum(UnifiedTransaction.amount),
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
                    transaction_type_key(tx_type): {
                        "count": count,
                        "total_amount": float(amount or 0),
                    }
                    for tx_type, count, amount in by_type_rows
                },
                "top_contributors": self.top_contributors_dict(session, limit=10),
            }

    def get_cross_state_analysis(self) -> dict[str, Any]:
        """Get cross-state analysis of the data."""
        with self._get_session() as session:
            total_transactions = session.exec(select(func.count(UnifiedTransaction.id))).one()

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
                    func.sum(case((UnifiedTransaction.amount <= 100, 1), else_=0)),
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
    ) -> None:
        """Export transactions to JSON format."""
        if self._get_transactions is None:
            raise RuntimeError("export_to_json requires get_transactions_fn on UnifiedAnalyticsService")
        transactions = self._get_transactions(state, transaction_type, limit)

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
