"""
UnifiedVersionedRepository — CRUD with version snapshotting.

Extracted from UnifiedDatabaseManager in Wave 3c (RF-CPLX-001).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, TypeVar

from sqlmodel import Session, func, select

from app.core.models import (
    UnifiedAddress,
    UnifiedAddressVersion,
    UnifiedCommittee,
    UnifiedCommitteePerson,
    UnifiedCommitteePersonVersion,
    UnifiedCommitteeVersion,
    UnifiedPerson,
    UnifiedPersonVersion,
    UnifiedTransaction,
    UnifiedTransactionVersion,
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_json_safe(value: Any) -> Any:
    """Serialize entity field values for version snapshots (RF-DRY-001)."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_json_safe(v) for v in value]
    return value


def entity_snapshot(entity: Any) -> dict[str, Any]:
    field_names = getattr(entity, "model_fields", None) or entity.__fields__
    return {k: to_json_safe(getattr(entity, k)) for k in field_names.keys()}


def record_version(
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
            "data": json.dumps(entity_snapshot(entity)),
            "changed_at": utc_now(),
            "changed_by": user,
            "change_reason": reason,
            "amendment_details": amendment_details,
        }
    )
    session.add(version)


# Backward-compat aliases for imports from unified_database (task-3c)
_utc_now = utc_now
_to_json_safe = to_json_safe
_entity_snapshot = entity_snapshot
_record_version = record_version

TEntity = TypeVar("TEntity")


class UnifiedVersionedRepository:
    """CRUD + version snapshotting for unified entities."""

    def __init__(self, get_session_fn: Callable[[], Session]) -> None:
        self._get_session = get_session_fn

    def _update_entity(
        self,
        entity_model: type[TEntity],
        entity_id: int | str,
        updates: dict,
        *,
        version_model: type,
        fk_field: str,
        user: str | None = None,
        reason: str | None = None,
        amendment_details: str | None = None,
    ) -> TEntity | None:
        """Generic update-with-versioning for any entity."""
        with self._get_session() as session:
            entity = session.get(entity_model, entity_id)
            if entity is None:
                return None
            version_count = session.exec(
                select(func.count()).where(getattr(version_model, fk_field) == entity_id)
            ).one()
            record_version(
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
                    raise AttributeError(f"{entity_model.__name__} has no field '{key}'")
                setattr(entity, key, value)
            if hasattr(entity, "last_modified_at"):
                entity.last_modified_at = utc_now()
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
        with self._get_session() as session:
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
        return self._get_versions(UnifiedTransactionVersion, "transaction_id", transaction_id)

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
        return self._get_versions(UnifiedCommitteeVersion, "committee_id", committee_id)

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
