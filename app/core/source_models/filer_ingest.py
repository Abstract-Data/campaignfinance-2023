"""Ingest builder for TEC FILER records (filers_*.csv).

A FILER row is the canonical source of committee identity: it carries the
full address, officer names/addresses/phones, office sought, and status that
don't appear on individual transaction rows.

This builder upserts ``UnifiedCommittee`` (find-or-create by filer_id) and
creates ``UnifiedCommitteePerson`` rows for treasurer, assistant treasurer,
and chair when those names are present.

Requires a live SQLModel ``Session`` because it must look up existing rows.
"""

from __future__ import annotations

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, select

from app.core.enums import CommitteeRole, EntityType, PersonType
from app.core.models.tables import (
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedCommitteePerson,
    UnifiedEntity,
    UnifiedPerson,
)
from app.core.value_objects import normalize_entity_name


def _s(value: object) -> str | None:
    """Strip and return None for blank/null values."""
    if value is None:
        return None
    v = str(value).strip()
    return v or None


def _find_or_create_address(session: Session, data: dict) -> UnifiedAddress | None:
    """Find an existing address or create a new one."""
    street_1 = _s(data.get("street_1"))
    city = _s(data.get("city"))
    state = _s(data.get("state"))
    zip_code = _s(data.get("zip_code"))

    if not any([street_1, city, state, zip_code]):
        return None

    from sqlalchemy.sql.expression import func as sa_func

    def _ci(col, val):
        return col.is_(None) if val is None else sa_func.lower(col) == val.lower()

    try:
        stmt = select(UnifiedAddress).where(
            _ci(UnifiedAddress.street_1, street_1),
            _ci(UnifiedAddress.city, city),
            _ci(UnifiedAddress.state, state),
            (
                UnifiedAddress.zip_code.is_(None)
                if zip_code is None
                else UnifiedAddress.zip_code == zip_code
            ),
        )
        existing = session.exec(stmt).first()
        if existing:
            return existing
    except SQLAlchemyError:
        pass

    return UnifiedAddress(
        street_1=street_1,
        city=city,
        state=state.upper() if state else None,
        zip_code=zip_code,
    )


def _find_or_create_person(
    session: Session,
    first_name: str | None,
    last_name: str | None,
    organization: str | None,
    state_id: int | None,
) -> UnifiedPerson | None:
    """Find an existing person by name/org+state, or build a new one."""
    if not any([first_name, last_name, organization]):
        return None

    from sqlalchemy.sql.expression import func as sa_func

    try:
        if organization:
            stmt = select(UnifiedPerson).where(
                sa_func.lower(UnifiedPerson.organization) == organization.lower(),
                UnifiedPerson.state_id == state_id,
                UnifiedPerson.organization.is_not(None),
            )
        elif first_name and last_name:
            stmt = select(UnifiedPerson).where(
                sa_func.lower(UnifiedPerson.first_name) == first_name.lower(),
                sa_func.lower(UnifiedPerson.last_name) == last_name.lower(),
                UnifiedPerson.state_id == state_id,
                UnifiedPerson.organization.is_(None),
            )
        else:
            return None
        existing = session.exec(stmt).first()
        if existing:
            return existing
    except SQLAlchemyError:
        pass

    person_type = PersonType.ORGANIZATION if organization else PersonType.INDIVIDUAL
    return UnifiedPerson(
        first_name=first_name,
        last_name=last_name,
        organization=organization,
        person_type=person_type,
        state_id=state_id,
    )


def _extract_address_fields(raw: dict, prefix: str) -> dict:
    """Pull address sub-fields from a raw row using a TEC field prefix."""
    return {
        "street_1": _s(raw.get(f"{prefix}StreetAddr1")),
        "city": _s(raw.get(f"{prefix}StreetCity")),
        "state": _s(raw.get(f"{prefix}StreetStateCd")),
        "zip_code": _s(raw.get(f"{prefix}StreetPostalCode")),
    }


def _extract_name_fields(raw: dict, prefix: str) -> tuple[str | None, str | None, str | None]:
    """Return (first_name, last_name, organization) for a given name prefix."""
    org = _s(raw.get(f"{prefix}NameOrganization"))
    last = _s(raw.get(f"{prefix}NameLast"))
    first = _s(raw.get(f"{prefix}NameFirst"))
    return first, last, org


def build_filer_committee(
    raw: dict,
    *,
    state_id: int,
    session: Session,
    file_origin_id: str | None = None,
) -> UnifiedCommittee | None:
    """Upsert a UnifiedCommittee from a TEC FILER row.

    Also creates UnifiedCommitteePerson rows for treasurer, assistant
    treasurer, and chair when those names are present in the raw record.

    Returns the committee (new or existing, with updates applied).
    """
    filer_id = _s(raw.get("filerIdent"))
    if not filer_id:
        return None

    # ── Find or create the committee ─────────────────────────────────────────
    try:
        stmt = select(UnifiedCommittee).where(UnifiedCommittee.filer_id == filer_id)
        committee = session.exec(stmt).first()
    except SQLAlchemyError:
        committee = None

    if committee is None:
        committee = UnifiedCommittee(filer_id=filer_id, state_id=state_id)

    # Apply the richer data from the FILER record
    committee.name = _s(raw.get("filerName")) or committee.name
    committee.committee_type = _s(raw.get("filerTypeCd")) or committee.committee_type
    committee.filer_status = _s(raw.get("committeeStatusCd"))
    committee.state_id = state_id

    # Address
    addr_data = _extract_address_fields(raw, "filer")
    if any(addr_data.values()):
        address = _find_or_create_address(session, addr_data)
        if address and not committee.address_id:
            committee.address = address

    session.add(committee)
    session.flush()  # ensures committee.filer_id is persisted before FK refs below

    # ── Officers ─────────────────────────────────────────────────────────────
    # Each officer runs in its own savepoint: a constraint failure on one (e.g. a
    # dedup miss racing the unique person index) is rolled back to the savepoint
    # and skipped, instead of poisoning the session and losing the whole committee.
    for name_prefix, role in (
        ("treas", CommitteeRole.TREASURER),
        ("assttreas", CommitteeRole.ASSISTANT_TREASURER),
        ("chair", CommitteeRole.CHAIR),
    ):
        try:
            with session.begin_nested():
                _upsert_officer(
                    session,
                    committee=committee,
                    raw=raw,
                    name_prefix=name_prefix,
                    role=role,
                    state_id=state_id,
                )
        except SQLAlchemyError:
            # Officer skipped; the committee and other officers are unaffected.
            continue

    return committee


def _ensure_person_entity(session: Session, person: UnifiedPerson, state_id: int | None) -> None:
    """Attach a deduped ``UnifiedEntity`` to a committee officer person.

    Officers created here would otherwise have no entity and be invisible to the
    resolution pipeline.  The entity is keyed by ``(entity_type, normalized_name,
    state_id)`` — the same key contributor/payee entities use — so an officer who
    is also a contributor collapses to one entity (and one canonical id).
    """
    if person is None or getattr(person, "entity", None) is not None:
        return
    is_org = bool(person.organization)
    name = person.organization if is_org else person.full_name
    normalized = normalize_entity_name(name)
    if not normalized:
        return
    entity_type = EntityType.ORGANIZATION if is_org else EntityType.PERSON
    try:
        existing = session.exec(
            select(UnifiedEntity).where(
                UnifiedEntity.entity_type == entity_type,
                UnifiedEntity.normalized_name == normalized,
                UnifiedEntity.state_id == state_id,
            )
        ).first()
    except SQLAlchemyError:
        existing = None
    if existing is not None:
        if existing.person is None:
            existing.person = person
        person.entity = existing
        return
    entity = UnifiedEntity(
        entity_type=entity_type,
        name=name,
        normalized_name=normalized,
        person=person,
        state_id=state_id,
    )
    person.entity = entity
    session.add(entity)


def _upsert_officer(
    session: Session,
    committee: UnifiedCommittee,
    raw: dict,
    name_prefix: str,
    role: CommitteeRole,
    state_id: int,
) -> None:
    """Create a UnifiedCommitteePerson for one officer role if name data exists."""
    first, last, org = _extract_name_fields(raw, name_prefix)
    if not any([first, last, org]):
        return

    person = _find_or_create_person(session, first, last, org, state_id)
    if person is None:
        return
    _ensure_person_entity(session, person, state_id)

    # Check if this officer relationship already exists
    try:
        if person.id is not None:
            stmt = select(UnifiedCommitteePerson).where(
                UnifiedCommitteePerson.committee_id == committee.filer_id,
                UnifiedCommitteePerson.person_id == person.id,
                UnifiedCommitteePerson.role == role,
            )
            if session.exec(stmt).first():
                return
    except SQLAlchemyError:
        pass

    session.add(person)
    session.flush()

    # Denormalize the officer's canonical entity onto the link row.  person.entity
    # is set by _ensure_person_entity and now has an id after the flush above;
    # leaving entity_id null would make the column meaningless for downstream joins.
    entity_id = person.entity.id if getattr(person, "entity", None) is not None else None

    cp = UnifiedCommitteePerson(
        committee_id=committee.filer_id,
        person_id=person.id,
        entity_id=entity_id,
        role=role,
        state_id=state_id,
        is_active=True,
    )
    session.add(cp)
