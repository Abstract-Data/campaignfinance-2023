"""Tests for backfill_entity_addresses — linking canonical entities to a
representative canonical address (drives the address_occupancy view).

Covers:
- an entity is linked to its source address's canonical address;
- when a deduped entity maps to several addresses, the most frequent wins;
- the backfill is idempotent across reruns;
- an entity whose source carries no address stays NULL.
"""

from __future__ import annotations

import pytest
from sqlmodel import Session, SQLModel, create_engine

from app.core.models import UnifiedEntity
from app.resolve.models.canonical import CanonicalAddress, CanonicalEntity, EntityType
from app.resolve.models.resolution import (
    AddressCrosswalk,
    ConfidenceBand,
    EntityCrosswalk,
    MatchMethod,
    SourceType,
)
from app.resolve.publish.addresses import backfill_entity_addresses

_TABLES = [
    CanonicalAddress.__table__,
    CanonicalEntity.__table__,
    EntityCrosswalk.__table__,
    AddressCrosswalk.__table__,
    UnifiedEntity.__table__,
]


@pytest.fixture
def session():
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(eng, tables=_TABLES)
    with Session(eng) as s:
        yield s
    eng.dispose()


# --------------------------------------------------------------------------- #
# seed helpers
# --------------------------------------------------------------------------- #
def _addr(session: Session, line: str = "123 Main St") -> CanonicalAddress:
    ca = CanonicalAddress(
        standardized_line_1=line, city="Austin", state="TX", zip5="78701"
    )
    session.add(ca)
    session.flush()
    return ca


def _entity(session: Session, name: str = "Entity") -> CanonicalEntity:
    ce = CanonicalEntity(
        entity_type=EntityType.person,
        canonical_name=name,
        normalized_name=name.lower(),
        state_code="TX",
    )
    session.add(ce)
    session.flush()
    return ce


def _unified_entity(session: Session, address_id: int | None) -> UnifiedEntity:
    ue = UnifiedEntity(address_id=address_id)
    session.add(ue)
    session.flush()
    return ue


def _entity_xw(session: Session, ue_id: int, ce_id: int, run_id: int = 1) -> None:
    session.add(
        EntityCrosswalk(
            source_type=SourceType.unified_entity,
            source_id=str(ue_id),
            canonical_entity_id=ce_id,
            match_method=MatchMethod.exact,
            confidence_band=ConfidenceBand.auto,
            run_id=run_id,
        )
    )


def _addr_xw(session: Session, src_addr_id: int, ca_id: int, run_id: int = 1) -> None:
    session.add(
        AddressCrosswalk(
            source_type=SourceType.unified_address,
            source_id=str(src_addr_id),
            canonical_address_id=ca_id,
            match_method=MatchMethod.exact,
            confidence_band=ConfidenceBand.auto,
            run_id=run_id,
        )
    )


# --------------------------------------------------------------------------- #
# tests
# --------------------------------------------------------------------------- #
def test_links_entity_to_its_address(session: Session) -> None:
    ca = _addr(session)
    ce = _entity(session)
    ue = _unified_entity(session, address_id=500)
    _entity_xw(session, ue.id, ce.id)
    _addr_xw(session, 500, ca.id)
    session.flush()

    assert backfill_entity_addresses(session) == 1
    session.refresh(ce)
    assert ce.canonical_address_id == ca.id


def test_most_frequent_address_wins(session: Session) -> None:
    ca1 = _addr(session, "1 First St")
    ca2 = _addr(session, "2 Second St")
    ce = _entity(session)
    # entity maps to three source entities: two at ca1, one at ca2
    for src in (10, 11):
        ue = _unified_entity(session, address_id=src)
        _entity_xw(session, ue.id, ce.id)
        _addr_xw(session, src, ca1.id)
    ue3 = _unified_entity(session, address_id=12)
    _entity_xw(session, ue3.id, ce.id)
    _addr_xw(session, 12, ca2.id)
    session.flush()

    backfill_entity_addresses(session)
    session.refresh(ce)
    assert ce.canonical_address_id == ca1.id


def test_idempotent_across_reruns(session: Session) -> None:
    ca = _addr(session)
    ce = _entity(session)
    ue = _unified_entity(session, address_id=7)
    _entity_xw(session, ue.id, ce.id)
    _addr_xw(session, 7, ca.id)
    session.flush()

    assert backfill_entity_addresses(session) == 1
    assert backfill_entity_addresses(session) == 1
    session.refresh(ce)
    assert ce.canonical_address_id == ca.id


def test_entity_without_address_stays_null(session: Session) -> None:
    ce = _entity(session, "NoAddr")
    ue = _unified_entity(session, address_id=None)
    _entity_xw(session, ue.id, ce.id)
    session.flush()

    assert backfill_entity_addresses(session) == 0
    session.refresh(ce)
    assert ce.canonical_address_id is None
