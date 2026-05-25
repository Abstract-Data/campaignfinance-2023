"""Task 0b — UnifiedPledge model and build_pledge() tests."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Session, create_engine

from app.core.source_models.pledges import UnifiedPledge
from app.core.source_models.pledges_ingest import build_pledge
from tests.resolve.conftest import (
    StubState,
    StubUnifiedEntity,
    StubUnifiedTransaction,
    create_resolve_tables,
    drop_resolve_tables,
)

SAMPLE_PLDG_RAW = {
    "recordType": "PLDG",
    "formTypeCd": "A",
    "schedFormTypeCd": "B",
    "reportInfoIdent": 12345,
    "receivedDt": "20240115",
    "infoOnlyFlag": "N",
    "filerIdent": "00012345",
    "filerTypeCd": "CAN",
    "filerName": "SMITH FOR TEXAS",
    "pledgeInfoId": 987654,
    "pledgeDt": "20240110",
    "pledgeAmount": "2500.00",
    "pledgeDescr": "Fundraiser pledge",
    "itemizeFlag": "Y",
    "travelFlag": "N",
    "pledgerPersentTypeCd": "INDIVIDUAL",
    "pledgerNameLast": "DOE",
    "pledgerNameFirst": "JANE",
}


def test_build_pledge_maps_pldg_fields_to_detail_row():
    transaction = SimpleNamespace(
        id=42,
        amount=Decimal("2500.00"),
        transaction_date=date(2024, 1, 10),
        description="Fundraiser pledge",
    )
    pledgor = SimpleNamespace(id=10)
    recipient = SimpleNamespace(id=20)

    pledge = build_pledge(
        transaction,
        pledgor,
        recipient,
        SAMPLE_PLDG_RAW,
        state_id=1,
    )

    assert isinstance(pledge, UnifiedPledge)
    assert pledge.transaction_id == 42
    assert pledge.pledgor_entity_id == 10
    assert pledge.recipient_entity_id == 20
    assert pledge.amount == Decimal("2500.00")
    assert pledge.pledge_date == date(2024, 1, 10)
    assert pledge.description == "Fundraiser pledge"
    assert pledge.is_fulfilled is False
    assert pledge.state_id == 1


@pytest.fixture(name="pledge_engine")
def pledge_engine_fixture():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    create_resolve_tables(engine)
    yield engine
    drop_resolve_tables(engine)


def test_unified_pledges_table_creates_and_enforces_transaction_uniqueness(
    pledge_engine,
):
    with Session(pledge_engine) as session:
        state = StubState(code="TX")
        pledgor = StubUnifiedEntity()
        recipient = StubUnifiedEntity()
        transaction = StubUnifiedTransaction(
            amount=Decimal("100.00"),
            transaction_date=date(2024, 1, 1),
        )
        session.add(state)
        session.add(pledgor)
        session.add(recipient)
        session.add(transaction)
        session.commit()
        session.refresh(state)
        session.refresh(pledgor)
        session.refresh(recipient)
        session.refresh(transaction)

        pledge = UnifiedPledge(
            transaction_id=transaction.id,
            pledgor_entity_id=pledgor.id,
            recipient_entity_id=recipient.id,
            state_id=state.id,
            amount=Decimal("100.00"),
            pledge_date=date(2024, 1, 1),
        )
        session.add(pledge)
        session.commit()

        duplicate = UnifiedPledge(
            transaction_id=transaction.id,
            pledgor_entity_id=pledgor.id,
            recipient_entity_id=recipient.id,
            state_id=state.id,
            amount=Decimal("50.00"),
            pledge_date=date(2024, 1, 2),
        )
        session.add(duplicate)
        with pytest.raises(IntegrityError):
            session.commit()
