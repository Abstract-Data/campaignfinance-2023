"""Task 1c tests for standardizers and stage-1 resolution input."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from sqlmodel import Session, SQLModel, create_engine, select

import app.resolve.models  # noqa: F401 — central ORM registry (UnifiedReport)
from app.core.enums import EntityType
from app.core.models import (
    State,
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.resolve.models.canonical import UnmappedEntityTypeError
from app.resolve.standardize.addresses import standardize_address
from app.resolve.standardize.names import standardize_name
from app.resolve.standardize.orgs import normalize_org_name
from app.resolve.standardize.stage1 import build_resolution_input
from app.resolve.standardize.staging import ResolutionInput, coerce_staging_entity_type


def _address_strategy():
    token = st.text(
        min_size=1,
        max_size=20,
        alphabet=st.characters(
            blacklist_categories=("Cs",),
        ),
    )
    return st.builds(
        lambda street, city, state, zip_code: f"{street}, {city}, {state} {zip_code}",
        street=token,
        city=token,
        state=st.sampled_from(["TX", "OK", "CA"]),
        zip_code=st.integers(min_value=10000, max_value=99999).map(str),
    )


@given(st.text(min_size=0, max_size=120))
def test_standardize_name_is_idempotent(raw_name: str):
    once = standardize_name(raw_name)
    twice = standardize_name(once)
    assert once == twice


@given(_address_strategy())
def test_standardize_address_is_idempotent(raw_address: str):
    once = standardize_address(raw_address)
    twice = standardize_address(once)
    assert once == twice


@given(st.text(min_size=0, max_size=120))
def test_normalize_org_name_is_idempotent(raw_org: str):
    once = normalize_org_name(raw_org)
    twice = normalize_org_name(once)
    assert once == twice


def test_normalize_org_name_equivalent_forms():
    assert normalize_org_name("Acme, L.L.C.") == normalize_org_name("ACME LLC")


def test_standardize_address_preserves_unit_suite():
    standardized = standardize_address("123 Main Street Apt 4, Austin, TX 78701")
    assert standardized.line_2 is not None
    assert "4" in standardized.line_2


def test_standardize_address_unparseable_returns_unparsed():
    standardized = standardize_address("!!! ??? ###")
    assert standardized.parse_status == "unparsed"


def test_coerce_staging_entity_type_maps_vendor_to_organization():
    assert coerce_staging_entity_type("vendor") == "organization"


def test_coerce_staging_entity_type_rejects_campaign():
    with pytest.raises(UnmappedEntityTypeError, match="campaign"):
        coerce_staging_entity_type("campaign")


def test_resolution_input_zip_column_lengths():
    zip5_col = ResolutionInput.__table__.c.zip5
    zip4_col = ResolutionInput.__table__.c.zip4
    assert zip5_col.type.length == 5
    assert zip4_col.type.length == 4


def test_resolution_input_table_registered_and_stage1_inserts_rows():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(
        engine,
        tables=[
            State.__table__,
            UnifiedAddress.__table__,
            UnifiedPerson.__table__,
            UnifiedCommittee.__table__,
            UnifiedEntity.__table__,
            ResolutionInput.__table__,
        ],
    )

    with Session(engine) as session:
        tx = State(code="TX", name="Texas")
        session.add(tx)
        session.commit()
        session.refresh(tx)

        address = UnifiedAddress(
            street_1="123 Main St",
            street_2="Suite 200",
            city="Austin",
            state="TX",
            zip_code="78701",
        )
        session.add(address)
        session.commit()
        session.refresh(address)

        person = UnifiedPerson(
            first_name="John",
            last_name="Smith",
            state_id=tx.id,
            address_id=address.id,
        )
        committee = UnifiedCommittee(
            filer_id="CMT-1",
            name="Friends of John Smith",
            state_id=tx.id,
            address_id=address.id,
        )
        entity = UnifiedEntity(
            name="Acme LLC",
            entity_type=EntityType.ORGANIZATION,
            state_id=tx.id,
            address_id=address.id,
        )
        session.add(person)
        session.add(committee)
        session.add(entity)
        session.commit()

        inserted = build_resolution_input(session, run_id=42, state_code="TX")
        assert inserted == 3

        rows = session.exec(
            select(ResolutionInput).where(ResolutionInput.run_id == 42)
        ).all()
        assert len(rows) == 3
        assert {row.source_type for row in rows} == {
            "unified_person",
            "unified_committee",
            "unified_entity",
        }


def test_build_resolution_input_rolls_back_delete_on_insert_failure(monkeypatch):
    """Delete and insert share one transaction (M-4)."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    SQLModel.metadata.create_all(
        engine,
        tables=[
            State.__table__,
            UnifiedAddress.__table__,
            UnifiedPerson.__table__,
            UnifiedCommittee.__table__,
            UnifiedEntity.__table__,
            ResolutionInput.__table__,
        ],
    )

    with Session(engine) as session:
        tx = State(code="TX", name="Texas")
        session.add(tx)
        session.commit()
        session.refresh(tx)

        address = UnifiedAddress(
            street_1="123 Main St",
            city="Austin",
            state="TX",
            zip_code="78701",
        )
        session.add(address)
        session.commit()
        session.refresh(address)

        session.add(
            UnifiedPerson(
                first_name="Jane",
                last_name="Doe",
                state_id=tx.id,
                address_id=address.id,
            )
        )
        session.add(
            ResolutionInput(
                run_id=7,
                source_type="unified_person",
                source_id="legacy",
                entity_type="person",
                raw_name="Legacy Row",
                raw_address="",
            )
        )
        session.commit()

        def _fail_add_all(_rows):
            raise RuntimeError("simulated insert failure")

        monkeypatch.setattr(session, "add_all", _fail_add_all)

        with pytest.raises(RuntimeError, match="simulated insert failure"):
            build_resolution_input(session, run_id=7, state_code="TX")

        surviving = session.exec(
            select(ResolutionInput).where(ResolutionInput.run_id == 7)
        ).all()
        assert len(surviving) == 1
        assert surviving[0].source_id == "legacy"



@pytest.mark.parametrize(
    "dirty",
    [
        "123 Main St Apt, Austin, TX 78701",   # occupancy type, no identifier
        "500 W 2nd St Unit, Dallas TX",
        "1 Plaza Fl, Houston, TX",
    ],
)
def test_standardize_address_survives_dirty_occupancy(dirty):
    """A scourgify AddressNormalizationError (occupancy type w/o identifier) must
    degrade to a parsed/unparsed result, never propagate and kill stage 1."""
    result = standardize_address(dirty)
    assert result.parse_status in {"parsed", "partial", "unparsed"}
