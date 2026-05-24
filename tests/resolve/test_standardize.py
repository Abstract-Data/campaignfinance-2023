"""Task 1c tests for standardizers and stage-1 resolution input."""

from __future__ import annotations

from hypothesis import given, strategies as st
from sqlmodel import Session, SQLModel, create_engine, select

from app.core.source_models.reports import UnifiedReport  # noqa: F401
from app.core.unified_sqlmodels import (
    EntityType,
    State,
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.resolve.standardize.addresses import standardize_address
from app.resolve.standardize.names import standardize_name
from app.resolve.standardize.orgs import normalize_org_name
from app.resolve.standardize.stage1 import build_resolution_input
from app.resolve.standardize.staging import ResolutionInput


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

