"""Tests for TECTravelData validator and table persistence (Task 1b)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from sqlalchemy import event
from sqlmodel import Session, SQLModel, create_engine, select

from app.states.texas.validators.texas_traveldata import TECTravelData

# ---------------------------------------------------------------------------
# Shared fixture data
# ---------------------------------------------------------------------------

MINIMAL_INDIVIDUAL = {
    "recordType": "TRVL",
    "formTypeCd": "MPAC",
    "schedFormTypeCd": "A1",
    "reportInfoIdent": 100001,
    "receivedDt": "20240315",
    "infoOnlyFlag": False,
    "filerIdent": "00012345",
    "filerTypeCd": "CAMPAIGN",
    "filerName": "Smith For Governor",
    "travelInfoId": 999,
    "parentType": "EXPEND",
    "parentId": 555,
    "parentDt": "20240310",
    "parentAmount": 1250.50,
    "parentFullName": None,
    "transportationTypeCd": "COMMAIR",
    "transportationTypeDescr": "Commercial Airline",
    "departureCity": "Austin",
    "arrivalCity": "Dallas",
    "departureDt": "20240310",
    "arrivalDt": "20240310",
    "travelPurpose": "Campaign fundraiser attendance",
    "travellerPersentTypeCd": "INDIVIDUAL",
    "travellerNameLast": "Smith",
    "travellerNameFirst": "John",
    "travellerNameSuffixCd": None,
    "travellerNamePrefixCd": None,
    "travellerNameShort": "J Smith",
    "travellerNameOrganization": None,
    "file_origin": "traveldata_2024.csv",
    "download_date": "2024-04-01",
}

MINIMAL_ENTITY = {
    **MINIMAL_INDIVIDUAL,
    "travelInfoId": 1000,
    "travellerPersentTypeCd": "ENTITY",
    "travellerNameLast": None,
    "travellerNameFirst": None,
    "travellerNameShort": None,
    "travellerNameOrganization": "Acme Consulting LLC",
}


# ---------------------------------------------------------------------------
# Validation tests
# ---------------------------------------------------------------------------


class TestTECTravelDataValidation:
    def test_individual_record_validates(self):
        """A complete INDIVIDUAL travel record should pass validation."""
        record = TECTravelData.model_validate(MINIMAL_INDIVIDUAL)
        assert record.travelInfoId == 999
        assert record.travellerPersentTypeCd == "INDIVIDUAL"
        assert record.travellerNameLast == "SMITH"  # str_to_upper applied

    def test_entity_record_validates(self):
        """A complete ENTITY travel record should pass validation."""
        record = TECTravelData.model_validate(MINIMAL_ENTITY)
        assert record.travelInfoId == 1000
        assert record.travellerPersentTypeCd == "ENTITY"
        assert record.travellerNameOrganization is not None

    def test_invalid_traveller_type_raises(self):
        """travellerPersentTypeCd not in (INDIVIDUAL, ENTITY) must raise."""
        bad_data = {**MINIMAL_INDIVIDUAL, "travellerPersentTypeCd": "PERSON"}
        with pytest.raises((ValidationError, Exception)):
            TECTravelData.model_validate(bad_data)

    def test_individual_missing_last_name_raises(self):
        """INDIVIDUAL without travellerNameLast must raise."""
        bad_data = {
            **MINIMAL_INDIVIDUAL,
            "travellerNameLast": None,
            "travellerNameFirst": "John",
        }
        with pytest.raises((ValidationError, Exception)):
            TECTravelData.model_validate(bad_data)

    def test_entity_missing_organization_raises(self):
        """ENTITY without travellerNameOrganization must raise."""
        bad_data = {
            **MINIMAL_ENTITY,
            "travellerNameOrganization": None,
        }
        with pytest.raises((ValidationError, Exception)):
            TECTravelData.model_validate(bad_data)

    def test_blank_strings_coerced_to_none(self):
        """Empty strings and 'null' literals should become None."""
        data = {
            **MINIMAL_INDIVIDUAL,
            "parentFullName": "",
            "travellerNameSuffixCd": "null",
        }
        record = TECTravelData.model_validate(data)
        assert record.parentFullName is None
        assert record.travellerNameSuffixCd is None


# ---------------------------------------------------------------------------
# SQLModel metadata registration test
# ---------------------------------------------------------------------------


class TestTECTravelDataRegistration:
    def test_table_registered_in_sqlmodel_metadata(self):
        """table=True should register texas.tx_travel_data in SQLModel.metadata."""
        assert TECTravelData.__table__ is not None
        assert "texas.tx_travel_data" in SQLModel.metadata.tables

    def test_primary_key_is_travel_info_id(self):
        """travelInfoId must be the primary key column."""
        pk_cols = [col.name for col in TECTravelData.__table__.primary_key]
        assert "travelInfoId" in pk_cols


# ---------------------------------------------------------------------------
# Persistence round-trip test using ATTACH DATABASE
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def texas_engine():
    """
    In-memory SQLite engine with a 'texas' schema attached so that
    schema-qualified tables can be created and queried.
    """
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})

    @event.listens_for(engine, "connect")
    def on_connect(dbapi_conn, _connection_record):
        dbapi_conn.execute("ATTACH DATABASE ':memory:' AS texas")

    # Create only the TECTravelData table in the attached schema
    TECTravelData.__table__.create(engine, checkfirst=True)
    return engine


class TestTECTravelDataPersistence:
    def test_round_trip_by_travel_info_id(self, texas_engine):
        """Insert a validated row and retrieve it back by travelInfoId."""
        record = TECTravelData.model_validate(MINIMAL_INDIVIDUAL)

        with Session(texas_engine) as session:
            session.add(record)
            session.commit()

        with Session(texas_engine) as session:
            fetched = session.exec(
                select(TECTravelData).where(TECTravelData.travelInfoId == 999)
            ).first()

        assert fetched is not None
        assert fetched.travelInfoId == 999
        assert fetched.travellerPersentTypeCd == "INDIVIDUAL"
        assert fetched.parentAmount == pytest.approx(1250.50)
        assert fetched.departureCity == "AUSTIN"

    def test_entity_row_persists(self, texas_engine):
        """An ENTITY row also round-trips correctly."""
        record = TECTravelData.model_validate(MINIMAL_ENTITY)

        with Session(texas_engine) as session:
            session.add(record)
            session.commit()

        with Session(texas_engine) as session:
            fetched = session.exec(
                select(TECTravelData).where(TECTravelData.travelInfoId == 1000)
            ).first()

        assert fetched is not None
        assert fetched.travellerNameOrganization == "ACME CONSULTING LLC"
