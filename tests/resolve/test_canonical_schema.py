"""Task 1a — Canonical-layer schema tests.

TDD step 1: verify all four canonical tables register in SQLModel.metadata
and create cleanly via create_all against an in-memory SQLite engine.

TDD step 5: additional structural assertions from the interface contract.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import UniqueConstraint, create_engine, inspect, text
from sqlmodel import Session, SQLModel

from app.core.enums import EntityType as UnifiedEntityType
from app.core.source_models.reports import UnifiedReport  # noqa: F401
from app.resolve.models.canonical import (
    CanonicalAddress,
    CanonicalCampaign,
    CanonicalEntity,
    CanonicalEntityTypeType,
    CanonicalNameHistory,
    EntityType,
    UnmappedEntityTypeError,
    map_unified_to_canonical_entity_type,
)


@pytest.fixture(scope="module")
def sqlite_engine():
    """In-memory SQLite engine for schema creation tests."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(
        engine,
        tables=[
            CanonicalAddress.__table__,
            CanonicalEntity.__table__,
            CanonicalCampaign.__table__,
            CanonicalNameHistory.__table__,
        ],
    )
    yield engine
    engine.dispose()


class TestCanonicalTablesCreate:
    EXPECTED_TABLES = {
        "canonical_entity",
        "canonical_campaign",
        "canonical_address",
        "canonical_name_history",
    }

    def test_tables_in_metadata(self):
        """All four canonical tables must appear in SQLModel.metadata."""
        registered = set(SQLModel.metadata.tables.keys())
        assert self.EXPECTED_TABLES.issubset(registered), (
            f"Missing tables: {self.EXPECTED_TABLES - registered}"
        )

    def test_create_all_succeeds(self, sqlite_engine):
        """create_all must succeed without error for all four tables."""
        inspector = inspect(sqlite_engine)
        actual = set(inspector.get_table_names())
        assert self.EXPECTED_TABLES.issubset(actual), (
            f"Tables not created in DB: {self.EXPECTED_TABLES - actual}"
        )


class TestCanonicalEntityStructure:
    def test_canonical_address_id_is_nullable(self, sqlite_engine):
        """canonical_entity.canonical_address_id must be nullable (many entities share one address)."""
        inspector = inspect(sqlite_engine)
        columns = {c["name"]: c for c in inspector.get_columns("canonical_entity")}
        assert "canonical_address_id" in columns, "canonical_address_id column missing"
        assert columns["canonical_address_id"]["nullable"] is True, (
            "canonical_address_id must be nullable"
        )

    def test_master_entity_id_is_nullable_self_fk(self, sqlite_engine):
        """canonical_entity.master_entity_id must be nullable (self-FK reserved for cross-state linking)."""
        inspector = inspect(sqlite_engine)
        columns = {c["name"]: c for c in inspector.get_columns("canonical_entity")}
        assert "master_entity_id" in columns, "master_entity_id column missing"
        assert columns["master_entity_id"]["nullable"] is True, (
            "master_entity_id must be nullable"
        )

    def test_entity_type_column_present(self, sqlite_engine):
        inspector = inspect(sqlite_engine)
        columns = {c["name"] for c in inspector.get_columns("canonical_entity")}
        assert "entity_type" in columns

    def test_required_columns_present(self, sqlite_engine):
        inspector = inspect(sqlite_engine)
        columns = {c["name"] for c in inspector.get_columns("canonical_entity")}
        required = {
            "id", "uuid", "entity_type", "canonical_name", "normalized_name",
            "state_code", "first_seen_date", "last_seen_date",
            "source_record_count", "created_at", "updated_at",
        }
        missing = required - columns
        assert not missing, f"canonical_entity missing columns: {missing}"


class TestCanonicalCampaignStructure:
    def test_identity_tuple_unique_constraint(self):
        """canonical_campaign must enforce uniqueness on (committee_entity_id, office_normalized, election_cycle)."""
        table = SQLModel.metadata.tables["canonical_campaign"]
        unique_constraints = [
            c for c in table.constraints
            if isinstance(c, UniqueConstraint)
        ]
        identity_cols = frozenset({"committee_entity_id", "office_normalized", "election_cycle"})
        found = any(
            frozenset(col.name for col in uc.columns) == identity_cols
            for uc in unique_constraints
        )
        assert found, (
            "canonical_campaign must have a UniqueConstraint on "
            "(committee_entity_id, office_normalized, election_cycle)"
        )

    def test_required_columns_present(self, sqlite_engine):
        inspector = inspect(sqlite_engine)
        columns = {c["name"] for c in inspector.get_columns("canonical_campaign")}
        required = {
            "id", "uuid", "committee_entity_id", "office_normalized",
            "election_cycle", "state_code", "created_at", "updated_at",
        }
        missing = required - columns
        assert not missing, f"canonical_campaign missing columns: {missing}"

    def test_election_cycle_is_non_nullable(self, sqlite_engine):
        """election_cycle is required on canonical_campaign (identity tuple member)."""
        inspector = inspect(sqlite_engine)
        columns = {c["name"]: c for c in inspector.get_columns("canonical_campaign")}
        assert "election_cycle" in columns
        assert columns["election_cycle"]["nullable"] is False

    def test_candidate_entity_id_is_nullable(self, sqlite_engine):
        inspector = inspect(sqlite_engine)
        columns = {c["name"]: c for c in inspector.get_columns("canonical_campaign")}
        assert "candidate_entity_id" in columns, "candidate_entity_id column missing"
        assert columns["candidate_entity_id"]["nullable"] is True, (
            "candidate_entity_id must be nullable"
        )


class TestCanonicalAddressStructure:
    def test_required_columns_present(self, sqlite_engine):
        inspector = inspect(sqlite_engine)
        columns = {c["name"] for c in inspector.get_columns("canonical_address")}
        required = {
            "id", "uuid", "standardized_line_1", "city", "state",
            "zip5", "parse_status", "frequency", "created_at", "updated_at",
        }
        missing = required - columns
        assert not missing, f"canonical_address missing columns: {missing}"

    def test_no_unique_constraint_on_address(self):
        """canonical_address_id on canonical_entity must be many-to-one; no uniqueness."""
        entity_table = SQLModel.metadata.tables["canonical_entity"]
        unique_constraints = [
            c for c in entity_table.constraints
            if isinstance(c, UniqueConstraint)
        ]
        for uc in unique_constraints:
            col_names = {col.name for col in uc.columns}
            assert "canonical_address_id" not in col_names, (
                "canonical_address_id must NOT have a unique constraint "
                "(many entities can share one address)"
            )


class TestCanonicalNameHistoryStructure:
    def test_required_columns_present(self, sqlite_engine):
        inspector = inspect(sqlite_engine)
        columns = {c["name"] for c in inspector.get_columns("canonical_name_history")}
        required = {
            "id", "subject_type", "subject_id", "name",
            "first_seen_date", "last_seen_date", "occurrence_count", "created_at",
        }
        missing = required - columns
        assert not missing, f"canonical_name_history missing columns: {missing}"

    def test_subject_normalized_name_unique_constraint(self):
        """One normalized name per subject."""
        table = SQLModel.metadata.tables["canonical_name_history"]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, UniqueConstraint)
        ]
        expected = frozenset({"subject_type", "subject_id", "normalized_name"})
        found = any(
            frozenset(col.name for col in uc.columns) == expected
            for uc in unique_constraints
        )
        assert found, (
            "canonical_name_history must have a UniqueConstraint on "
            "(subject_type, subject_id, normalized_name)"
        )


class TestUnifiedEntityTypeMapping:
    def test_direct_mappings(self):
        assert map_unified_to_canonical_entity_type("person") == EntityType.person
        assert map_unified_to_canonical_entity_type("organization") == EntityType.organization
        assert map_unified_to_canonical_entity_type("committee") == EntityType.committee

    def test_vendor_maps_to_organization(self):
        assert map_unified_to_canonical_entity_type("vendor") == EntityType.organization
        assert (
            map_unified_to_canonical_entity_type(UnifiedEntityType.VENDOR)
            == EntityType.organization
        )

    def test_other_maps_to_organization(self):
        assert map_unified_to_canonical_entity_type("other") == EntityType.organization

    def test_campaign_is_unmapped(self):
        with pytest.raises(UnmappedEntityTypeError, match="campaign"):
            map_unified_to_canonical_entity_type("campaign")

    def test_unknown_type_raises(self):
        with pytest.raises(UnmappedEntityTypeError):
            map_unified_to_canonical_entity_type("unknown_type")


@pytest.mark.integration
def test_canonical_entity_committee_round_trip_on_postgres():
    url = os.environ.get("DATABASE_URL") or os.environ.get("TEST_DATABASE_URL")
    if not url or not url.startswith("postgresql"):
        pytest.skip("DATABASE_URL postgresql not set")

    engine = create_engine(url)
    SQLModel.metadata.create_all(engine, tables=[CanonicalEntity.__table__])
    with Session(engine) as session:
        entity = CanonicalEntity(
            entity_type=EntityType.committee,
            canonical_name="test pac",
            normalized_name="test pac",
            state_code="TX",
        )
        session.add(entity)
        session.flush()
        stored = session.execute(
            text("SELECT entity_type::text FROM canonical_entity WHERE id = :id"),
            {"id": entity.id},
        ).scalar_one()
        session.rollback()
        assert stored == "COMMITTEE"


class TestCanonicalEntityTypePostgresBinding:
    def test_bind_param_uppercases_for_entitytype_enum(self):
        col_type = CanonicalEntityTypeType()
        assert col_type.process_bind_param(EntityType.committee, None) == "COMMITTEE"
        assert col_type.process_bind_param(EntityType.person, None) == "PERSON"

    def test_result_value_lowercases_to_python_enum(self):
        col_type = CanonicalEntityTypeType()
        assert col_type.process_result_value("COMMITTEE", None) == EntityType.committee
        assert col_type.process_result_value("ORGANIZATION", None) == EntityType.organization


def test_app_resolve_package_imports():
    """app.resolve package exports canonical and resolution models."""
    import app.resolve as resolve_pkg
    from app.resolve.models import CanonicalEntity, EntityCrosswalk

    assert resolve_pkg is not None
    assert CanonicalEntity is not None
    assert EntityCrosswalk is not None
