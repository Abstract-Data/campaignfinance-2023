"""Targeted unit tests for Wave 5 coverage uplift (builders, loader, csv_reader)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
from sqlmodel import Session, SQLModel, create_engine

import app.core.models  # noqa: F401 — register unified tables
from app.core.builders import UnifiedSQLModelBuilder
from app.core.enums import (
    CommitteeRole,
    EntityType,
    PersonRole,
    PersonType,
    TransactionType,
)
from app.core.models import (
    State,
    UnifiedAddress,
    UnifiedCampaign,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
    UnifiedTransaction,
)
from app.core.processor import ProcessStats
from app.core.unified_database import get_db_manager, reset_db_manager_cache
from app.core.unified_state_loader import (
    UnifiedStateLoader,
    _load_committee_index,
    _load_person_index,
    load_state_data,
)
from app.funcs.csv_reader import FileReader


@pytest.fixture
def sqlite_engine(tmp_path: Path):
    db_path = tmp_path / "wave5.db"
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    for table in SQLModel.metadata.sorted_tables:
        if table.schema is None:
            table.create(engine, checkfirst=True)
    return engine


class TestFileReaderParquet:
    def test_read_csv_iso8859_fallback(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "latin.csv"
        csv_path.write_bytes(b"name,value\nJos\xe9,1\n")
        rows = list(FileReader().read_csv(csv_path))
        assert rows[0]["name"] == "José"

    def test_read_folder_yields_csv_rows(self, tmp_path: Path) -> None:
        (tmp_path / "one.csv").write_text("key\nval\n", encoding="utf-8")
        rows = list(FileReader().read_folder(tmp_path))
        assert rows[0]["key"] == "val"

    def test_read_dispatches_csv(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "sample.csv"
        csv_path.write_text("filerIdent,contributionAmount\n99001,100.00\n", encoding="utf-8")
        rows = list(FileReader().read(csv_path))
        assert rows[0]["filerIdent"] == "99001"

    def test_read_dispatches_parquet(self, tmp_path: Path) -> None:
        parquet_path = tmp_path / "sample.parquet"
        pl.DataFrame({"filerIdent": ["99001"], "amount": [100]}).write_parquet(parquet_path)
        rows = list(FileReader().read(parquet_path))
        assert rows[0]["filerIdent"] == "99001"

    def test_read_parquet_yields_dict_rows(self, tmp_path: Path) -> None:
        parquet_path = tmp_path / "rows.parquet"
        pl.DataFrame({"a": [1], "b": [2]}).write_parquet(parquet_path)
        rows = list(FileReader().read_parquet(parquet_path))
        assert rows == [{"a": 1, "b": 2}]


class TestBuilderFieldResolution:
    def test_direct_unified_field_lookup(self) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        assert builder._get_field_value({"amount": "10"}, "amount") == "10"

    def test_strict_field_resolution_raises(self) -> None:
        builder = UnifiedSQLModelBuilder(
            "texas", state_id=1, state_code="TX", strict_field_resolution=True
        )
        with pytest.raises(ValueError, match="No explicit mapping"):
            builder._get_field_value({"unknown_field_xyz": "x"}, "amount")

    def test_fuzzy_match_two_word_overlap(self) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        assert builder._fuzzy_match("contributor_name_last", "person_name_last") is True

    def test_normalize_entity_name_strips_punctuation(self) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        assert builder._normalize_entity_name("  ACME, LLC!  ") == "acme llc"

    def test_parse_amount_and_date_helpers(self) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        assert builder._parse_amount("$1,234.56") == builder._parse_amount("1234.56")
        assert builder._parse_date("2024-03-15").year == 2024
        assert builder._parse_boolean("yes") is True

    def test_determine_transaction_type_from_record_type(self) -> None:
        builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX")
        raw = {"record_type": "RCPT"}
        assert builder._determine_transaction_type(raw) == TransactionType.CONTRIBUTION

    def test_find_address_requires_two_fields(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX", session=session)
            assert builder._find_address_by_fields({"street_1": "1 Main"}) is None
            session.add(
                UnifiedAddress(
                    street_1="1 Main St",
                    city="Austin",
                    state="TX",
                    zip_code="78701",
                )
            )
            session.commit()
            found = builder._find_address_by_fields(
                {"street_1": "1 Main St", "city": "Austin", "state": "TX"}
            )
        assert found is not None

    def test_build_campaign_uses_committee_name(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            committee = UnifiedCommittee(filer_id="F1", name="Citizens PAC", state_id=1)
            session.add(committee)
            session.commit()
            builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX", session=session)
            txn = UnifiedTransaction(
                transaction_date=date(2024, 5, 1),
                transaction_type=TransactionType.CONTRIBUTION,
                state_id=1,
            )
            campaign = builder.build_campaign(
                {"record_type": "RCPT"},
                committee=committee,
                candidate=None,
                transaction=txn,
            )
        assert isinstance(campaign, UnifiedCampaign)
        assert campaign.name == "Citizens PAC"

    def test_build_person_and_address_with_session(self, sqlite_engine) -> None:
        raw = {
            "person_first_name": "John",
            "person_last_name": "Smith",
            "person_employer": "ACME Corp",
            "address_street_1": "1 Main St",
            "address_city": "Austin",
            "address_state": "TX",
            "address_zip": "78701",
        }
        with Session(sqlite_engine) as session:
            builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX", session=session)
            person = builder.build_person(raw, PersonRole.CONTRIBUTOR)
        assert person is not None
        assert person.person_type == PersonType.INDIVIDUAL
        assert person.employer == "ACME Corp"
        assert person.address is not None

    def test_build_committee_candidate_name_fallback(self, sqlite_engine) -> None:
        raw = {
            "committee_type": "candidate committee",
            "Candidate Name": "Jane Candidate",
            "committee_filer_id": "FC-9",
        }
        with Session(sqlite_engine) as session:
            builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX", session=session)
            committee = builder.build_committee(raw)
        assert committee is not None
        assert "Jane Candidate" in (committee.name or "")

    def test_get_or_create_entity_returns_existing(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            existing = UnifiedEntity(
                entity_type=EntityType.PERSON,
                name="Existing Person",
                normalized_name="existing person",
                state_id=1,
            )
            session.add(existing)
            session.commit()
            builder = UnifiedSQLModelBuilder("texas", state_id=1, state_code="TX", session=session)
            entity = builder._get_or_create_entity(
                EntityType.PERSON,
                "Existing Person",
                None,
            )
        assert entity is not None
        assert entity.id == existing.id


class TestUnifiedStateLoaderUnit:
    def test_discover_data_files_finds_csv_and_parquet(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "texas"
        state_dir.mkdir()
        (state_dir / "contributions.csv").write_text("a\n1\n", encoding="utf-8")
        (state_dir / "committees.parquet").write_bytes(b"")
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        files = loader._discover_data_files()
        names = {f.name for f in files}
        assert "contributions.csv" in names
        assert "committees.parquet" in names

    def test_extract_officer_from_record_texas(self, tmp_path: Path) -> None:
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        record = {
            "filer_id": "C-100",
            "treasurer_name": "Jane Treasurer",
            "chair_name": "Bob Chair",
            "committee_name": "Test PAC",
        }
        result = loader._extract_officer_from_record(record)
        assert result is not None
        assert result["committee_id"] == "C-100"
        roles = {o["role"] for o in result["officers"]}
        assert CommitteeRole.TREASURER in roles
        assert CommitteeRole.CHAIR in roles

    def test_extract_officer_returns_none_without_committee_id(self, tmp_path: Path) -> None:
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        assert loader._extract_officer_from_record({"treasurer_name": "Jane"}) is None

    def test_generate_summary_report_shape(self, tmp_path: Path) -> None:
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        loader.stats["files_processed"] = 2
        summary = loader._generate_summary_report()
        assert summary["state"] == "TEXAS"
        assert summary["summary"]["total_files_processed"] == 2

    def test_load_state_data_raises_when_no_files(self, tmp_path: Path) -> None:
        (tmp_path / "texas").mkdir()
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        with pytest.raises(ValueError, match="No data files"):
            loader.load_state_data()

    def test_process_records_batch_empty_returns_zero_stats(self, tmp_path: Path) -> None:
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        stats = loader.process_records_batch([])
        assert stats.total == 0

    def test_person_matches_officer_by_name(self, sqlite_engine, tmp_path: Path) -> None:
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        with Session(sqlite_engine) as session:
            person = UnifiedPerson(
                first_name="Pat",
                last_name="O'Brien",
                person_type="individual",
                state_id=1,
            )
            session.add(person)
            session.commit()
            session.refresh(person)
            officer = {"name": "Pat O'Brien", "role": CommitteeRole.TREASURER}
            assert loader._person_matches_officer(person.id, officer, session) is True

    def test_find_or_create_person_single_name_returns_none(
        self, sqlite_engine, tmp_path: Path
    ) -> None:
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        with Session(sqlite_engine) as session:
            result = loader._find_or_create_person("Madonna", session, {}, state_id=1)
        assert result is None


class TestLoaderIndexHelpers:
    def test_load_committee_index_none_state_id(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            assert _load_committee_index(session, None) == {}

    def test_load_person_index_none_state_id(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            assert _load_person_index(session, None) == {}

    def test_load_committee_index_populated(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            session.add(State(code="TX", name="Texas"))
            session.add(
                UnifiedCommittee(
                    filer_id="F1",
                    name="Alpha PAC",
                    state_id=1,
                )
            )
            session.commit()
            index = _load_committee_index(session, 1)
        assert index["alpha pac"] == "F1"

    def test_load_person_index_populated(self, sqlite_engine) -> None:
        with Session(sqlite_engine) as session:
            session.add(State(code="TX", name="Texas"))
            person = UnifiedPerson(
                first_name="Sam",
                last_name="Jones",
                person_type="individual",
                state_id=1,
            )
            session.add(person)
            session.commit()
            session.refresh(person)
            with Session(sqlite_engine) as session2:
                index = _load_person_index(session2, 1)
        assert index[("sam", "jones")] == person.id


class TestLoadStateDataConvenience:
    def test_load_state_data_delegates_to_loader(self, tmp_path: Path) -> None:
        (tmp_path / "texas").mkdir()
        mock_loader = MagicMock()
        mock_loader.load_state_data.return_value = {"state": "TEXAS"}
        with patch(
            "app.core.unified_state_loader.UnifiedStateLoader",
            return_value=mock_loader,
        ):
            result = load_state_data("texas", tmp_path, auto_link_officers=False)
        assert result["state"] == "TEXAS"
        mock_loader.load_state_data.assert_called_once()

    def test_extract_committee_officers_from_csv(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "texas"
        state_dir.mkdir()
        officer_file = state_dir / "committee_filers.csv"
        officer_file.write_text(
            "filer_id,treasurer_name,chair_name\n"
            "C1,Jane Treasurer,Bob Chair\n",
            encoding="utf-8",
        )
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        loader._extract_committee_officers([officer_file])
        assert "C1" in loader.committee_officers
        assert len(loader.committee_officers["C1"][0]["officers"]) == 2

    def test_create_committee_relationships_persists_person(
        self, sqlite_engine, tmp_path: Path
    ) -> None:
        reset_db_manager_cache()
        manager = get_db_manager(database_url=str(sqlite_engine.url), bootstrap=False)
        manager.engine = sqlite_engine
        with manager.get_session() as session:
            session.add(State(code="TX", name="Texas"))
            session.commit()

        loader = UnifiedStateLoader("texas", tmp_path, db_manager=manager)
        loader.committee_officers = {
            "C1": [
                {
                    "officers": [
                        {
                            "name": "Jane Doe",
                            "role": CommitteeRole.TREASURER,
                            "committee_id": "C1",
                        }
                    ]
                }
            ]
        }
        loader._create_committee_relationships()
        assert loader.stats["committee_relationships_created"] >= 1

    def test_process_data_file_with_csv(self, tmp_path: Path) -> None:
        csv_path = tmp_path / "contributions.csv"
        csv_path.write_text(
            "record_type,filerIdent,contributionAmount,contributionDt\n"
            "RCPT,99001,25.00,2024-02-01\n",
            encoding="utf-8",
        )
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=MagicMock())
        batch_stats = ProcessStats(success=1)
        with patch.object(loader, "process_records_batch", return_value=batch_stats):
            file_stats = loader._process_data_file(csv_path, auto_link_officers=False)
        assert file_stats["transactions"] == 1

    def test_link_transaction_to_officers_when_role_exists(
        self, sqlite_engine, tmp_path: Path
    ) -> None:
        reset_db_manager_cache()
        manager = get_db_manager(database_url=str(sqlite_engine.url), bootstrap=False)
        manager.engine = sqlite_engine
        with manager.get_session() as session:
            session.add(State(code="TX", name="Texas"))
            committee = UnifiedCommittee(filer_id="C9", name="PAC", state_id=1)
            person = UnifiedPerson(
                first_name="Pat",
                last_name="Lee",
                person_type="individual",
                state_id=1,
            )
            session.add(committee)
            session.add(person)
            session.commit()
            session.refresh(person)
            txn = UnifiedTransaction(
                committee_id="C9",
                transaction_type=TransactionType.CONTRIBUTION,
                state_id=1,
            )
            session.add(txn)
            session.commit()
            session.refresh(txn)
            from app.core.models import UnifiedCommitteePerson, UnifiedTransactionPerson

            cp = UnifiedCommitteePerson(
                committee_id="C9",
                person_id=person.id,
                role=CommitteeRole.TREASURER,
            )
            session.add(cp)
            txp = UnifiedTransactionPerson(
                transaction_id=txn.id,
                person_id=person.id,
                state_id=1,
                role=PersonRole.CONTRIBUTOR,
            )
            session.add(txp)
            session.commit()
            session.refresh(cp)
            session.refresh(txp)

            loader = UnifiedStateLoader("texas", tmp_path, db_manager=manager)
            loader.committee_officers = {
                "C9": [
                    {
                        "officers": [
                            {
                                "name": "Pat Lee",
                                "role": CommitteeRole.TREASURER,
                                "committee_id": "C9",
                            }
                        ]
                    }
                ]
            }
            loader._link_transaction_to_officers(txn, {}, session)
            session.refresh(txp)
            assert txp.committee_person_id == cp.id

    def test_auto_link_all_transactions_updates_stats(self, tmp_path: Path) -> None:
        db_manager = MagicMock()
        db_manager.auto_link_transactions_to_committee_roles.return_value = {"total": 3}
        loader = UnifiedStateLoader("texas", tmp_path, db_manager=db_manager)
        loader.committee_officers = {"C1": []}
        loader._auto_link_all_transactions()
        assert loader.stats["transaction_links_created"] == 3

    def test_load_state_data_pipeline_with_mocks(self, tmp_path: Path) -> None:
        state_dir = tmp_path / "texas"
        state_dir.mkdir()
        (state_dir / "committee_officers.csv").write_text(
            "filer_id,treasurer_name\nC1,Jane Treasurer\n",
            encoding="utf-8",
        )
        (state_dir / "contributions.csv").write_text(
            "record_type,filerIdent,contributionAmount,contributionDt\n"
            "RCPT,99001,100.00,2024-01-01\n",
            encoding="utf-8",
        )
        db_manager = MagicMock()
        batch_stats = ProcessStats(success=1, failures=0, db_errors=0, skipped=0)
        with patch.object(
            UnifiedStateLoader,
            "process_records_batch",
            return_value=batch_stats,
        ):
            loader = UnifiedStateLoader("texas", tmp_path, db_manager=db_manager)
            summary = loader.load_state_data(
                auto_link_officers=False,
                create_relationships=False,
            )
        assert summary["summary"]["total_files_processed"] >= 1
