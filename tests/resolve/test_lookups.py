"""Tests for EXCAT/CVR3 lookup models and ingest builders (task 0c)."""

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError
from sqlmodel import Field, Session, SQLModel, create_engine, select

from app.core.source_models.lookups import CommitteePurpose, ExpenditureCategory
from app.core.source_models.lookups_ingest import (
    build_committee_purpose,
    build_expenditure_category,
)


class _State(SQLModel, table=True):
    __tablename__ = "states"
    __table_args__ = {"extend_existing": True}

    id: int | None = Field(default=None, primary_key=True)
    code: str = Field(max_length=2)


class _UnifiedCommittee(SQLModel, table=True):
    __tablename__ = "unified_committees"
    __table_args__ = {"extend_existing": True}

    filer_id: str = Field(primary_key=True, max_length=100)
    name: str | None = None


SAMPLE_EXCAT = {
    "recordType": "EXCAT",
    "expendCategoryCodeValue": "POLAD",
    "expendCategoryCodeLabel": "Political Advertising",
}

SAMPLE_CVR3 = {
    "recordType": "CVR3",
    "formTypeCd": "GPAC",
    "reportInfoIdent": 12345678901,
    "receivedDt": "20240115",
    "infoOnlyFlag": "N",
    "filerIdent": "00012345",
    "filerTypeCd": "GPAC",
    "filerName": "Example PAC",
    "committeeActivityId": 98765432101,
    "subjectCategoryCd": "CANDIDATE",
    "subjectPositionCd": "SUPPORT",
    "subjectDescr": "Support Jane Doe for State Senate District 14",
}


def test_build_expenditure_category_maps_excat_fields() -> None:
    category = build_expenditure_category(SAMPLE_EXCAT)

    assert isinstance(category, ExpenditureCategory)
    assert category.code == "POLAD"
    assert category.description == "Political Advertising"
    assert category.created_at is not None
    assert category.updated_at is not None


def test_build_committee_purpose_maps_cvr3_fields() -> None:
    purpose = build_committee_purpose(SAMPLE_CVR3, state_id=43)

    assert isinstance(purpose, CommitteePurpose)
    assert purpose.committee_id == "00012345"
    assert purpose.report_ident == "12345678901"
    assert purpose.state_id == 43
    assert purpose.form_type == "GPAC"
    assert purpose.purpose_text == "Support Jane Doe for State Senate District 14"
    assert purpose.uuid
    assert purpose.created_at is not None
    assert purpose.updated_at is not None


@pytest.fixture(name="lookup_session")
def lookup_session_fixture():
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(
        engine,
        tables=[
            _State.__table__,
            _UnifiedCommittee.__table__,
            ExpenditureCategory.__table__,
            CommitteePurpose.__table__,
        ],
    )
    with Session(engine) as session:
        session.add(_State(id=43, code="TX"))
        session.add(_UnifiedCommittee(filer_id="00012345", name="Example PAC"))
        session.commit()
        yield session


def test_lookup_tables_create_and_expenditure_category_code_is_unique(
    lookup_session: Session,
) -> None:
    lookup_session.add(
        build_expenditure_category(
            {
                "recordType": "EXCAT",
                "expendCategoryCodeValue": "POLAD",
                "expendCategoryCodeLabel": "Political Advertising",
            }
        )
    )
    lookup_session.commit()

    lookup_session.add(
        build_expenditure_category(
            {
                "recordType": "EXCAT",
                "expendCategoryCodeValue": "POLAD",
                "expendCategoryCodeLabel": "Duplicate code",
            }
        )
    )
    with pytest.raises(IntegrityError):
        lookup_session.commit()

    lookup_session.rollback()

    categories = lookup_session.exec(select(ExpenditureCategory)).all()
    assert len(categories) == 1
    assert categories[0].code == "POLAD"

    purpose = build_committee_purpose(SAMPLE_CVR3, state_id=43)
    lookup_session.add(purpose)
    lookup_session.commit()

    stored = lookup_session.exec(select(CommitteePurpose)).one()
    assert stored.committee_id == "00012345"
    assert stored.purpose_text == SAMPLE_CVR3["subjectDescr"]
