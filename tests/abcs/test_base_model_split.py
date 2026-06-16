"""Tests for Base/Create/Read/Table validator model hierarchy (TASK-4e)."""

from __future__ import annotations

from datetime import date, datetime, timezone

from app.abcs.base_models import (
    BaseValidatorModel,
    CreateValidatorModel,
    ReadValidatorModel,
)
from app.states.texas.validators.texas_contributions import (
    TECContribution,
    TECContributionCreate,
    TECContributionRead,
)

# ---------------------------------------------------------------------------
# Base hierarchy
# ---------------------------------------------------------------------------


def test_base_validator_imports():
    assert BaseValidatorModel is not None
    assert CreateValidatorModel is not None
    assert ReadValidatorModel is not None


def test_create_validator_model_has_no_id_field():
    assert "id" not in CreateValidatorModel.model_fields


def test_read_validator_model_has_id_and_created_at():
    assert "id" in ReadValidatorModel.model_fields
    assert "created_at" in ReadValidatorModel.model_fields


def test_base_validator_ignores_extra_fields():
    instance = BaseValidatorModel.model_validate({"extra_field": "x"})
    assert not hasattr(instance, "extra_field") or getattr(instance, "extra_field", None) is None


# ---------------------------------------------------------------------------
# Texas contributions
# ---------------------------------------------------------------------------

_TEXAS_CONTRIB_MINIMAL = {
    "recordType": "RCPT",
    "formTypeCd": "COH",
    "schedFormTypeCd": "A1",
    "reportInfoIdent": 1,
    "filerIdent": 100,
    "filerTypeCd": "COH",
    "filerName": "Test Filer",
    "contributionInfoId": 999,
    "contributionAmount": 100.0,
    "contributorPersentTypeCd": "ENTITY",
    "contributorNameOrganization": "Acme PAC",
    "contributorPacFein": None,
    "file_origin": "test.csv",
    "download_date": date(2024, 1, 20),
    "receivedDt": date(2024, 1, 15),
    "contributionDt": date(2024, 1, 10),
}


def test_texas_contribution_create_succeeds_with_required_fields():
    record = TECContributionCreate.model_validate(_TEXAS_CONTRIB_MINIMAL)
    assert record.contributionInfoId == 999


def test_texas_contribution_create_parses_raw_yyyymmdd_contribution_date():
    """AddressValidatedModel.format_dates runs before field coercion (MRO)."""
    data = {**_TEXAS_CONTRIB_MINIMAL, "contributionDt": "20240115"}
    record = TECContributionCreate.model_validate(data)
    assert isinstance(record.contributionDt, date)
    assert record.contributionDt == date(2024, 1, 15)


def test_texas_contribution_create_has_no_id_field():
    assert "id" not in TECContributionCreate.model_fields


def test_texas_contribution_create_ignores_supplied_id():
    data = {**_TEXAS_CONTRIB_MINIMAL, "id": "user-supplied-id"}
    record = TECContributionCreate.model_validate(data)
    assert "id" not in record.model_fields_set
    assert not hasattr(record, "id") or getattr(record, "id", None) is None


def test_texas_contribution_read_includes_id_and_created_at():
    created = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    read = TECContributionRead.model_validate(
        {**_TEXAS_CONTRIB_MINIMAL, "id": "abc123hash", "created_at": created}
    )
    assert read.id == "ABC123HASH"  # TECSettings applies str_to_upper
    assert read.created_at == created


def test_texas_contribution_table_still_maps_to_db():
    assert TECContribution.__tablename__ == "tx_contributions"
    assert "id" in TECContribution.model_fields


# ---------------------------------------------------------------------------
# Oklahoma expenditure
# ---------------------------------------------------------------------------

_OK_EXPENDITURE_MINIMAL = {
    "expenditure_type": "MONETARY",
    "expenditure_date": "01/15/2024",
    "filed_date": "01/20/2024",
    "amended": "N",
    "download_date": date(2024, 1, 21),
    "file_origin": "test.csv",
    "expenditure_id": 42,
}


def test_oklahoma_expenditure_create_succeeds(ok_expenditure_models):
    record = ok_expenditure_models.OklahomaExpenditureCreate.model_validate(_OK_EXPENDITURE_MINIMAL)
    assert record.expenditure_id == 42


def test_oklahoma_expenditure_create_has_no_id_field(ok_expenditure_models):
    assert "id" not in ok_expenditure_models.OklahomaExpenditureCreate.model_fields


def test_oklahoma_expenditure_create_rejects_user_supplied_id_via_extra_ignore(
    ok_expenditure_models,
):
    data = {**_OK_EXPENDITURE_MINIMAL, "id": 999}
    record = ok_expenditure_models.OklahomaExpenditureCreate.model_validate(data)
    assert "id" not in record.model_fields_set


def test_oklahoma_expenditure_read_includes_id_and_created_at(ok_expenditure_models):
    created = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    read = ok_expenditure_models.OklahomaExpenditureRead.model_validate(
        {**_OK_EXPENDITURE_MINIMAL, "id": 7, "created_at": created}
    )
    assert read.id == 7
    assert read.created_at == created


def test_oklahoma_expenditure_table_still_maps_to_db(ok_expenditure_models):
    assert ok_expenditure_models.OklahomaExpenditure.__tablename__ == "expenditures"
    assert "id" in ok_expenditure_models.OklahomaExpenditure.model_fields
