"""Tests for UnifiedSQLDataProcessor (TASK-4a)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from app.core.enums import TransactionType
from app.core.processor import DETAIL_BUILDERS, UnifiedSQLDataProcessor


@pytest.fixture
def processor() -> UnifiedSQLDataProcessor:
    return UnifiedSQLDataProcessor()


def _base_raw(record_type: str, **extra: object) -> dict:
    data: dict = {
        "record_type": record_type,
        "filerIdent": "12345",
        "contributionAmount": "100.00",
        "contributionDt": "2024-01-15",
    }
    data.update(extra)
    return data


# Role-scoped name columns each detail builder needs, keyed by TEC record type.
# Mirrors the real TEC column naming (RCPT→contributor*, LOAN/DEBT→lender*,
# CRED→payor*, TRVL→traveller*) so each builder resolves its own role slot
# instead of relying on the removed contributor-prefix fallback.
_ROLE_NAME_FIELDS: dict[str, dict[str, str]] = {
    "RCPT": {"contributorNameFirst": "Test", "contributorNameLast": "DONOR"},
    "LOAN": {"lenderNameFirst": "Test", "lenderNameLast": "LENDER"},
    "DEBT": {"lenderNameFirst": "Test", "lenderNameLast": "CREDITOR"},
    "CRED": {"payorNameFirst": "Test", "payorNameLast": "PAYOR"},
    "TRVL": {"travellerNameFirst": "Test", "travellerNameLast": "TRAVELER"},
}


@pytest.mark.parametrize(
    ("record_type", "expected_type", "detail_attr"),
    [
        ("RCPT", TransactionType.CONTRIBUTION, "contribution"),
        ("LOAN", TransactionType.LOAN, "loan"),
        ("DEBT", TransactionType.DEBT, "debt"),
        ("CRED", TransactionType.CREDIT, "credit"),
        ("TRVL", TransactionType.TRAVEL, "travel"),
        ("ASSET", TransactionType.ASSET, "asset"),
    ],
)
def test_detail_builders_attach_expected_record(
    processor: UnifiedSQLDataProcessor,
    record_type: str,
    expected_type: TransactionType,
    detail_attr: str,
) -> None:
    assert expected_type in DETAIL_BUILDERS
    raw = _base_raw(record_type, **_ROLE_NAME_FIELDS.get(record_type, {}))
    txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
    assert txn.transaction_type == expected_type
    assert getattr(txn, detail_attr) is not None


def test_get_builder_returns_new_instance_per_call(
    processor: UnifiedSQLDataProcessor,
) -> None:
    b1 = processor.get_builder("texas", state_id=1, state_code="TX")
    b2 = processor.get_builder("texas", state_id=2, state_code="TX")
    assert b1 is not b2
    assert b1.state_id == 1
    assert b2.state_id == 2


def test_process_record_stream_yields_lazily(processor: UnifiedSQLDataProcessor) -> None:
    records = [_base_raw("RCPT"), _base_raw("LOAN")]

    def _source() -> Iterator[dict]:
        yield from records

    results = list(processor.process_record_stream(_source(), "texas", state_id=1, state_code="TX"))
    assert len(results) == 2
    assert results[0].transaction_type == TransactionType.CONTRIBUTION
    assert results[1].transaction_type == TransactionType.LOAN


# ---------------------------------------------------------------------------
# _build_guarantors — LOAN/DEBT guarantor extraction
# ---------------------------------------------------------------------------
from app.core.processor import _build_guarantors  # noqa: E402


def _guar(idx: int, **over: object) -> dict:
    """A raw row carrying guarantor *idx* with name/address fields."""
    base = {
        f"guarantorNameLast{idx}": "Hinojosa",
        f"guarantorNameFirst{idx}": "Juan",
        f"guarantorNameOrganization{idx}": None,
        f"guarantorPersentTypeCd{idx}": "INDIVIDUAL",
        f"guarantorStreetCity{idx}": "McAllen",
        f"guarantorStreetStateCd{idx}": "TX",
        f"guarantorStreetPostalCode{idx}": "78501",
    }
    base.update(over)
    return base


def test_build_guarantors_parses_single_slot() -> None:
    rows = _build_guarantors(_guar(1), state_id=1)
    assert len(rows) == 1
    g = rows[0]
    assert g.position == 1
    assert (g.last_name, g.first_name, g.city, g.state_code) == (
        "Hinojosa",
        "Juan",
        "McAllen",
        "TX",
    )
    assert g.person_type == "INDIVIDUAL"


def test_build_guarantors_keeps_slot_index_with_gaps() -> None:
    raw = {**_guar(1), **_guar(3, guarantorNameLast3="Lucio", guarantorNameFirst3="Eduardo")}
    rows = _build_guarantors(raw, state_id=1)
    assert [g.position for g in rows] == [1, 3]
    assert rows[1].last_name == "Lucio"


def test_build_guarantors_empty_when_no_names() -> None:
    assert _build_guarantors({"guarantorStreetCity1": "Austin"}, state_id=1) == []


def test_build_guarantors_includes_org_only() -> None:
    raw = {"guarantorNameOrganization2": "ACME Bank"}
    rows = _build_guarantors(raw, state_id=1)
    assert len(rows) == 1 and rows[0].position == 2 and rows[0].organization == "ACME Bank"


def test_build_guarantors_clips_to_column_widths() -> None:
    rows = _build_guarantors(
        _guar(1, guarantorStreetStateCd1="TEXAS", guarantorNameLast1="X" * 200),
        state_id=1,
    )
    assert rows[0].state_code == "TE"  # clipped to 2
    assert len(rows[0].last_name) == 100  # clipped to 100


def test_build_guarantors_blank_strings_are_none() -> None:
    rows = _build_guarantors(_guar(1, guarantorNameOrganization1="   "), state_id=1)
    assert rows[0].organization is None
