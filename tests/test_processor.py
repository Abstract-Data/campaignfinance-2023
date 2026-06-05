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
