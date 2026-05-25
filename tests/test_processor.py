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
    raw = _base_raw(record_type)
    if record_type in {"DEBT", "CRED"}:
        raw.update(
            {
                "contributorNameLast": "CREDITOR",
                "contributorNameFirst": "Test",
                "recipientNameLast": "DEBTOR",
                "recipientNameFirst": "Party",
            }
        )
    txn = processor.process_record(raw, "texas", state_id=1, state_code="TX")
    assert txn.transaction_type == expected_type
    assert getattr(txn, detail_attr) is not None


def test_process_record_stream_yields_lazily(processor: UnifiedSQLDataProcessor) -> None:
    records = [_base_raw("RCPT"), _base_raw("LOAN")]

    def _source() -> Iterator[dict]:
        yield from records

    results = list(
        processor.process_record_stream(_source(), "texas", state_id=1, state_code="TX")
    )
    assert len(results) == 2
    assert results[0].transaction_type == TransactionType.CONTRIBUTION
    assert results[1].transaction_type == TransactionType.LOAN
