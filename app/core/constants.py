"""Shared constants and SQLAlchemy types for unified models."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import Numeric

from app.core.enums import TransactionType

MONEY_TYPE = Numeric(15, 2)
DEFAULT_STATE: str | None = None

RECORD_TYPE_CODES: frozenset[str] = frozenset(
    {"RCPT", "EXPN", "LOAN", "PLDG", "DEBT", "CRED", "TRVL", "ASSET"}
)

PLACEHOLDER_NAMES: frozenset[str] = frozenset(
    {
        "NON-ITEMIZED CONTRIBUTOR",
        "NON-ITEMIZED",
        "UNKNOWN",
        "ANONYMOUS",
    }
)

AMOUNT_BUCKETS: tuple[tuple[Decimal, Decimal], ...] = (
    (Decimal("0"), Decimal("50")),
    (Decimal("50"), Decimal("200")),
    (Decimal("200"), Decimal("1000")),
    (Decimal("1000"), Decimal("10000")),
    (Decimal("10000"), Decimal("999999999")),
)

RECORD_TYPE_TO_TRANSACTION: dict[str, TransactionType] = {
    "RCPT": TransactionType.CONTRIBUTION,
    "EXPN": TransactionType.EXPENDITURE,
    "LOAN": TransactionType.LOAN,
    "PLDG": TransactionType.PLEDGE,
    "DEBT": TransactionType.DEBT,
    "CRED": TransactionType.CREDIT,
    "TRVL": TransactionType.TRAVEL,
    "ASSET": TransactionType.ASSET,
}
