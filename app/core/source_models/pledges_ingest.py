"""Construct UnifiedPledge rows from PLDG source records."""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from app.core.source_models.pledges import UnifiedPledge


def _get_field_value(raw: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in raw and raw[key] is not None:
            return raw[key]
    return None


def _parse_amount(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = re.sub(r"[^\d.-]", "", value)
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                continue
    return None


def _parse_boolean(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().upper() in {"Y", "YES", "TRUE", "1"}


def build_pledge(
    transaction: Any,
    pledgor_entity: Any,
    recipient_entity: Any,
    raw: dict[str, Any],
    *,
    state_id: int,
) -> UnifiedPledge:
    """Build a pledge detail row from a transaction and entity pair."""
    amount = _parse_amount(_get_field_value(raw, "pledgeAmount", "amount")) or getattr(
        transaction, "amount", None
    )
    pledge_date = _parse_date(
        _get_field_value(raw, "pledgeDt", "transaction_date", "pledge_date")
    ) or getattr(transaction, "transaction_date", None)
    description = _get_field_value(raw, "pledgeDescr", "description") or getattr(
        transaction, "description", None
    )

    transaction_id = getattr(transaction, "id", None)
    pledgor_entity_id = getattr(pledgor_entity, "id", None)
    recipient_entity_id = getattr(recipient_entity, "id", None)

    return UnifiedPledge(
        transaction_id=transaction_id,
        pledgor_entity_id=pledgor_entity_id,
        recipient_entity_id=recipient_entity_id,
        state_id=state_id,
        amount=amount,
        pledge_date=pledge_date,
        is_fulfilled=_parse_boolean(
            _get_field_value(raw, "is_fulfilled", "fulfilledFlag", "pledgeFulfilledFlag")
        ),
        description=description,
        metadata_json=json.dumps(raw, default=str) if raw else None,
    )
