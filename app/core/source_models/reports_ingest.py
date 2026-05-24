"""Ingest builders for TEC CVR1 (CoverSheet1Data) report records."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal

from sqlalchemy import text
from sqlmodel import Session

from app.core.source_models.reports import UnifiedReport


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text_val = str(value).strip()
    return text_val or None


def _parse_date(value: object) -> date | None:
    """Parse a TEC date string (yyyyMMdd) to a Python date."""
    raw = _optional_str(value)
    if raw is None:
        return None
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    for sep in ("-", "/"):
        parts = raw.split(sep)
        if len(parts) == 3:
            try:
                y, m, d = (int(p) for p in parts)
                return date(y, m, d)
            except ValueError:
                pass
    return None


def _parse_amount(value: object) -> Decimal | None:
    """Parse a TEC numeric string to Decimal, returning None for empty strings."""
    raw = _optional_str(value)
    if raw is None:
        return None
    raw = raw.replace(",", "").replace("$", "")
    try:
        return Decimal(raw)
    except Exception:
        return None


def build_report(
    raw: dict,
    *,
    state_id: int,
    file_origin_id: str | None = None,
) -> UnifiedReport:
    """Map a raw TEC CVR1 (CoverSheetData) record to a ``UnifiedReport``.

    Parameters
    ----------
    raw:
        Dictionary keyed by the original TEC column names from CFS-ReadMe.txt
        (e.g. ``reportInfoIdent``, ``formTypeCd``, ``filedDt``).
    state_id:
        FK to ``states.id`` for the filing state.
    file_origin_id:
        FK to ``file_origins.id`` for the source parquet file, or ``None``.
    """
    report_ident = _optional_str(raw.get("reportInfoIdent"))
    if report_ident is None:
        raise ValueError("CVR1 record is missing reportInfoIdent")

    return UnifiedReport(
        state_id=state_id,
        committee_id=_optional_str(raw.get("filerIdent")),
        report_ident=report_ident,
        form_type=_optional_str(raw.get("formTypeCd")),
        filed_date=_parse_date(raw.get("filedDt")),
        period_start=_parse_date(raw.get("periodStartDt")),
        period_end=_parse_date(raw.get("periodEndDt")),
        is_final=False,
        total_contributions=_parse_amount(raw.get("totalContribAmount")),
        total_unitemized_contributions=_parse_amount(
            raw.get("unitemizedContribAmount")
        ),
        total_expenditures=_parse_amount(raw.get("totalExpendAmount")),
        total_unitemized_expenditures=_parse_amount(
            raw.get("unitemizedExpendAmount")
        ),
        loan_balance=_parse_amount(raw.get("loanBalanceAmount")),
        contributions_maintained=_parse_amount(raw.get("contribsMaintainedAmount")),
        cash_on_hand=_parse_amount(raw.get("cashOnHandAmount")),
        file_origin_id=file_origin_id,
        raw_data=json.dumps(dict(raw)),
    )


def link_transactions_to_reports(session: Session) -> int:
    """Set ``report_id`` on ``unified_transactions`` rows that share
    ``(state_id, report_ident)`` with a ``unified_reports`` row.

    Only transactions whose ``report_id`` is currently NULL are updated so
    that re-running is safe (already-linked rows are left untouched).

    Parameters
    ----------
    session:
        Active SQLModel/SQLAlchemy session.  The caller is responsible for
        managing the enclosing transaction; this function commits internally
        so the rowcount is accurately reported.

    Returns
    -------
    int
        Number of transaction rows that were updated.
    """
    stmt = text(
        """
        UPDATE unified_transactions
        SET report_id = (
            SELECT r.id
            FROM unified_reports r
            WHERE r.report_ident = unified_transactions.report_ident
            LIMIT 1
        )
        WHERE unified_transactions.report_id IS NULL
          AND unified_transactions.report_ident IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM unified_reports r
              WHERE r.report_ident = unified_transactions.report_ident
          )
        """
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount
