"""Ingest builders for TEC CVR1 (CoverSheet1Data) and FINL report records."""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal, InvalidOperation

from sqlalchemy import text
from sqlmodel import Session, select

from app.core.source_models.reports import UnifiedReport
from app.logger import Logger

_logger = Logger(__name__)


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text_val = str(value).strip()
    return text_val or None


def _parse_date(value: object) -> date | None:
    """Parse a TEC date string (yyyyMMdd, YYYY-MM-DD, or MM/DD/YYYY) to a date."""
    raw = _optional_str(value)
    if raw is None:
        return None
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    for sep in ("-", "/"):
        parts = raw.split(sep)
        if len(parts) != 3:
            continue
        try:
            a, b, c = (int(p) for p in parts)
        except ValueError:
            continue
        if a > 31:
            return date(a, b, c)
        if c > 31:
            return date(c, a, b)
    return None


def _parse_amount(value: object) -> Decimal | None:
    """Parse a TEC numeric string to Decimal, returning None for empty strings."""
    raw = _optional_str(value)
    if raw is None:
        return None
    raw = raw.replace(",", "").replace("$", "")
    try:
        return Decimal(raw)
    except (InvalidOperation, ValueError):
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

    # Treasurer name snapshot: branch on treasPersentTypeCd.
    treas_type = _optional_str(raw.get("treasPersentTypeCd"))
    if treas_type == "ENTITY":
        treasurer_name = _optional_str(raw.get("treasNameOrganization"))
    else:
        # INDIVIDUAL or missing — join first + last, skipping blanks.
        first = _optional_str(raw.get("treasNameFirst"))
        last = _optional_str(raw.get("treasNameLast"))
        parts = [p for p in (first, last) if p]
        treasurer_name = " ".join(parts) if parts else None

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
        committee_name_at_filing=_optional_str(raw.get("filerName")),
        treasurer_name_at_filing=treasurer_name,
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
            WHERE r.state_id = unified_transactions.state_id
              AND r.report_ident = unified_transactions.report_ident
            LIMIT 1
        )
        WHERE unified_transactions.report_id IS NULL
          AND unified_transactions.report_ident IS NOT NULL
          AND unified_transactions.state_id IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM unified_reports r
              WHERE r.state_id = unified_transactions.state_id
                AND r.report_ident = unified_transactions.report_ident
          )
        """
    )
    result = session.execute(stmt)
    session.commit()
    return result.rowcount


def backfill_report_at_filing(session: Session) -> int:
    """Backfill ``committee_name_at_filing`` and ``treasurer_name_at_filing``
    for existing ``unified_reports`` rows that have ``raw_data`` but whose
    at-filing columns are NULL.

    The function is dialect-aware: it uses PostgreSQL JSONB operators when
    connected to Postgres, and ``json_extract`` for SQLite.  Two UPDATE
    statements are issued — one for the committee name and one for the
    treasurer name — and the total rowcount is returned.

    The caller does not need to manage transactions; this function commits
    internally (mirroring :func:`link_transactions_to_reports`).

    Parameters
    ----------
    session:
        Active SQLModel/SQLAlchemy session bound to either SQLite or PostgreSQL.

    Returns
    -------
    int
        Combined number of rows updated across both UPDATE statements.
    """
    dialect = session.get_bind().dialect.name

    if dialect == "postgresql":
        stmt_committee = text(
            """
            UPDATE unified_reports
            SET committee_name_at_filing = NULLIF(TRIM(raw_data::jsonb ->> 'filerName'), '')
            WHERE committee_name_at_filing IS NULL
              AND raw_data IS NOT NULL
            """
        )
        stmt_treasurer = text(
            """
            UPDATE unified_reports
            SET treasurer_name_at_filing = CASE
                WHEN TRIM(raw_data::jsonb ->> 'treasPersentTypeCd') = 'ENTITY'
                    THEN NULLIF(TRIM(raw_data::jsonb ->> 'treasNameOrganization'), '')
                ELSE NULLIF(
                    TRIM(
                        CONCAT_WS(' ',
                            NULLIF(TRIM(raw_data::jsonb ->> 'treasNameFirst'), ''),
                            NULLIF(TRIM(raw_data::jsonb ->> 'treasNameLast'), '')
                        )
                    ),
                    ''
                )
            END
            WHERE treasurer_name_at_filing IS NULL
              AND raw_data IS NOT NULL
            """
        )
    else:
        # SQLite
        stmt_committee = text(
            """
            UPDATE unified_reports
            SET committee_name_at_filing = NULLIF(TRIM(json_extract(raw_data, '$.filerName')), '')
            WHERE committee_name_at_filing IS NULL
              AND raw_data IS NOT NULL
            """
        )
        stmt_treasurer = text(
            """
            UPDATE unified_reports
            SET treasurer_name_at_filing = CASE
                WHEN TRIM(json_extract(raw_data, '$.treasPersentTypeCd')) = 'ENTITY'
                    THEN NULLIF(TRIM(json_extract(raw_data, '$.treasNameOrganization')), '')
                ELSE CASE
                    WHEN NULLIF(TRIM(json_extract(raw_data, '$.treasNameFirst')), '') IS NOT NULL
                         AND NULLIF(TRIM(json_extract(raw_data, '$.treasNameLast')), '') IS NOT NULL
                        THEN TRIM(json_extract(raw_data, '$.treasNameFirst')) || ' '
                             || TRIM(json_extract(raw_data, '$.treasNameLast'))
                    WHEN NULLIF(TRIM(json_extract(raw_data, '$.treasNameFirst')), '') IS NOT NULL
                        THEN TRIM(json_extract(raw_data, '$.treasNameFirst'))
                    WHEN NULLIF(TRIM(json_extract(raw_data, '$.treasNameLast')), '') IS NOT NULL
                        THEN TRIM(json_extract(raw_data, '$.treasNameLast'))
                    ELSE NULL
                END
            END
            WHERE treasurer_name_at_filing IS NULL
              AND raw_data IS NOT NULL
            """
        )

    rowcount = 0
    rowcount += session.execute(stmt_committee).rowcount
    rowcount += session.execute(stmt_treasurer).rowcount
    session.commit()
    return rowcount


def treasurer_for_report(session: Session, report: "UnifiedReport"):
    """Return the :class:`~app.states.texas.validators.texas_filers.TECTreasurer`
    whose effective date range covers the report's ``filed_date``.

    The lookup joins through the ``TECTreasurerLink`` association table:
    ``unified_reports.committee_id`` is matched against
    ``TECTreasurerLink.filer_identity_id`` (which stores the integer
    ``filerIdent``), and ``TECTreasurerLink.treasurer_id`` links to
    ``TECTreasurer.treasId``.

    Date-range logic:
    - ``TECTreasurer.treasEffStartDt <= report.filed_date``
    - ``TECTreasurer.treasEffStopDt >= report.filed_date``  OR  stop date is NULL
      (NULL stop date means the treasurer is still active / open-ended).

    Parameters
    ----------
    session:
        Active SQLModel/SQLAlchemy session (must have the ``texas`` schema
        attached or be connected to a Postgres database that contains it).
    report:
        A :class:`~app.core.source_models.reports.UnifiedReport` instance.
        ``committee_id`` and ``filed_date`` must be non-NULL for a match
        to be possible.

    Returns
    -------
    TECTreasurer | None
        The matching treasurer record, or ``None`` if no match is found
        (e.g. no treasurer link exists, or the report has no ``filed_date``).
    """
    from app.states.texas.validators.texas_filers import TECTreasurer, TECTreasurerLink

    if report.committee_id is None or report.filed_date is None:
        return None

    try:
        filer_ident = int(report.committee_id)
    except (ValueError, TypeError):
        return None

    stmt = (
        select(TECTreasurer)
        .join(TECTreasurerLink, TECTreasurerLink.treasurer_id == TECTreasurer.treasId)
        .where(TECTreasurerLink.filer_identity_id == filer_ident)
        .where(TECTreasurer.treasEffStartDt <= report.filed_date)
        .where(
            (TECTreasurer.treasEffStopDt >= report.filed_date)
            | (TECTreasurer.treasEffStopDt.is_(None))
        )
    )
    return session.exec(stmt).first()


def reconcile_report_totals(
    session: Session,
    *,
    tolerance: Decimal = Decimal("1.00"),
    sample_size: int = 100,
) -> dict[str, int]:
    """Compare declared report totals to summed linked transactions.

    Samples up to *sample_size* reports that have linked transactions and logs
    mismatches beyond *tolerance*. This is a data-quality signal, not a hard gate.

    Returns
    -------
    dict[str, int]
        Summary counts: ``checked``, ``matched``, ``mismatched``, ``skipped``.
    """
    from app.core.enums import TransactionType
    from app.core.models import UnifiedTransaction

    stmt = text(
        """
        SELECT r.id, r.report_ident, r.total_contributions, r.total_expenditures
        FROM unified_reports r
        WHERE EXISTS (
            SELECT 1 FROM unified_transactions t
            WHERE t.report_id = r.id
        )
        LIMIT :limit
        """
    )
    rows = session.execute(stmt, {"limit": sample_size}).fetchall()

    checked = 0
    matched = 0
    mismatched = 0
    skipped = 0

    for row in rows:
        report_id, report_ident, declared_contrib, declared_exp = row
        checked += 1

        if declared_contrib is None and declared_exp is None:
            skipped += 1
            continue

        txns = session.exec(
            select(UnifiedTransaction).where(UnifiedTransaction.report_id == report_id)
        ).all()

        if not txns:
            skipped += 1
            continue

        summed_contrib = sum(
            (t.amount or Decimal(0))
            for t in txns
            if t.transaction_type == TransactionType.CONTRIBUTION
        )
        summed_exp = sum(
            (t.amount or Decimal(0))
            for t in txns
            if t.transaction_type == TransactionType.EXPENDITURE
        )

        def _within(declared: Decimal | None, summed: Decimal) -> bool:
            if declared is None:
                return True
            return abs(summed - declared) <= tolerance

        contrib_ok = _within(declared_contrib, summed_contrib)
        exp_ok = _within(declared_exp, summed_exp)

        if contrib_ok and exp_ok:
            matched += 1
        else:
            mismatched += 1
            _logger.warning(
                f"[reconcile] report {report_ident!r} (id={report_id}): "
                f"declared_contrib={declared_contrib} vs summed={summed_contrib}, "
                f"declared_exp={declared_exp} vs summed={summed_exp} "
                f"(tolerance={tolerance})"
            )

    _logger.info(
        f"[reconcile] checked={checked} matched={matched} "
        f"mismatched={mismatched} skipped={skipped}"
    )
    return {
        "checked": checked,
        "matched": matched,
        "mismatched": mismatched,
        "skipped": skipped,
    }


def build_final_report(
    raw: dict,
    *,
    state_id: int,
    session: Session | None = None,
    file_origin_id: str | None = None,
) -> UnifiedReport | None:
    """Handle a TEC FINL record by marking the matching UnifiedReport.is_final = True.

    FINL rows do not create a new row — they mutate the existing CVR1 report.
    Returns the updated UnifiedReport if found, or ``None`` if the report does
    not yet exist (e.g. CVR1 was not loaded or the ident is unknown).

    Columns present in TEC finals_*.csv:
        recordType, formTypeCd, reportInfoIdent, receivedDt, infoOnlyFlag,
        filerIdent, filerTypeCd, filerName,
        finalUnexpendContribFlag, finalRetainedAssetsFlag, finalOfficeholderAckFlag
    """
    if session is None:
        return None

    report_ident_raw = raw.get("reportInfoIdent")
    if report_ident_raw is None:
        return None
    report_ident = str(report_ident_raw).strip()
    if not report_ident:
        return None

    stmt = select(UnifiedReport).where(UnifiedReport.report_ident == report_ident)
    report = session.exec(stmt).first()
    if report is None:
        _logger.debug(
            f"[build_final_report] no UnifiedReport found for ident={report_ident!r} — "
            "FINL record skipped (CVR1 may not have been loaded yet)"
        )
        return None

    report.is_final = True
    session.add(report)
    return report
