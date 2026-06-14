"""Stage 1 runner: standardize source records into resolution_input."""

from __future__ import annotations

from typing import Any

import polars as pl
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, delete, select

import app.resolve.models  # noqa: F401 — registers UnifiedReport before ORM use
from app.core.models import (
    State,
    UnifiedAddress,
    UnifiedCommittee,
    UnifiedEntity,
    UnifiedPerson,
)
from app.resolve.standardize.addresses import StandardizedAddress, standardize_address
from app.resolve.standardize.names import StandardizedName, standardize_name
from app.resolve.standardize.orgs import normalize_org_name
from app.resolve.standardize.phonetics import phonetic_code
from app.resolve.standardize.staging import ResolutionInput


def _compose_raw_address(
    street_1: str | None,
    street_2: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
) -> str:
    parts = [street_1, street_2, city, state, zip_code]
    return ", ".join(part.strip() for part in parts if part and part.strip())


def _entity_activity_dates(
    session: Session, state_id: int
) -> dict[int, list[Any]]:
    """Return ``{entity_id: [first_date, last_date]}`` from the transactions that
    reference each entity.

    Person/organization entities are dated via ``unified_transaction_persons``
    (contributor / payee / lender roles); committee entities via the
    ``committee_id`` they own.  These are the real filing dates the canonical
    name-history windows should use (not the ETL ``created_at``).
    """
    dates: dict[int, list[Any]] = {}

    def merge(eid: int | None, lo: Any, hi: Any) -> None:
        if eid is None:
            return
        cur = dates.get(eid)
        if cur is None:
            dates[eid] = [lo, hi]
            return
        if lo is not None and (cur[0] is None or lo < cur[0]):
            cur[0] = lo
        if hi is not None and (cur[1] is None or hi > cur[1]):
            cur[1] = hi

    person_sql = text(
        """
        SELECT tp.entity_id, MIN(t.transaction_date), MAX(t.transaction_date)
        FROM unified_transaction_persons tp
        JOIN unified_transactions t ON t.id = tp.transaction_id
        WHERE tp.entity_id IS NOT NULL
          AND t.state_id = :sid
          AND t.transaction_date IS NOT NULL
        GROUP BY tp.entity_id
        """
    )
    committee_sql = text(
        """
        SELECT e.id, MIN(t.transaction_date), MAX(t.transaction_date)
        FROM unified_entities e
        JOIN unified_transactions t ON t.committee_id = e.committee_id
        WHERE e.committee_id IS NOT NULL
          AND t.state_id = :sid
          AND t.transaction_date IS NOT NULL
        GROUP BY e.id
        """
    )
    # Degrade gracefully: the transaction tables may be absent (e.g. resolve-only
    # test fixtures).  Without activity dates, survivorship falls back to
    # ``created_at`` rather than failing the whole stage.
    for stmt in (person_sql, committee_sql):
        try:
            results = session.execute(stmt, {"sid": state_id}).fetchall()
        except SQLAlchemyError:
            session.rollback()
            continue
        for eid, lo, hi in results:
            merge(eid, lo, hi)
    return dates


def _activity_dates_by_key(session: Session, sql: Any, state_id: int) -> dict[Any, list[Any]]:
    """Run *sql* (returns ``key, min_date, max_date`` per row) → ``{key: [lo, hi]}``.

    Degrades to ``{}`` if the transaction tables are absent (resolve-only test
    fixtures) so a missing table never fails the stage.
    """
    out: dict[Any, list[Any]] = {}
    try:
        results = session.execute(sql, {"sid": state_id}).fetchall()
    except SQLAlchemyError:
        session.rollback()
        return out
    for key, lo, hi in results:
        if key is not None:
            out[key] = [lo, hi]
    return out


def _person_activity_dates(session: Session, state_id: int) -> dict[int, list[Any]]:
    """``{person_id: [first_date, last_date]}`` from the transactions each person
    participated in (so name variants sourced from a person row get real windows)."""
    return _activity_dates_by_key(
        session,
        text(
            """
            SELECT tp.person_id, MIN(t.transaction_date), MAX(t.transaction_date)
            FROM unified_transaction_persons tp
            JOIN unified_transactions t ON t.id = tp.transaction_id
            WHERE tp.person_id IS NOT NULL
              AND t.state_id = :sid
              AND t.transaction_date IS NOT NULL
            GROUP BY tp.person_id
            """
        ),
        state_id,
    )


def _committee_activity_dates(session: Session, state_id: int) -> dict[str, list[Any]]:
    """``{filer_id: [first_date, last_date]}`` from the transactions each committee
    filed (so committee-sourced name variants get real windows)."""
    return _activity_dates_by_key(
        session,
        text(
            """
            SELECT t.committee_id, MIN(t.transaction_date), MAX(t.transaction_date)
            FROM unified_transactions t
            WHERE t.committee_id IS NOT NULL
              AND t.state_id = :sid
              AND t.transaction_date IS NOT NULL
            GROUP BY t.committee_id
            """
        ),
        state_id,
    )


def _collect_source_rows(session: Session, state_id: int) -> list[dict[str, Any]]:
    """Stage person, committee, and entity source records.

    Cross-entity_type self-matches (the bug that stalled the scorer) are now
    prevented in blocking, which requires both sides of a pair to share an
    ``entity_type`` (see ``blocking_sql``).  Activity dates are attached to the
    ``unified_entity`` rows; because a person/committee and its entity cluster
    together, the cluster's date window is sourced from the entity row.
    """
    person_rows = session.exec(
        select(
            UnifiedPerson.id,
            UnifiedPerson.first_name,
            UnifiedPerson.middle_name,
            UnifiedPerson.last_name,
            UnifiedPerson.suffix,
            UnifiedPerson.organization,
            UnifiedPerson.employer,
            UnifiedAddress.street_1,
            UnifiedAddress.street_2,
            UnifiedAddress.city,
            UnifiedAddress.state,
            UnifiedAddress.zip_code,
        )
        .outerjoin(UnifiedAddress, UnifiedPerson.address_id == UnifiedAddress.id)
        .where(UnifiedPerson.state_id == state_id)
    ).all()

    committee_rows = session.exec(
        select(
            UnifiedCommittee.filer_id,
            UnifiedCommittee.name,
            UnifiedAddress.street_1,
            UnifiedAddress.street_2,
            UnifiedAddress.city,
            UnifiedAddress.state,
            UnifiedAddress.zip_code,
        )
        .outerjoin(UnifiedAddress, UnifiedCommittee.address_id == UnifiedAddress.id)
        .where(UnifiedCommittee.state_id == state_id)
    ).all()

    entity_rows = session.exec(
        select(
            UnifiedEntity.id,
            UnifiedEntity.name,
            UnifiedEntity.entity_type,
            UnifiedEntity.person_id,
            UnifiedEntity.committee_id,
            UnifiedAddress.street_1,
            UnifiedAddress.street_2,
            UnifiedAddress.city,
            UnifiedAddress.state,
            UnifiedAddress.zip_code,
        )
        .outerjoin(UnifiedAddress, UnifiedEntity.address_id == UnifiedAddress.id)
        .where(UnifiedEntity.state_id == state_id)
    ).all()

    activity = _entity_activity_dates(session, state_id)
    person_activity = _person_activity_dates(session, state_id)
    committee_activity = _committee_activity_dates(session, state_id)
    output: list[dict[str, Any]] = []
    for row in person_rows:
        raw_name = " ".join(
            part for part in [row.first_name, row.middle_name, row.last_name, row.suffix] if part
        ).strip()
        if not raw_name:
            raw_name = row.organization or ""
        p_window = person_activity.get(row.id)
        p_first, p_last = (p_window[0], p_window[1]) if p_window else (None, None)
        output.append(
            {
                "source_type": "unified_person",
                "source_id": str(row.id),
                "entity_type": "person",
                "raw_name": raw_name,
                "raw_address": _compose_raw_address(
                    row.street_1, row.street_2, row.city, row.state, row.zip_code
                ),
                "employer": row.employer,
                "first_activity_date": p_first,
                "last_activity_date": p_last,
            }
        )

    for row in committee_rows:
        c_window = committee_activity.get(row.filer_id)
        c_first, c_last = (c_window[0], c_window[1]) if c_window else (None, None)
        output.append(
            {
                "source_type": "unified_committee",
                "source_id": str(row.filer_id),
                "entity_type": "committee",
                "raw_name": row.name or "",
                "raw_address": _compose_raw_address(
                    row.street_1, row.street_2, row.city, row.state, row.zip_code
                ),
                "first_activity_date": c_first,
                "last_activity_date": c_last,
            }
        )

    for row in entity_rows:
        window = activity.get(row.id)
        first_date, last_date = (window[0], window[1]) if window else (None, None)
        output.append(
            {
                "source_type": "unified_entity",
                "source_id": str(row.id),
                "entity_type": row.entity_type.value,
                "linked_person_id": row.person_id,
                "linked_committee_id": row.committee_id,
                "raw_name": row.name or "",
                "raw_address": _compose_raw_address(
                    row.street_1, row.street_2, row.city, row.state, row.zip_code
                ),
                "first_activity_date": first_date,
                "last_activity_date": last_date,
            }
        )

    return output


def _name_fields(std_name: Any) -> dict[str, Any]:
    if not isinstance(std_name, StandardizedName):
        return {
            "first_name": None,
            "middle_name": None,
            "last_name": None,
            "suffix": None,
            "is_organization": False,
            "first_name_phonetic": phonetic_code(""),
            "last_name_phonetic": phonetic_code(""),
        }

    first = std_name.first
    last = std_name.last
    return {
        "first_name": first,
        "middle_name": std_name.middle,
        "last_name": last,
        "suffix": std_name.suffix,
        "is_organization": std_name.is_organization,
        "first_name_phonetic": phonetic_code(first or ""),
        "last_name_phonetic": phonetic_code(last or ""),
    }


def _address_fields(std_address: Any) -> dict[str, Any]:
    if not isinstance(std_address, StandardizedAddress):
        return {
            "line_1": None,
            "line_2": None,
            "city": None,
            "state": None,
            "zip5": None,
            "zip4": None,
            "parse_status": "unparsed",
        }

    return {
        "line_1": std_address.line_1,
        "line_2": std_address.line_2,
        "city": std_address.city,
        "state": std_address.state,
        "zip5": std_address.zip5,
        "zip4": std_address.zip4,
        "parse_status": std_address.parse_status,
    }


def _compute_features(rows: list[dict[str, Any]], run_id: int) -> list[ResolutionInput]:
    if not rows:
        return []

    # Keep the activity-date columns OUT of the Polars frame: they are a mix of
    # None (person/committee rows) and dates (entity rows), which breaks Polars
    # schema inference ("could not append value ... of type date").  Polars is
    # only used for name/address standardization; dates are reattached by row
    # index below (iter_rows preserves input order).
    activity_dates = [
        (r.get("first_activity_date"), r.get("last_activity_date")) for r in rows
    ]
    # Keep the deterministic link ids out of the Polars frame too (mixed None/int and
    # None/str across source types); reattach by row index like the activity dates.
    linked_ids = [
        (r.get("linked_person_id"), r.get("linked_committee_id")) for r in rows
    ]
    # Employer is only present on person rows; keep it out of the Polars frame so
    # that mixed-type schema inference (str | None vs absent key) does not break.
    # Normalize using the same org-name helper as normalized_org.
    raw_employers = [r.get("employer") for r in rows]
    _frame_skip = (
        "first_activity_date", "last_activity_date", "linked_person_id", "linked_committee_id",
        "employer",
    )
    frame_rows = [
        {k: v for k, v in r.items() if k not in _frame_skip}
        for r in rows
    ]
    frame = pl.DataFrame(frame_rows)
    # map_elements required here: standardize_name/standardize_address wrap
    # probablepeople, usaddress, and scourgify — no native Polars equivalent.
    enriched = frame.with_columns(
        pl.col("raw_name")
        .map_elements(
            standardize_name,
            return_dtype=pl.Object,
        )
        .alias("std_name"),
        pl.col("raw_address")
        .map_elements(
            standardize_address,
            return_dtype=pl.Object,
        )
        .alias("std_address"),
        pl.col("raw_name")
        .map_elements(
            normalize_org_name,
            return_dtype=pl.String,
        )
        .alias("normalized_org"),
    )

    staged: list[ResolutionInput] = []
    for idx, row in enumerate(enriched.iter_rows(named=True)):
        first_activity_date, last_activity_date = activity_dates[idx]
        linked_person_id, linked_committee_id = linked_ids[idx]
        name_values = _name_fields(row["std_name"])
        address_values = _address_fields(row["std_address"])
        normalized_org = row["normalized_org"] or ""
        raw_employer = raw_employers[idx]
        normalized_employer = normalize_org_name(raw_employer) if raw_employer else None
        staged.append(
            ResolutionInput(
                run_id=run_id,
                source_type=row["source_type"],
                source_id=row["source_id"],
                entity_type=row["entity_type"],
                normalized_org=normalized_org or None,
                org_name_phonetic=phonetic_code(normalized_org.split(" ")[0])
                if normalized_org
                else "",
                employer=normalized_employer or None,
                raw_name=row["raw_name"],
                raw_address=row["raw_address"],
                first_activity_date=first_activity_date,
                last_activity_date=last_activity_date,
                linked_person_id=linked_person_id,
                linked_committee_id=linked_committee_id,
                **name_values,
                **address_values,
            )
        )

    return staged


def build_resolution_input(session: Session, run_id: int, state_code: str) -> int:
    """Build stage-1 standardized records for one state and run id."""
    state = session.exec(select(State).where(State.code == state_code.upper())).one_or_none()
    if state is None or state.id is None:
        return 0

    source_rows = _collect_source_rows(session, state.id)
    staged_rows = _compute_features(source_rows, run_id=run_id)
    if not staged_rows:
        return 0

    with session.begin_nested():
        session.exec(delete(ResolutionInput).where(ResolutionInput.run_id == run_id))
        session.add_all(staged_rows)
    session.commit()
    return len(staged_rows)
