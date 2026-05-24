"""Stage 1 runner: standardize source records into resolution_input."""

from __future__ import annotations

from typing import Any

import polars as pl
from sqlmodel import Session, delete, select

import app.resolve.models  # noqa: F401 — registers UnifiedReport before ORM use
from app.core.unified_sqlmodels import (
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


def _collect_source_rows(session: Session, state_id: int) -> list[dict[str, Any]]:
    person_rows = session.exec(
        select(
            UnifiedPerson.id,
            UnifiedPerson.first_name,
            UnifiedPerson.middle_name,
            UnifiedPerson.last_name,
            UnifiedPerson.suffix,
            UnifiedPerson.organization,
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
            UnifiedAddress.street_1,
            UnifiedAddress.street_2,
            UnifiedAddress.city,
            UnifiedAddress.state,
            UnifiedAddress.zip_code,
        )
        .outerjoin(UnifiedAddress, UnifiedEntity.address_id == UnifiedAddress.id)
        .where(UnifiedEntity.state_id == state_id)
    ).all()

    output: list[dict[str, Any]] = []
    for row in person_rows:
        raw_name = " ".join(
            part for part in [row.first_name, row.middle_name, row.last_name, row.suffix] if part
        ).strip()
        if not raw_name:
            raw_name = row.organization or ""
        output.append(
            {
                "source_type": "unified_person",
                "source_id": str(row.id),
                "entity_type": "person",
                "raw_name": raw_name,
                "raw_address": _compose_raw_address(
                    row.street_1,
                    row.street_2,
                    row.city,
                    row.state,
                    row.zip_code,
                ),
            }
        )

    for row in committee_rows:
        output.append(
            {
                "source_type": "unified_committee",
                "source_id": str(row.filer_id),
                "entity_type": "committee",
                "raw_name": row.name or "",
                "raw_address": _compose_raw_address(
                    row.street_1,
                    row.street_2,
                    row.city,
                    row.state,
                    row.zip_code,
                ),
            }
        )

    for row in entity_rows:
        output.append(
            {
                "source_type": "unified_entity",
                "source_id": str(row.id),
                "entity_type": row.entity_type.value,
                "raw_name": row.name or "",
                "raw_address": _compose_raw_address(
                    row.street_1,
                    row.street_2,
                    row.city,
                    row.state,
                    row.zip_code,
                ),
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

    frame = pl.DataFrame(rows)
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
    for row in enriched.iter_rows(named=True):
        name_values = _name_fields(row["std_name"])
        address_values = _address_fields(row["std_address"])
        normalized_org = row["normalized_org"] or ""
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
                raw_name=row["raw_name"],
                raw_address=row["raw_address"],
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
