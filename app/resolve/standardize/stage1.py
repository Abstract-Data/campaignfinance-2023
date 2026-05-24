"""Stage 1 runner: standardize source records into resolution_input."""

from __future__ import annotations

from typing import Any

import polars as pl
from sqlmodel import Session, delete, select

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
            part
            for part in [row.first_name, row.middle_name, row.last_name, row.suffix]
            if part
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


def _compute_features(rows: list[dict[str, Any]], run_id: int) -> list[ResolutionInput]:
    if not rows:
        return []

    frame = pl.DataFrame(rows)
    features = (
        frame.with_columns(
            pl.col("raw_name").map_elements(
                standardize_name,
                return_dtype=pl.Object,
            ).alias("std_name"),
            pl.col("raw_address").map_elements(
                standardize_address,
                return_dtype=pl.Object,
            ).alias("std_address"),
        )
        .with_columns(
            pl.col("std_name").map_elements(
                lambda value: value.first if isinstance(value, StandardizedName) else None,
                return_dtype=pl.String,
            ).alias("first_name"),
            pl.col("std_name").map_elements(
                lambda value: value.middle if isinstance(value, StandardizedName) else None,
                return_dtype=pl.String,
            ).alias("middle_name"),
            pl.col("std_name").map_elements(
                lambda value: value.last if isinstance(value, StandardizedName) else None,
                return_dtype=pl.String,
            ).alias("last_name"),
            pl.col("std_name").map_elements(
                lambda value: value.suffix if isinstance(value, StandardizedName) else None,
                return_dtype=pl.String,
            ).alias("suffix"),
            pl.col("std_name").map_elements(
                lambda value: value.is_organization if isinstance(value, StandardizedName) else False,
                return_dtype=pl.Boolean,
            ).alias("is_organization"),
            pl.col("std_address").map_elements(
                lambda value: value.line_1 if isinstance(value, StandardizedAddress) else None,
                return_dtype=pl.String,
            ).alias("line_1"),
            pl.col("std_address").map_elements(
                lambda value: value.line_2 if isinstance(value, StandardizedAddress) else None,
                return_dtype=pl.String,
            ).alias("line_2"),
            pl.col("std_address").map_elements(
                lambda value: value.city if isinstance(value, StandardizedAddress) else None,
                return_dtype=pl.String,
            ).alias("city"),
            pl.col("std_address").map_elements(
                lambda value: value.state if isinstance(value, StandardizedAddress) else None,
                return_dtype=pl.String,
            ).alias("state"),
            pl.col("std_address").map_elements(
                lambda value: value.zip5 if isinstance(value, StandardizedAddress) else None,
                return_dtype=pl.String,
            ).alias("zip5"),
            pl.col("std_address").map_elements(
                lambda value: value.zip4 if isinstance(value, StandardizedAddress) else None,
                return_dtype=pl.String,
            ).alias("zip4"),
            pl.col("std_address").map_elements(
                lambda value: value.parse_status if isinstance(value, StandardizedAddress) else "unparsed",
                return_dtype=pl.String,
            ).alias("parse_status"),
            pl.col("raw_name").map_elements(
                normalize_org_name,
                return_dtype=pl.String,
            ).alias("normalized_org"),
        )
        .with_columns(
            pl.col("first_name").map_elements(
                lambda value: phonetic_code(value or ""),
                return_dtype=pl.String,
            ).alias("first_name_phonetic"),
            pl.col("last_name").map_elements(
                lambda value: phonetic_code(value or ""),
                return_dtype=pl.String,
            ).alias("last_name_phonetic"),
            pl.col("normalized_org").map_elements(
                lambda value: phonetic_code((value or "").split(" ")[0]) if value else "",
                return_dtype=pl.String,
            ).alias("org_name_phonetic"),
        )
    )

    rows_dict = features.select(
        "source_type",
        "source_id",
        "entity_type",
        "first_name",
        "middle_name",
        "last_name",
        "suffix",
        "is_organization",
        "line_1",
        "line_2",
        "city",
        "state",
        "zip5",
        "zip4",
        "parse_status",
        "normalized_org",
        "first_name_phonetic",
        "last_name_phonetic",
        "org_name_phonetic",
        "raw_name",
        "raw_address",
    ).to_dicts()

    return [ResolutionInput(run_id=run_id, **row) for row in rows_dict]


def build_resolution_input(session: Session, run_id: int, state_code: str) -> int:
    """Build stage-1 standardized records for one state and run id."""
    state = session.exec(
        select(State).where(State.code == state_code.upper())
    ).one_or_none()
    if state is None or state.id is None:
        return 0

    source_rows = _collect_source_rows(session, state.id)
    staged_rows = _compute_features(source_rows, run_id=run_id)
    if not staged_rows:
        return 0

    session.exec(delete(ResolutionInput).where(ResolutionInput.run_id == run_id))
    session.add_all(staged_rows)
    session.commit()
    return len(staged_rows)
