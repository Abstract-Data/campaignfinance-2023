"""Deterministic canonical-address builder.

An address's identity is its standardized parts — already produced by
``standardize_address`` — so it needs no probabilistic resolution.  This dedups
every ``unified_address`` into a ``canonical_address`` row (with an occurrence
``frequency``) and crosswalks each source row, mirroring the canonical-campaign
builder.  Run as the ``--pass-type address`` pass.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session, select

from app.resolve.models.canonical import CanonicalAddress, ParseStatus
from app.resolve.models.resolution import (
    AddressCrosswalk,
    ConfidenceBand,
    MatchMethod,
    SourceType,
)
from app.resolve.standardize.addresses import standardize_address

_AddrKey = tuple[str | None, str | None, str | None, str | None, str | None]


def _compose(
    street_1: str | None,
    street_2: str | None,
    city: str | None,
    state: str | None,
    zip_code: str | None,
) -> str:
    parts = [street_1, street_2, city, state, zip_code]
    return ", ".join(p.strip() for p in parts if p and p.strip())


def _parse_status(value: str | None) -> ParseStatus:
    try:
        return ParseStatus(value)
    except ValueError:
        return ParseStatus.unparsed


def _clip(value: str | None, max_len: int) -> str | None:
    """Clamp to the canonical_address column width; drop empties."""
    if value is None:
        return None
    v = value.strip()
    if not v:
        return None
    return v[:max_len]


def _state2(value: str | None) -> str | None:
    """Keep a 2-letter state code; drop anything else (malformed parses)."""
    v = (value or "").strip().upper()
    return v if len(v) == 2 and v.isalpha() else None


def build_canonical_addresses(session: Session, run_id: int | None = None) -> int:
    """Populate ``canonical_address`` (+ ``address_crosswalk``) from
    ``unified_addresses``.

    Returns the number of canonical address rows written.  Idempotent: clears the
    canonical layer first.  When *run_id* is given, also writes one
    ``address_crosswalk`` row per source address (cleared for that run first).
    """
    rows = session.execute(
        text(
            "SELECT id, street_1, street_2, city, state, zip_code FROM unified_addresses"
        )
    ).fetchall()

    by_key: dict[_AddrKey, CanonicalAddress] = {}
    freq: dict[_AddrKey, int] = {}
    source_keys: list[tuple[int, _AddrKey]] = []  # (unified_address.id, identity key)
    for aid, street_1, street_2, city, state, zip_code in rows:
        std = standardize_address(_compose(street_1, street_2, city, state, zip_code))
        # Clamp to the canonical_address column widths (malformed parses can
        # overflow, e.g. a non-2-char "state").
        line_1 = _clip(std.line_1, 500)
        line_2 = _clip(std.line_2, 500)
        city_v = _clip(std.city, 200)
        state_v = _state2(std.state)
        zip5_v = _clip(std.zip5, 5)
        zip4_v = _clip(std.zip4, 4)
        key: _AddrKey = (line_1, line_2, city_v, state_v, zip5_v)
        if not any(key):  # nothing parseable — skip (don't make an empty canonical row)
            continue
        source_keys.append((aid, key))
        freq[key] = freq.get(key, 0) + 1
        if key in by_key:
            continue
        by_key[key] = CanonicalAddress(
            standardized_line_1=line_1,
            standardized_line_2=line_2,
            city=city_v,
            state=state_v,
            zip5=zip5_v,
            zip4=zip4_v,
            parse_status=_parse_status(std.parse_status),
            last_run_id=run_id,
        )

    for key, ca in by_key.items():
        ca.frequency = freq.get(key, 0)

    # Idempotent rebuild of the canonical layer.
    if run_id is not None:
        session.execute(
            text("DELETE FROM address_crosswalk WHERE run_id = :rid"), {"rid": run_id}
        )
    # canonical_address is a single global table (no state_code column) and this
    # builder reads *all* unified_addresses above, so the unscoped DELETE is a
    # full rebuild — every prior canonical row is re-derived from source, not lost.
    session.execute(text("DELETE FROM canonical_address"))
    session.add_all(list(by_key.values()))
    session.flush()  # populate canonical_address.id for crosswalk rows

    if run_id is not None:
        key_to_id = {key: ca.id for key, ca in by_key.items()}
        for aid, key in source_keys:
            canonical_id = key_to_id.get(key)
            if canonical_id is None:
                continue
            session.add(
                AddressCrosswalk(
                    source_type=SourceType.unified_address,
                    source_id=str(aid),
                    canonical_address_id=canonical_id,
                    match_method=MatchMethod.exact,
                    confidence_band=ConfidenceBand.auto,
                    run_id=run_id,
                )
            )

    session.commit()
    return len(by_key)


def canonical_address_count(session: Session) -> int:
    return len(session.exec(select(CanonicalAddress)).all())
