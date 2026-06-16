"""Deterministic canonical-campaign builder.

A campaign's identity is structural — ``(committee_entity, office, election
cycle)`` — not something that needs probabilistic resolution.  This maps every
``unified_campaign`` onto a ``canonical_campaign`` row using the entity crosswalk
(committee → canonical committee entity; candidate person → canonical entity),
deduping by the identity tuple.  Run after the entity pass so the crosswalk is
populated.

**Sentinel value for missing election year:** When ``election_year`` is NULL
(officeholder committees, multi-cycle PACs, and other filers with no election
year), ``election_cycle`` is stored as ``0``.  The identity tuple is therefore
``(committee_entity, office, 0)`` for such rows.  ``election_cycle == 0`` means
"no election cycle / officeholder" — it is *not* a real cycle.  The builder is
delete-and-rebuild each run, so the sentinel cannot drift into an invalid state.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlmodel import Session, select

from app.resolve.models.canonical import CanonicalCampaign
from app.resolve.models.resolution import (
    CampaignCrosswalk,
    ConfidenceBand,
    MatchMethod,
    SourceType,
)


def _office_normalized(office_sought: str | None) -> str | None:
    if not office_sought:
        return None
    norm = office_sought.strip().lower()
    return norm or None


def build_canonical_campaigns(
    session: Session, state_code: str = "TX", run_id: int | None = None
) -> int:
    """Populate ``canonical_campaign`` (+ ``campaign_crosswalk``) from
    ``unified_campaigns``.

    A campaign's identity is ``(committee_entity, office, cycle)``.  Returns the
    number of canonical campaign rows written.  Idempotent: clears the state's
    existing rows first so re-runs don't duplicate.  When *run_id* is given, also
    writes one ``campaign_crosswalk`` row per source campaign that maps to a
    canonical campaign (cleared for that run first).
    """
    # committee filer_id / candidate person_id -> canonical_entity_id
    # entity_crosswalk is append-only across runs, so the same source_id can
    # carry rows from several runs with different canonical_entity_id values.
    # Scope to the latest run (mirroring publish/views.py) — otherwise the
    # last-wins dict comprehension picks a nondeterministic canonical entity.
    committee_xw: dict[str, int] = {
        sid: cid
        for sid, cid in session.execute(
            text(
                "SELECT source_id, canonical_entity_id FROM entity_crosswalk "
                "WHERE source_type = 'unified_committee' "
                "AND run_id = (SELECT MAX(run_id) FROM entity_crosswalk)"
            )
        )
    }
    person_xw: dict[str, int] = {
        sid: cid
        for sid, cid in session.execute(
            text(
                "SELECT source_id, canonical_entity_id FROM entity_crosswalk "
                "WHERE source_type = 'unified_person' "
                "AND run_id = (SELECT MAX(run_id) FROM entity_crosswalk)"
            )
        )
    }

    rows = session.execute(
        text(
            """
            SELECT id, primary_committee_id, candidate_person_id, election_year,
                   office_sought, name
            FROM unified_campaigns
            WHERE primary_committee_id IS NOT NULL
            """
        )
    ).fetchall()

    # Dedup by (committee_entity, office_normalized, cycle).  Track which source
    # campaign ids map to each identity so we can crosswalk every source row to
    # the surviving canonical campaign.
    Key = tuple[int, str | None, int]
    by_identity: dict[Key, CanonicalCampaign] = {}
    source_keys: list[tuple[int, Key]] = []  # (unified_campaign.id, identity key)
    for (
        uc_id,
        primary_committee_id,
        candidate_person_id,
        election_year,
        office_sought,
        name,
    ) in rows:
        committee_entity_id = committee_xw.get(str(primary_committee_id))
        if committee_entity_id is None:
            continue
        office = _office_normalized(office_sought)
        # election_cycle == 0 means "no election cycle / officeholder"; NULL
        # election_year maps to sentinel 0 so the non-null column stays valid.
        cycle = int(election_year) if election_year is not None else 0
        key = (committee_entity_id, office, cycle)
        candidate_entity_id = (
            person_xw.get(str(candidate_person_id)) if candidate_person_id is not None else None
        )
        source_keys.append((uc_id, key))
        existing = by_identity.get(key)
        if existing is not None:
            # Prefer a candidate-bearing row: backfill the candidate the first
            # row lacked, so the kept campaign isn't left candidate-less just
            # because a non-CAND duplicate was seen first.
            if existing.candidate_entity_id is None and candidate_entity_id is not None:
                existing.candidate_entity_id = candidate_entity_id
            continue
        by_identity[key] = CanonicalCampaign(
            committee_entity_id=committee_entity_id,
            office_normalized=office,
            election_cycle=cycle,  # 0 when election_year is NULL (officeholder sentinel)
            candidate_entity_id=candidate_entity_id,
            canonical_name=name,
            state_code=state_code,
        )

    session.execute(
        text("DELETE FROM canonical_campaign WHERE state_code = :sc"), {"sc": state_code}
    )
    session.add_all(list(by_identity.values()))
    session.flush()  # populate canonical_campaign.id for crosswalk rows

    if run_id is not None:
        session.execute(text("DELETE FROM campaign_crosswalk WHERE run_id = :rid"), {"rid": run_id})
        key_to_canonical_id = {key: cc.id for key, cc in by_identity.items()}
        for uc_id, key in source_keys:
            canonical_id = key_to_canonical_id.get(key)
            if canonical_id is None:
                continue
            session.add(
                CampaignCrosswalk(
                    source_type=SourceType.unified_campaign,
                    source_id=str(uc_id),
                    canonical_campaign_id=canonical_id,
                    match_method=MatchMethod.exact,
                    confidence_band=ConfidenceBand.auto,
                    run_id=run_id,
                )
            )

    session.commit()
    return len(by_identity)


def canonical_campaign_count(session: Session) -> int:
    return len(session.exec(select(CanonicalCampaign)).all())
