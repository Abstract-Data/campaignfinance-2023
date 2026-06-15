"""Gate: the vectorized campaign build vs the ORM loader.

The ORM builds ``unified_campaigns`` / ``unified_campaign_entities`` during
``process_record`` (via ``builders.build_campaign``); the vectorized engine builds
them in ``ingest_vectorized.campaigns.finalize_campaigns``. This test loads the FULL
golden fixture through both (the ORM via the file-by-file loader + post-load linking,
mirroring ``production_loader.discover_and_load``; the vectorized via
``run_vectorized``) and compares the campaign output.

ACCEPTED CONTRACT — ``ORM ⊆ vec`` (not ``diff_snapshots == []``):
The ORM severs ``transaction.campaign`` before ``session.add``
(``production_loader._finalize_transaction_for_persist``), so a built campaign is
persisted only opportunistically, via its persistent committee's ``.campaigns``
cascade across the loader's per-file 1000-row commit batches. The persisted set is an
emergent SQLAlchemy unit-of-work artifact (NOT derivable from the source data —
verified empirically), so the ORM persists only a fraction of the principled distinct
(committee, year) keys. The vectorized engine emits the principled DETERMINISTIC
superset. => every ORM campaign + campaign_entity must be PRESENT in the vec output
(0 ORM-only rows); both sides non-empty. The vec is the more-correct answer; exact
``==`` is impossible and not chased.

NAME MATCH (the re-land's point): with the FILER family (#50) on current main the
committee dim stores the AUTHORITATIVE committee name, and ``unified_campaigns.name``
derives from ``committee.name`` on BOTH sides. The structural campaign key therefore
now INCLUDES the display ``name`` (it no longer has to be excluded as a cross-family
gap). ``test_campaign_names_match_orm`` asserts this directly: every ORM campaign's
(committee, year, name) tuple is present in the vec output, i.e. names match and the
residual is purely the flush-order subset relationship.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _load_golden, _make_engine


def _campaign_keys(engine) -> set[tuple]:
    """Structural campaign identity owned by the campaign build:
    (primary_committee_id, election_year, office_sought, district, has_candidate, name).

    Includes the committee-derived display ``name`` (authoritative on both sides now
    that FILER is on main) and excludes surrogate ids. ``has_candidate`` is read as a
    presence flag (always False on the golden fixture — CAND rows are routed to the ORM
    enrichment path and never build a campaign — kept in the key so a future
    candidate-bearing record is verified)."""
    from app.core.models import UnifiedCampaign

    with engine.connect() as conn:
        rows = conn.execute(
            select(
                UnifiedCampaign.primary_committee_id,
                UnifiedCampaign.election_year,
                UnifiedCampaign.office_sought,
                UnifiedCampaign.district,
                UnifiedCampaign.candidate_person_id,
                UnifiedCampaign.name,
            )
        ).all()
    return {(r[0], r[1], r[2], r[3], r[4] is not None, r[5]) for r in rows}


def _campaign_name_keys(engine) -> set[tuple]:
    """(primary_committee_id, election_year, name) — for the name-match assertion."""
    from app.core.models import UnifiedCampaign

    with engine.connect() as conn:
        rows = conn.execute(
            select(
                UnifiedCampaign.primary_committee_id,
                UnifiedCampaign.election_year,
                UnifiedCampaign.name,
            )
        ).all()
    return {(r[0], r[1], r[2]) for r in rows}


def _campaign_entity_keys(engine) -> set[tuple]:
    """Campaign-entity linkage owned by the campaign build, FK-resolved to the
    committee's natural id: (committee_id, role, is_primary). The campaign side is keyed
    by the campaign's (committee, year) so a link is tied to the right campaign."""
    from app.core.models import (
        UnifiedCampaign,
        UnifiedCampaignEntity,
        UnifiedEntity,
    )

    with engine.connect() as conn:
        rows = conn.execute(
            select(
                UnifiedCampaign.primary_committee_id,
                UnifiedCampaign.election_year,
                UnifiedEntity.committee_id,
                UnifiedCampaignEntity.role,
                UnifiedCampaignEntity.is_primary,
            )
            .join(
                UnifiedCampaign,
                UnifiedCampaign.id == UnifiedCampaignEntity.campaign_id,
            )
            .join(UnifiedEntity, UnifiedEntity.id == UnifiedCampaignEntity.entity_id)
        ).all()
    return {(r[0], r[1], r[2], getattr(r[3], "name", r[3]), bool(r[4])) for r in rows}


def _make_engines(tmp_path: Path):
    """Load the FULL golden fixture via the ORM loader and via run_vectorized."""
    orm_engine = _make_engine(tmp_path / "orm.db")
    _load_golden(orm_engine)

    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, FIXTURES)
    return orm_engine, vec_engine


def test_campaigns_family_orm_subset_of_vec(tmp_path: Path):
    """ORM campaigns + campaign_entities are a SUBSET of the vectorized output
    (structural identity incl. authoritative name, FK-resolved to committee id), both
    sides non-empty, 0 ORM-only rows."""
    orm_engine, vec_engine = _make_engines(tmp_path)

    orm_campaigns = _campaign_keys(orm_engine)
    vec_campaigns = _campaign_keys(vec_engine)
    assert orm_campaigns, "ORM produced no campaigns — fixture/loader problem"
    assert vec_campaigns, "vectorized produced no campaigns — campaign build not writing"

    # candidate_person_id is None for every campaign on this fixture (no record type
    # that builds a campaign populates the candidate role); both engines must agree.
    assert all(not k[4] for k in orm_campaigns), "ORM campaign carried a candidate?"
    assert all(not k[4] for k in vec_campaigns), "vectorized campaign carried a candidate?"

    missing = orm_campaigns - vec_campaigns
    assert not missing, (
        "ORM campaigns missing from vectorized output (committee,year,office,district,"
        "has_candidate,name):\n  " + "\n  ".join(sorted(str(m) for m in missing))
    )

    orm_links = _campaign_entity_keys(orm_engine)
    vec_links = _campaign_entity_keys(vec_engine)
    assert orm_links, "ORM produced no campaign_entities"
    assert vec_links, "vectorized produced no campaign_entities"
    # Every ORM campaign_entity is a COMMITTEE, is_primary link.
    assert all(k[3] == "COMMITTEE" and k[4] for k in orm_links), (
        "unexpected non-COMMITTEE / non-primary campaign_entity on the golden fixture"
    )
    missing_links = orm_links - vec_links
    assert not missing_links, (
        "ORM campaign_entities missing from vectorized output "
        "(committee,year,committee_id,role,is_primary):\n  "
        + "\n  ".join(sorted(str(m) for m in missing_links))
    )


def test_campaign_names_match_orm(tmp_path: Path):
    """Re-land verification: with authoritative committee names on main, every ORM
    campaign's (committee, year, NAME) tuple is present in the vectorized output — i.e.
    the display names MATCH and the only residual is the ORM's flush-order subset."""
    orm_engine, vec_engine = _make_engines(tmp_path)

    orm_names = _campaign_name_keys(orm_engine)
    vec_names = _campaign_name_keys(vec_engine)
    assert orm_names and vec_names, "one side produced no campaigns"

    name_mismatch = orm_names - vec_names
    assert not name_mismatch, (
        "ORM campaign (committee,year,name) tuples missing from vectorized output — "
        "names diverge (not a pure flush-order subset):\n  "
        + "\n  ".join(sorted(str(m) for m in name_mismatch))
    )
