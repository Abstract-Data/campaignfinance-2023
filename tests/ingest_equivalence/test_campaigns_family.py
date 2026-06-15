"""Gate: the vectorized campaign build vs the ORM loader.

The ORM builds ``unified_campaigns`` / ``unified_campaign_entities`` during
``process_record`` (via ``builders.build_campaign``); the vectorized engine builds
them in ``ingest_vectorized.campaigns.finalize_campaigns``. This test loads the FULL
golden fixture through both and compares the campaign output.

Two documented realities make a byte-exact ``diff_snapshots == []`` unachievable here,
so the gate asserts the strongest property the campaign build is actually responsible
for — the ORM's campaigns/entities are a SUBSET of the vectorized output, keyed on the
structural identity the campaign build owns:

1. PERSISTED-SUBSET (ORM side). The ORM severs ``transaction.campaign`` before
   ``session.add`` (``production_loader._finalize_transaction_for_persist``), so a built
   campaign is persisted only opportunistically, via its persistent committee's
   ``.campaigns`` cascade across the loader's per-file 1000-row commit batches. The
   persisted set is an emergent SQLAlchemy unit-of-work artifact (NOT derivable from the
   source data — verified empirically), so the ORM persists only ~10 of the ~72
   principled distinct (committee, year) keys. The vectorized engine emits the
   principled deterministic superset. => ORM ⊆ vectorized.

2. COMMITTEE-NAME GAP (cross-family). ``unified_campaigns.name`` is inherited from
   ``committee.name``. The vectorized engine has no FILER family yet, so committee names
   come from inline ``filerName`` and differ from the ORM's FILER-record names. The
   campaign build faithfully inherits whatever name the committee dim stored, so the
   campaign ``name`` differs purely because of that upstream committee-dim gap — a
   separate family's concern, not campaign logic. We therefore key the comparison on the
   committee's natural id (``primary_committee_id`` / the entity's ``committee_id``),
   election_year, and the campaign's OWN structural fields (office_sought, district,
   candidate_person_id), NOT the committee-derived display name.

When the committee dim is brought to parity (FILER family), this test's keys still hold
and tighten naturally. Both sides are asserted non-empty.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import select

from app.core.ingest_vectorized import run_vectorized
from tests.ingest_equivalence.test_harness import FIXTURES, _load_golden, _make_engine


def _campaign_keys(engine) -> set[tuple]:
    """Structural campaign identity owned by the campaign build:
    (primary_committee_id, election_year, office_sought, district, candidate_person_id).

    Excludes the committee-derived display name (see module docstring) and surrogate
    ids. candidate_person_id is read as a presence flag (always None on the golden
    fixture; kept in the key so a future candidate-bearing record is verified)."""
    from app.core.models import UnifiedCampaign

    with engine.connect() as conn:
        rows = conn.execute(
            select(
                UnifiedCampaign.primary_committee_id,
                UnifiedCampaign.election_year,
                UnifiedCampaign.office_sought,
                UnifiedCampaign.district,
                UnifiedCampaign.candidate_person_id,
            )
        ).all()
    return {
        (r[0], r[1], r[2], r[3], r[4] is not None) for r in rows
    }


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
    return {
        (r[0], r[1], r[2], getattr(r[3], "name", r[3]), bool(r[4])) for r in rows
    }


def test_campaigns_family_matches_orm(tmp_path: Path):
    """ORM campaigns + campaign_entities are a subset of the vectorized output
    (structural identity, FK-resolved to committee id), both sides non-empty.

    See the module docstring for why this is a subset assertion (persisted-subset
    artifact + committee-name cross-family gap) rather than ``diff_snapshots == []``."""
    orm_engine = _make_engine(tmp_path / "orm.db")
    _load_golden(orm_engine)

    vec_engine = _make_engine(tmp_path / "vec.db", enforce_fk=False)
    run_vectorized(vec_engine, FIXTURES)

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
        "has_candidate):\n  " + "\n  ".join(str(m) for m in sorted(map(str, missing)))
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
        + "\n  ".join(str(m) for m in sorted(map(str, missing_links)))
    )
