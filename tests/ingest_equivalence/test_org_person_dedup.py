"""Regression for blocker #1: org-person dedup must key on ``lower(organization)`` ALONE.

The ORM ``BuilderCache.person_key`` keys an org-person on ``("org", lower(org), state)`` —
ignoring any incidental first/last — and the partial index ``uix_persons_org_state`` is on
``(lower(organization), state_id) WHERE organization IS NOT NULL``. The vectorized families
previously keyed persons on the 3-tuple ``(lower(org), lower(fn), lower(ln))``, so two org
rows with the same org but different incidental contact names survived the engine's dedup
yet collided on the org-only unique index on Postgres. ``common.collapse_org_person_key``
nulls fn/ln when org is set so the dedup/lookup key matches the ORM + the index.
"""

from __future__ import annotations

import polars as pl

from app.core.ingest_vectorized import common


def test_collapse_nulls_names_for_org_rows_only():
    frame = pl.DataFrame(
        {
            "_pk_org": ["cooper", "cooper", None, None],
            "_pk_fn": ["jane", "bob", "ann", "ann"],
            "_pk_ln": ["doe", "smith", "lee", "lee"],
        }
    )
    out = common.collapse_org_person_key(frame)
    # org rows: fn/ln nulled (keyed on org alone)
    org_rows = out.filter(pl.col("_pk_org").is_not_null())
    assert org_rows["_pk_fn"].null_count() == org_rows.height
    assert org_rows["_pk_ln"].null_count() == org_rows.height
    # individual rows: names preserved
    ind_rows = out.filter(pl.col("_pk_org").is_null())
    assert ind_rows["_pk_fn"].to_list() == ["ann", "ann"]


def test_org_case_variants_with_differing_names_dedup_to_one():
    """The real collision: "Cooper" and "COOPER" (same org, different incidental contact
    names) must collapse to ONE person key — matching uix_persons_org_state."""
    frame = pl.DataFrame(
        {
            "_pk_org": ["cooper", "cooper", "cooper"],  # already lower-cased by the builders
            "_pk_fn": ["jane", "bob", None],
            "_pk_ln": ["doe", "smith", None],
        }
    )
    deduped = common.collapse_org_person_key(frame).unique(
        subset=["_pk_org", "_pk_fn", "_pk_ln"], maintain_order=True
    )
    assert deduped.height == 1, "org rows sharing lower(org) must dedup to a single person"


def test_individuals_dedup_independently_of_org_rows():
    """Individuals (org NULL) still key on (lower(first), lower(last)); distinct names stay
    distinct, and they don't merge with org rows."""
    frame = pl.DataFrame(
        {
            "_pk_org": [None, None, "acme"],
            "_pk_fn": ["ann", "ben", "ann"],
            "_pk_ln": ["lee", "ng", "lee"],
        }
    )
    deduped = common.collapse_org_person_key(frame).unique(
        subset=["_pk_org", "_pk_fn", "_pk_ln"], maintain_order=True
    )
    # two distinct individuals + one org = 3 keys (the org row's ann/lee is nulled, so it
    # does NOT collide with the individual ann/lee).
    assert deduped.height == 3
