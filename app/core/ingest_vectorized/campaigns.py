"""Vectorized campaign build — ``unified_campaigns`` + ``unified_campaign_entities``.

Reproduces the DATA SEMANTICS of ``builders.UnifiedSQLModelBuilder.build_campaign``
(called per transaction by ``processor.process_record``) columnar, as a single
post-load finalization step. Runs AFTER every family AND after
``finalize_entity_representatives`` so committees, the COMMITTEE entities, and the
transactions are all in place.

What the ORM build_campaign does, per transaction (record types RCPT / EXPN / LOAN /
DEBT / CRED / TRVL / ASSET / PLDG — the ones that produce a UnifiedTransaction):

  * campaign_name = ``_get_field_value(raw_data, "campaign_name")`` else
    candidate.full_name (the CANDIDATE-role participant) else committee.name. For Texas
    there is NO ``campaign_name`` source field, and NO record type that builds a
    transaction populates the CANDIDATE role: CAND rows are routed to the ORM loader's
    ENRICHMENT path (``production_loader._persist_cand_link``, NOT ``process_record``),
    so they never call ``build_campaign`` and never carry a candidate. The candidate is
    therefore ALWAYS None here -> campaign_name = committee.name. With the FILER family
    (#50) on main the committee dim now stores the AUTHORITATIVE committee name, so the
    campaign display name matches the ORM's (both derive from the same committee.name).
  * normalized_name = normalize_entity_name(campaign_name); the row is skipped when it
    is empty (no committee / blank committee name).
  * election_year = transaction.transaction_date.year (else None).
  * office_sought = _get_field_value(raw_data, "office_sought") and district =
    _get_field_value(raw_data, "district_info"). The Texas field library maps these
    from candidateHoldOffice*/candidateSeekOffice* (Hold before Seek, by definition
    order). Those columns are absent from EVERY transaction record type that builds a
    campaign (RCPT/EXPN/LOAN/DEBT/CRED/TRVL/ASSET/PLDG), so both are always None here.
    We still resolve them (Hold-before-Seek) so a future record type carrying them
    gets the ORM's value. BOUNDED DIVERGENCE (unreachable today, since the columns are
    absent): the ORM's _get_field_value returns the first source column that is a KEY
    in raw_data even when its value is "", whereas this treats a blank value as absent
    and falls through to the next source — so a present-but-empty candidateHoldOfficeCd
    would store "" (ORM) vs Seek's value/None (here). Likewise, for a key whose
    transactions disagree on office/district, the kept value is the dedup-sort-first
    one, not the ORM's first-BUILT one. Both are dormant given the absent columns.
  * dedup key (BuilderCache.campaign_key) = (normalized_name, committee_filer_id,
    candidate_id=None, election_year). One campaign per distinct key.
  * candidate_person_id = None (candidate is always None here).
  * primary_committee_id = committee.filer_id; state_id = state.
  * campaign_entities: the committee's COMMITTEE entity (role=COMMITTEE,
    is_primary=True) — only when the committee has a COMMITTEE entity. The CANDIDATE
    entity row never appears (candidate is None).

DOCUMENTED RESIDUAL vs the ORM's PERSISTED rows: the ORM severs
``transaction.campaign`` before ``session.add`` (production_loader
``_finalize_transaction_for_persist``), so a built campaign is only persisted
opportunistically, via its persistent committee's ``.campaigns`` cascade, across the
loader's per-file 1000-row commit batches. The persisted subset is therefore an
emergent SQLAlchemy unit-of-work artifact (NOT derivable from the source data or the
batch boundaries — verified empirically), and on the golden fixture the ORM persists
only a fraction of the principled distinct keys. This module emits the principled,
DETERMINISTIC superset: every distinct (normalized_name, committee, election_year) for
a transaction whose committee has a non-empty name. The equivalence gate
(``tests/ingest_equivalence/test_campaigns_family.py``) asserts the ORM's rows are a
SUBSET of this output (FK-resolved linkage), not byte equality, for exactly this
reason.

Pure Polars expressions (no map_elements/.apply); all reads are parameterized
SQLAlchemy core selects into Polars; writes go through ``common.write_frame``.
"""

from __future__ import annotations

from typing import Any

import polars as pl
from sqlalchemy import select

from app.core.ingest_vectorized import common

# Texas field-library mappings for office_sought / district_info, in the ORM's
# resolution order (build_campaign -> _get_field_value iterates the state mappings in
# definition order and returns the FIRST source column present in raw_data; "Hold"
# is defined before "Seek", so Hold wins when both are present).
_OFFICE_SOUGHT_SRC = ("candidateHoldOfficeCd", "candidateSeekOfficeCd")
_DISTRICT_SRC = ("candidateHoldOfficeDistrict", "candidateSeekOfficeDistrict")


def _committee_frame(session: Any, state_id: int) -> pl.DataFrame:
    """Committees for the state: filer_id + stored name (the campaign-name source)."""
    from app.core.models import UnifiedCommittee

    rows = session.execute(
        select(UnifiedCommittee.filer_id, UnifiedCommittee.name).where(
            UnifiedCommittee.state_id == state_id
        )
    ).all()
    return pl.DataFrame(
        {
            "_committee_id": [r[0] for r in rows],
            "_committee_name": [r[1] for r in rows],
        },
        schema={"_committee_id": pl.Utf8, "_committee_name": pl.Utf8},
    )


def _committee_entity_frame(session: Any, state_id: int) -> pl.DataFrame:
    """COMMITTEE entities for the state, keyed on the committee's filer_id.

    The campaign_entity COMMITTEE row links the campaign to its committee's entity; the
    ORM only adds it when ``committee.entity`` exists, so committees without a COMMITTEE
    entity get a campaign but no entity row.
    """
    from app.core.models import UnifiedEntity

    rows = session.execute(
        select(UnifiedEntity.id, UnifiedEntity.committee_id, UnifiedEntity.entity_type).where(
            UnifiedEntity.state_id == state_id
        )
    ).all()
    df = pl.DataFrame(
        {
            "_ce_entity_id": [r[0] for r in rows],
            "_committee_id": [r[1] for r in rows],
            "_ce_type": [getattr(r[2], "name", r[2]) for r in rows],
        },
        schema={"_ce_entity_id": pl.Int64, "_committee_id": pl.Utf8, "_ce_type": pl.Utf8},
    ).filter((pl.col("_ce_type") == "COMMITTEE") & pl.col("_committee_id").is_not_null())
    # One entity per committee (committee_id is unique on COMMITTEE entities; min id is
    # a stable pick if a stray duplicate ever exists).
    return df.group_by("_committee_id").agg(pl.col("_ce_entity_id").min().alias("_ce_entity_id"))


def _transaction_frame(session: Any, state_id: int) -> pl.DataFrame:
    """Transactions for the state: committee_id + transaction_date(year) + raw_data.

    ``raw_data`` is parsed (struct json decode is not available as a pure expr, so the
    office/district source columns are extracted with ``str.json_path_match`` — a native
    Polars string expression, NOT a per-row UDF).
    """
    from app.core.models import UnifiedTransaction

    rows = session.execute(
        select(
            UnifiedTransaction.committee_id,
            UnifiedTransaction.transaction_date,
            UnifiedTransaction.raw_data,
        ).where(UnifiedTransaction.state_id == state_id)
    ).all()
    return pl.DataFrame(
        {
            "_committee_id": [r[0] for r in rows],
            "_txn_year": [(r[1].year if r[1] is not None else None) for r in rows],
            "_raw": [r[2] for r in rows],
        },
        schema={"_committee_id": pl.Utf8, "_txn_year": pl.Int64, "_raw": pl.Utf8},
    )


def _office_expr(sources: tuple[str, ...]) -> pl.Expr:
    """First non-null/non-empty value among *sources* extracted from the raw_data JSON
    string, mirroring ``_get_field_value`` returning the first mapped source column
    present. ``json_path_match`` is a native string expression (no UDF)."""
    expr: pl.Expr | None = None
    for col in sources:
        # json_path_match returns null when the key is absent; clean blank -> null.
        got = pl.col("_raw").str.json_path_match("$." + col)
        got = pl.when(got.cast(pl.Utf8).str.strip_chars().str.len_chars() > 0).then(got).otherwise(
            None
        )
        expr = got if expr is None else expr.fill_null(got)
    assert expr is not None
    return expr


def finalize_campaigns(session: Any, state_id: int) -> dict[str, int]:
    """Build ``unified_campaigns`` + ``unified_campaign_entities`` for the state.

    Returns ``{"campaigns": n, "campaign_entities": m}``. See the module docstring for
    the exact ORM semantics replicated and the documented superset residual.
    """
    from app.core.models import UnifiedCampaign, UnifiedCampaignEntity

    committees = _committee_frame(session, state_id)
    txns = _transaction_frame(session, state_id)
    if committees.is_empty() or txns.is_empty():
        return {"campaigns": 0, "campaign_entities": 0}

    # Join each transaction to its committee's stored name (the campaign_name source).
    joined = txns.join(committees, on="_committee_id", how="inner").with_columns(
        common.normalize_entity_name("_committee_name").alias("_norm"),
        _office_expr(_OFFICE_SOUGHT_SRC).alias("_office"),
        _office_expr(_DISTRICT_SRC).alias("_district"),
    )
    # Skip rows whose normalized campaign name is empty (build_campaign returns None).
    joined = joined.filter(pl.col("_norm") != "")
    if joined.is_empty():
        return {"campaigns": 0, "campaign_entities": 0}

    # Dedup key = (normalized_name, committee_filer_id, candidate_id=None, election_year).
    # First occurrence wins for the stored display name / office / district (the ORM
    # keeps the first-built campaign's values; later same-key transactions reuse it).
    campaigns = joined.sort(
        ["_norm", "_committee_id", "_txn_year"], nulls_last=True
    ).unique(
        subset=["_norm", "_committee_id", "_txn_year"],
        keep="first",
        maintain_order=True,
    )

    rows = campaigns.select(
        pl.col("_committee_name").alias("name"),
        pl.col("_norm").alias("normalized_name"),
        pl.col("_txn_year").alias("election_year"),
        pl.col("_office").alias("office_sought"),
        pl.col("_district").alias("district"),
        pl.lit(None, dtype=pl.Int64).alias("candidate_person_id"),
        pl.col("_committee_id").alias("primary_committee_id"),
        pl.lit(state_id).alias("state_id"),
    )
    n_campaigns = common.write_frame(session, UnifiedCampaign, rows, conflict_cols=None)

    # Read campaign ids back (keyed on the natural dedup key) for the entity-link rows.
    cmap = _campaign_id_map(session, state_id)
    ent = _committee_entity_frame(session, state_id)
    if cmap.is_empty() or ent.is_empty():
        return {"campaigns": n_campaigns, "campaign_entities": 0}

    # One COMMITTEE campaign_entity per campaign whose committee has a COMMITTEE entity.
    links = cmap.join(ent, on="_committee_id", how="inner").select(
        pl.col("_campaign_id").alias("campaign_id"),
        pl.col("_ce_entity_id").alias("entity_id"),
        pl.lit(state_id).alias("state_id"),
        pl.lit("COMMITTEE").alias("role"),
        pl.lit(True).alias("is_primary"),
        pl.lit(None, dtype=pl.Date).alias("start_date"),
        pl.lit(None, dtype=pl.Date).alias("end_date"),
        pl.lit(None, dtype=pl.Utf8).alias("notes"),
    )
    n_entities = common.write_frame(session, UnifiedCampaignEntity, links, conflict_cols=None)
    return {"campaigns": n_campaigns, "campaign_entities": n_entities}


def _campaign_id_map(session: Any, state_id: int) -> pl.DataFrame:
    """Read campaigns back keyed on (normalized_name, primary_committee_id, election_year)
    -> surrogate id, so the entity-link rows can reference the right campaign id.

    The dedup guarantees one campaign per key; a min() fold is a stable pick if a prior
    partial run left a duplicate.
    """
    from app.core.models import UnifiedCampaign

    rows = session.execute(
        select(
            UnifiedCampaign.id,
            UnifiedCampaign.normalized_name,
            UnifiedCampaign.primary_committee_id,
            UnifiedCampaign.election_year,
        ).where(UnifiedCampaign.state_id == state_id)
    ).all()
    df = pl.DataFrame(
        {
            "_campaign_id": [r[0] for r in rows],
            "_norm": [r[1] for r in rows],
            "_committee_id": [r[2] for r in rows],
            "_txn_year": [r[3] for r in rows],
        },
        schema={
            "_campaign_id": pl.Int64,
            "_norm": pl.Utf8,
            "_committee_id": pl.Utf8,
            "_txn_year": pl.Int64,
        },
    )
    return df.group_by(["_norm", "_committee_id", "_txn_year"]).agg(
        pl.col("_campaign_id").min().alias("_campaign_id")
    )
