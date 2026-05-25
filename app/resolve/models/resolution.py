"""Resolution-layer SQLModels: crosswalk tables, match audit, and review queue.

Defines the six tables that form the resolution layer:

- ``MatchRun``       — one row per pipeline execution
- ``EntityCrosswalk``   — source unified_* records → canonical_entity
- ``AddressCrosswalk``  — source unified_addresses → canonical_address
- ``CampaignCrosswalk`` — source unified_campaigns → canonical_campaign
- ``MatchDecision``  — every pairwise decision produced by a run
- ``MergeReview``    — human review queue for medium-confidence pairs

FK columns pointing at the canonical layer (canonical_entity_id,
canonical_address_id, canonical_campaign_id) are plain integer columns
without FK constraints because the canonical-layer models are owned by
task-1a (parallel task). Full FK wiring is performed by task-1z.

Task: 1b | Branch: resolve/phase-1/task-1b-resolution-schema
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from sqlalchemy import Column, Float, Integer, Text, UniqueConstraint
from sqlmodel import Field, SQLModel

# Shared width for source identifiers flowing staging → crosswalk → decisions.
# Must match ``ResolutionInput.source_id`` and ``CandidatePair`` source columns.
SOURCE_ID_MAX_LENGTH = 128


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class SourceType(str, Enum):
    """Which unified source table a crosswalk row came from."""

    unified_person = "unified_person"
    unified_committee = "unified_committee"
    unified_entity = "unified_entity"


class MatchMethod(str, Enum):
    """Algorithm that produced a match decision or crosswalk assignment."""

    exact = "exact"
    deterministic_rule = "deterministic_rule"
    probabilistic = "probabilistic"
    approved_review = "approved_review"
    manual = "manual"


class ConfidenceBand(str, Enum):
    """Confidence level of a crosswalk assignment."""

    auto = "auto"
    review = "review"
    manual = "manual"


class PassType(str, Enum):
    """Which entity dimension a match run processes."""

    entity = "entity"
    address = "address"
    campaign = "campaign"


class RunStatus(str, Enum):
    """Current lifecycle state of a match run."""

    running = "running"
    completed = "completed"
    failed = "failed"


class DecisionBand(str, Enum):
    """Band classification of a pairwise match decision."""

    auto = "auto"
    review = "review"
    reject = "reject"


class DecisionOutcome(str, Enum):
    """Final outcome recorded for a pairwise decision."""

    merged = "merged"
    queued = "queued"
    rejected = "rejected"


class ReviewStatus(str, Enum):
    """Human-review status for a MergeReview queue entry."""

    pending = "pending"
    approved = "approved"
    rejected = "rejected"


# ---------------------------------------------------------------------------
# MatchRun — must be defined first; is the FK target for all other tables
# ---------------------------------------------------------------------------


class MatchRun(SQLModel, table=True):
    """One row per pipeline execution.

    Snapshots the engine version and config so that every downstream
    decision and crosswalk row is traceable to the exact run that produced
    it.  Integer counters accumulate stats as the run progresses.
    """

    __tablename__ = "match_run"

    id: int | None = Field(default=None, primary_key=True)
    state_code: str = Field(max_length=2, index=True)
    pass_type: PassType
    engine_version: str | None = Field(default=None, max_length=50)
    config_json: str | None = Field(
        default=None, sa_column=Column(Text, nullable=True)
    )
    started_at: datetime = Field(default_factory=_utc_now)
    finished_at: datetime | None = Field(default=None)
    status: RunStatus = Field(default=RunStatus.running)

    records_in: int = Field(default=0)
    pairs_compared: int = Field(default=0)
    auto_merges: int = Field(default=0)
    queued: int = Field(default=0)
    rejected: int = Field(default=0)
    canonical_out: int = Field(default=0)


# ---------------------------------------------------------------------------
# Crosswalk tables
# ---------------------------------------------------------------------------


class EntityCrosswalk(SQLModel, table=True):
    """Maps each source unified_person / unified_committee / unified_entity
    record to its resolved canonical_entity.

    ``source_id`` is a **string** to accommodate the string ``filer_id`` PK
    used by ``unified_committees``.

    ``match_score`` is nullable because ``exact`` and ``deterministic_rule``
    methods carry no probabilistic score.
    """

    __tablename__ = "entity_crosswalk"
    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            "run_id",
            name="uq_entity_crosswalk_source_run",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    source_type: SourceType
    source_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH, index=True)
    # Plain integer; FK → canonical_entity.id wired by task-1z.
    canonical_entity_id: int = Field(
        sa_column=Column(Integer, nullable=False, index=True)
    )
    match_method: MatchMethod
    match_score: float | None = Field(
        default=None, sa_column=Column(Float, nullable=True)
    )
    confidence_band: ConfidenceBand
    run_id: int | None = Field(
        default=None, foreign_key="match_run.id", index=True
    )
    decided_at: datetime | None = Field(default=None)
    decided_by: str | None = Field(default=None, max_length=100)


class AddressCrosswalk(SQLModel, table=True):
    """Maps each source address record to its resolved canonical_address.

    Same shape as EntityCrosswalk; ``canonical_address_id`` replaces
    ``canonical_entity_id``.
    """

    __tablename__ = "address_crosswalk"
    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            "run_id",
            name="uq_address_crosswalk_source_run",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    source_type: SourceType
    source_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH, index=True)
    # Plain integer; FK → canonical_address.id wired by task-1z.
    canonical_address_id: int = Field(
        sa_column=Column(Integer, nullable=False, index=True)
    )
    match_method: MatchMethod
    match_score: float | None = Field(
        default=None, sa_column=Column(Float, nullable=True)
    )
    confidence_band: ConfidenceBand
    run_id: int | None = Field(
        default=None, foreign_key="match_run.id", index=True
    )
    decided_at: datetime | None = Field(default=None)
    decided_by: str | None = Field(default=None, max_length=100)


class CampaignCrosswalk(SQLModel, table=True):
    """Maps each source campaign record to its resolved canonical_campaign.

    Same shape as EntityCrosswalk; ``canonical_campaign_id`` replaces
    ``canonical_entity_id``.
    """

    __tablename__ = "campaign_crosswalk"
    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            "run_id",
            name="uq_campaign_crosswalk_source_run",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    source_type: SourceType
    source_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH, index=True)
    # Plain integer; FK → canonical_campaign.id wired by task-1z.
    canonical_campaign_id: int = Field(
        sa_column=Column(Integer, nullable=False, index=True)
    )
    match_method: MatchMethod
    match_score: float | None = Field(
        default=None, sa_column=Column(Float, nullable=True)
    )
    confidence_band: ConfidenceBand
    run_id: int | None = Field(
        default=None, foreign_key="match_run.id", index=True
    )
    decided_at: datetime | None = Field(default=None)
    decided_by: str | None = Field(default=None, max_length=100)


# ---------------------------------------------------------------------------
# MatchDecision — every pairwise decision in a run
# ---------------------------------------------------------------------------


class MatchDecision(SQLModel, table=True):
    """One row per candidate pair evaluated during a match run.

    Captures both source identifiers, the Splink score, the band
    classification, and the final outcome. ``explanation_json`` stores the
    per-comparison contribution breakdown for auditability.
    """

    __tablename__ = "match_decision"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int | None = Field(
        default=None, foreign_key="match_run.id", index=True
    )
    source_a_type: SourceType
    source_a_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH)
    source_b_type: SourceType
    source_b_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH)
    score: float | None = Field(
        default=None, sa_column=Column(Float, nullable=True)
    )
    method: MatchMethod | None = Field(default=None)
    band: DecisionBand
    outcome: DecisionOutcome
    explanation_json: str | None = Field(
        default=None, sa_column=Column(Text, nullable=True)
    )


# ---------------------------------------------------------------------------
# MergeReview — human review queue
# ---------------------------------------------------------------------------


class MergeReview(SQLModel, table=True):
    """A candidate pair queued for human review.

    Approved rows become confirmed merge edges on subsequent runs.
    Rejected rows are durable: the pair is never re-queued.

    ``status`` defaults to ``pending`` so new queue entries are immediately
    visible to reviewers without an explicit write.
    """

    __tablename__ = "merge_review"

    id: int | None = Field(default=None, primary_key=True)
    run_id: int | None = Field(
        default=None, foreign_key="match_run.id", index=True
    )
    source_a_type: SourceType
    source_a_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH)
    source_b_type: SourceType
    source_b_id: str = Field(max_length=SOURCE_ID_MAX_LENGTH)
    score: float | None = Field(
        default=None, sa_column=Column(Float, nullable=True)
    )
    explanation_json: str | None = Field(
        default=None, sa_column=Column(Text, nullable=True)
    )
    status: ReviewStatus = Field(default=ReviewStatus.pending)
    reviewer: str | None = Field(default=None, max_length=100)
    decided_at: datetime | None = Field(default=None)
    notes: str | None = Field(
        default=None, sa_column=Column(Text, nullable=True)
    )
