"""Queue lifecycle functions for the merge_review human-review workflow.

Exported API
------------
list_pending(session, *, run_id=None, entity_type=None, limit=None)
    Return pending MergeReview rows, newest/highest-score first.

get_review(session, review_id)
    Fetch one row by PK; raise KeyError if absent.

approve(session, review_id, *, reviewer, notes="")
    Mark a pending row as approved.  Raises AlreadyDecidedError if the row
    has already been decided (approved or rejected).

reject(session, review_id, *, reviewer, notes="")
    Mark a pending row as rejected.  Raises AlreadyDecidedError if decided.

AlreadyDecidedError
    Raised when approve/reject is called on a row that is no longer pending.

Task: 3a | Branch: resolve/phase-3/task-3a-review-cli
"""

from __future__ import annotations

from datetime import datetime, timezone

from sqlmodel import Session, select

from app.resolve.models.resolution import MergeReview, ReviewStatus, SourceType

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

_ENTITY_TYPE_MAP: dict[str, SourceType] = {
    "person": SourceType.unified_person,
    "committee": SourceType.unified_committee,
    "entity": SourceType.unified_entity,
}


class AlreadyDecidedError(ValueError):
    """Raised when approve/reject is attempted on an already-decided row."""

    def __init__(self, review_id: int, current_status: ReviewStatus) -> None:
        super().__init__(
            f"MergeReview {review_id} is already {current_status.value!r} "
            "and cannot be re-decided."
        )
        self.review_id = review_id
        self.current_status = current_status


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def list_pending(
    session: Session,
    *,
    run_id: int | None = None,
    entity_type: str | None = None,
    limit: int | None = None,
) -> list[MergeReview]:
    """Return pending MergeReview rows, ordered by score descending (nulls last).

    Parameters
    ----------
    session:
        Active SQLModel session.
    run_id:
        When provided, restrict results to this run.
    entity_type:
        Filter by source type: one of ``"person"``, ``"committee"``,
        ``"entity"``.  Maps to the ``source_a_type`` column.
    limit:
        Maximum number of rows to return.
    """
    stmt = select(MergeReview).where(MergeReview.status == ReviewStatus.pending)

    if run_id is not None:
        stmt = stmt.where(MergeReview.run_id == run_id)

    if entity_type is not None:
        source_type = _resolve_entity_type(entity_type)
        stmt = stmt.where(MergeReview.source_a_type == source_type)

    # Nulls sort after scored rows by pulling them to the end with a secondary
    # ordering on whether the score IS NULL (False=0 before True=1).
    stmt = stmt.order_by(
        MergeReview.score.is_(None),
        MergeReview.score.desc(),
    )

    if limit is not None:
        stmt = stmt.limit(limit)

    return list(session.exec(stmt).all())


def get_review(session: Session, review_id: int) -> MergeReview:
    """Fetch a single MergeReview row by primary key.

    Raises
    ------
    KeyError
        When no row with *review_id* exists.
    """
    row = session.get(MergeReview, review_id)
    if row is None:
        raise KeyError(f"MergeReview {review_id!r} not found.")
    return row


def approve(
    session: Session,
    review_id: int,
    *,
    reviewer: str,
    notes: str = "",
) -> MergeReview:
    """Set a pending review row to *approved*.

    Parameters
    ----------
    session:
        Active SQLModel session.  The decision is committed before returning.
    review_id:
        PK of the MergeReview row to approve.
    reviewer:
        Human-readable name / ID of the person making the decision.
    notes:
        Optional free-text notes.  Defaults to an empty string.

    Raises
    ------
    KeyError
        When the review_id does not exist.
    AlreadyDecidedError
        When the row has already been approved or rejected.
    """
    return _decide(session, review_id, ReviewStatus.approved, reviewer=reviewer, notes=notes)


def reject(
    session: Session,
    review_id: int,
    *,
    reviewer: str,
    notes: str = "",
) -> MergeReview:
    """Set a pending review row to *rejected*.

    Parameters
    ----------
    session:
        Active SQLModel session.  The decision is committed before returning.
    review_id:
        PK of the MergeReview row to reject.
    reviewer:
        Human-readable name / ID of the person making the decision.
    notes:
        Optional free-text notes.  Defaults to an empty string.

    Raises
    ------
    KeyError
        When the review_id does not exist.
    AlreadyDecidedError
        When the row has already been approved or rejected.
    """
    return _decide(session, review_id, ReviewStatus.rejected, reviewer=reviewer, notes=notes)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _decide(
    session: Session,
    review_id: int,
    new_status: ReviewStatus,
    *,
    reviewer: str,
    notes: str,
) -> MergeReview:
    """Core implementation shared by approve() and reject()."""
    row = get_review(session, review_id)

    if row.status != ReviewStatus.pending:
        raise AlreadyDecidedError(review_id, row.status)

    row.status = new_status
    row.reviewer = reviewer
    row.decided_at = datetime.now(timezone.utc)
    row.notes = notes

    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _resolve_entity_type(entity_type: str) -> SourceType:
    """Map a CLI-friendly entity type name to a SourceType enum value.

    Raises
    ------
    ValueError
        When *entity_type* is not one of the recognised names.
    """
    key = entity_type.lower()
    source_type = _ENTITY_TYPE_MAP.get(key)
    if source_type is None:
        valid = ", ".join(sorted(_ENTITY_TYPE_MAP))
        raise ValueError(f"Unknown entity_type {entity_type!r}. Valid values: {valid}.")
    return source_type
