"""Reversibility tooling: unmerge a completed resolution run.

Provides two public callables:

- ``can_unmerge(session, run_id)``  — guard check before reversal
- ``unmerge_run(session, run_id)``  — transactional unmerge

The sentinel string ``"reverted"`` is written to ``match_run.status`` via a
parameterized raw SQL UPDATE to avoid requiring a new enum member in the
read-only ``RunStatus`` enum (task-3c must not modify any file outside its
own Files list).  All other DB operations use SQLAlchemy ORM constructs.

Task: 3c | Branch: resolve/phase-3/task-3c-reversibility
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import delete, func, select, text
from sqlmodel import Session

from app.resolve.models.canonical import CanonicalEntity, CanonicalNameHistory
from app.resolve.models.resolution import (
    AddressCrosswalk,
    CampaignCrosswalk,
    EntityCrosswalk,
    MatchDecision,
    MergeReview,
)
from app.resolve.stages.survivorship import run_survivorship_stage

# Status sentinel written via parameterized raw SQL — not a RunStatus enum member.
_REVERTED: str = "reverted"
_COMPLETED: str = "completed"


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass
class RunReversal:
    """Summary returned by :func:`unmerge_run` after a successful reversal."""

    run_id: int
    entity_crosswalk_removed: int
    address_crosswalk_removed: int
    campaign_crosswalk_removed: int
    match_decision_removed: int
    merge_review_removed: int
    canonical_rows_rebuilt: int


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_run_raw(session: Session, run_id: int) -> dict | None:
    """Return a ``match_run`` row as a plain dict, bypassing ORM enum coercion.

    Necessary because a reverted run has ``status='reverted'``, which is not
    a valid ``RunStatus`` enum member; loading it via the ORM would raise a
    Pydantic validation error.
    """
    row = session.execute(
        text("SELECT id, state_code, pass_type, status " "FROM match_run WHERE id = :run_id"),
        {"run_id": run_id},
    ).first()
    if row is None:
        return None
    return dict(row._mapping)


def _count_crosswalk_rows(session: Session, run_id: int) -> tuple[int, int, int]:
    """Return (entity_xwalk_count, address_xwalk_count, campaign_xwalk_count)."""
    ec = session.execute(
        select(func.count()).select_from(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id)
    ).scalar_one()
    ac = session.execute(
        select(func.count()).select_from(AddressCrosswalk).where(AddressCrosswalk.run_id == run_id)
    ).scalar_one()
    cc = session.execute(
        select(func.count())
        .select_from(CampaignCrosswalk)
        .where(CampaignCrosswalk.run_id == run_id)
    ).scalar_one()
    return ec, ac, cc


def _count_decision_rows(session: Session, run_id: int) -> tuple[int, int]:
    """Return (match_decision_count, merge_review_count)."""
    md = session.execute(
        select(func.count()).select_from(MatchDecision).where(MatchDecision.run_id == run_id)
    ).scalar_one()
    mr = session.execute(
        select(func.count()).select_from(MergeReview).where(MergeReview.run_id == run_id)
    ).scalar_one()
    return md, mr


def _latest_completed_run_id(
    session: Session,
    state_code: str,
    pass_type: str,
) -> int | None:
    """Return the id of the most recent completed (non-reverted) run.

    Filters on ``status = 'completed'`` which already excludes reverted,
    running, and failed runs.
    """
    row = session.execute(
        text(
            "SELECT id FROM match_run "
            "WHERE state_code = :state_code "
            "  AND pass_type = :pass_type "
            "  AND status = 'completed' "
            "ORDER BY id DESC LIMIT 1"
        ),
        {"state_code": state_code, "pass_type": pass_type},
    ).first()
    return row[0] if row else None


def _mark_run_reverted(session: Session, run_id: int) -> None:
    """Set ``match_run.status = 'reverted'`` via parameterized raw SQL.

    Uses ``text()`` with bound parameters only — the status value is a
    hard-coded sentinel, not user-supplied input.
    """
    session.execute(
        text("UPDATE match_run SET status = :status WHERE id = :run_id"),
        {"status": _REVERTED, "run_id": run_id},
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def can_unmerge(session: Session, run_id: int) -> tuple[bool, str]:
    """Guard: return ``(True, "")`` only if *run_id* is safely reversible.

    Only the most recent non-reverted completed run for its ``(state_code,
    pass_type)`` pair may be safely unmerged.  Reversing an older run would
    leave newer runs with stale canonical references; the caller must revert
    those later runs first.

    Returns
    -------
    (True, "")
        The run may be unmerged.
    (False, reason)
        *reason* explains why reversal is blocked.
    """
    raw = _get_run_raw(session, run_id)
    if raw is None:
        return False, f"run_id={run_id} not found"

    status: str = raw["status"]
    if status == _REVERTED:
        return False, f"run_id={run_id} is already reverted"
    if status != _COMPLETED:
        return False, (
            f"run_id={run_id} has status={status!r}; " "only completed runs can be unmerged"
        )

    latest_id = _latest_completed_run_id(
        session,
        state_code=raw["state_code"],
        pass_type=raw["pass_type"],
    )
    if latest_id is None:
        return False, "no completed runs found (internal inconsistency)"

    if latest_id != run_id:
        return False, (
            f"run_id={run_id} is not the latest non-reverted completed run; "
            f"revert run_id={latest_id} first"
        )

    return True, ""


def unmerge_run(session: Session, run_id: int) -> RunReversal:
    """Revert a completed ``match_run`` transactionally.

    Steps
    -----
    1. Guard via :func:`can_unmerge`; raise ``ValueError`` if blocked.
    2. Count and delete the run's resolution-layer rows:
       ``entity_crosswalk``, ``address_crosswalk``, ``campaign_crosswalk``,
       ``match_decision``, and ``merge_review`` rows surfaced by this run.
       Decided ``merge_review`` rows from *prior* runs are left untouched.
    3. Mark ``match_run.status = 'reverted'`` via parameterized raw SQL.
    4. Rebuild the canonical layer by re-running survivorship for the most
       recent prior completed run.  ``run_survivorship_stage`` clears the live
       canonical snapshot internally before writing new rows and then issues
       a ``session.commit()`` — committing ALL preceding operations in this
       transaction.  If no prior run exists, canonical tables are emptied and
       committed explicitly.
    5. Return a :class:`RunReversal` summary.

    Raises
    ------
    ValueError
        If ``can_unmerge`` returns ``(False, reason)``.
    """
    ok, reason = can_unmerge(session, run_id)
    if not ok:
        raise ValueError(f"Cannot unmerge run_id={run_id}: {reason}")

    raw = _get_run_raw(session, run_id)
    assert raw is not None, "guarded by can_unmerge above"
    state_code: str = raw["state_code"]
    pass_type: str = raw["pass_type"]

    # ------------------------------------------------------------------
    # Count rows before deletion (returned in RunReversal summary)
    # ------------------------------------------------------------------
    ec_removed, ac_removed, cc_removed = _count_crosswalk_rows(session, run_id)
    md_removed, mr_removed = _count_decision_rows(session, run_id)

    # ------------------------------------------------------------------
    # Delete resolution-layer rows for this run using ORM delete statements
    # ------------------------------------------------------------------
    session.exec(delete(EntityCrosswalk).where(EntityCrosswalk.run_id == run_id))
    session.exec(delete(AddressCrosswalk).where(AddressCrosswalk.run_id == run_id))
    session.exec(delete(CampaignCrosswalk).where(CampaignCrosswalk.run_id == run_id))
    session.exec(delete(MatchDecision).where(MatchDecision.run_id == run_id))
    session.exec(delete(MergeReview).where(MergeReview.run_id == run_id))

    # ------------------------------------------------------------------
    # Mark the run as reverted (parameterized text() — value is hard-coded)
    # ------------------------------------------------------------------
    _mark_run_reverted(session, run_id)

    # ------------------------------------------------------------------
    # Rebuild canonical from the most recent prior completed run.
    # run_survivorship_stage calls _clear_live_canonical_snapshot (deletes
    # all canonical_entity / canonical_name_history rows) then commits
    # the entire transaction — including the preceding ORM deletes and the
    # status update.
    # ------------------------------------------------------------------
    prior_run_id = _latest_completed_run_id(
        session,
        state_code=state_code,
        pass_type=pass_type,
    )

    canonical_rebuilt = 0
    if prior_run_id is not None:
        config = {"state_code": state_code, "pass_type": pass_type}
        result = run_survivorship_stage(session, prior_run_id, config)
        canonical_rebuilt = result.get("canonical_out", 0)
    else:
        # No prior run: empty the live canonical snapshot and commit.
        session.exec(delete(CanonicalNameHistory))
        session.exec(delete(CanonicalEntity))
        session.commit()

    return RunReversal(
        run_id=run_id,
        entity_crosswalk_removed=ec_removed,
        address_crosswalk_removed=ac_removed,
        campaign_crosswalk_removed=cc_removed,
        match_decision_removed=md_removed,
        merge_review_removed=mr_removed,
        canonical_rows_rebuilt=canonical_rebuilt,
    )
