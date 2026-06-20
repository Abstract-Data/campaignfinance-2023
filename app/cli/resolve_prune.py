"""cf resolve prune — remove stale resolve run rows, keeping latest N runs.

Removes in FK-safe order, mirroring app/resolve/reverse.py:
  match_decision -> merge_review -> scored_pairs -> candidate_pairs ->
  merge_edges -> cluster_assignment -> resolution_input ->
  entity_crosswalk / address_crosswalk / campaign_crosswalk ->
  match_run headers (only with --purge-headers)

Idempotent and transactional: either all stale rows are removed or none.
"""

from __future__ import annotations

from typing import Annotated

import typer
from sqlalchemy import delete, func, select
from sqlmodel import Session

from app.logger import Logger

logger = Logger(__name__)
app = typer.Typer()


def _stale_run_ids(session: Session, keep: int) -> list[int]:
    """Return run_ids to prune (all except the latest *keep*)."""
    from app.resolve.models.resolution import MatchRun

    rows = session.execute(select(MatchRun.id).order_by(MatchRun.id.desc())).scalars().all()

    if len(rows) <= keep:
        return []
    return list(rows[keep:])


def _count_rows(session: Session, model_cls: type, run_id_attr: str, stale_ids: list[int]) -> int:
    """Count rows in model_cls where run_id_attr is in stale_ids."""
    if not stale_ids:
        return 0
    col = getattr(model_cls, run_id_attr)
    return session.execute(select(func.count()).where(col.in_(stale_ids))).scalar_one()


def _remove_rows(session: Session, model_cls: type, run_id_attr: str, stale_ids: list[int]) -> int:
    """Remove rows from model_cls where run_id_attr is in stale_ids.

    Returns the number of rows that would be (or were) removed.
    """
    if not stale_ids:
        return 0
    col = getattr(model_cls, run_id_attr)
    count = session.execute(select(func.count()).where(col.in_(stale_ids))).scalar_one()
    if count > 0:
        session.execute(delete(model_cls).where(col.in_(stale_ids)))
    return count


@app.command(name="prune")
def prune(
    keep: Annotated[
        int,
        typer.Option("--keep", help="Number of most recent run_ids to retain."),
    ] = 1,
    purge_headers: Annotated[
        bool,
        typer.Option(
            "--purge-headers",
            help="Also remove match_run header rows for stale runs.",
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Show what would be removed without removing."),
    ] = False,
) -> None:
    """Remove stale resolve run rows, keeping the latest --keep run_ids.

    Removes in FK-safe order. Idempotent and transactional.
    """
    from app.core.unified_database import get_db_manager
    from app.resolve.blocking import CandidatePair
    from app.resolve.models.resolution import (
        AddressCrosswalk,
        CampaignCrosswalk,
        EntityCrosswalk,
        MatchDecision,
        MatchRun,
        MergeReview,
    )
    from app.resolve.stages.cluster import ClusterAssignment
    from app.resolve.stages.fastpath import MergeEdge
    from app.resolve.stages.score import ScoredPair
    from app.resolve.standardize.staging import ResolutionInput

    manager = get_db_manager(bootstrap=False)
    prefix = "[DRY RUN] " if dry_run else ""

    with Session(manager.engine) as session:
        stale_ids = _stale_run_ids(session, keep)
        if not stale_ids:
            msg = "No stale runs to prune."
            logger.info(msg)
            typer.echo(prefix + msg)
            return

        logger.info("cf resolve prune: pruning run_ids=" + repr(stale_ids))
        typer.echo(prefix + "Stale run_ids to prune: " + repr(stale_ids))
        typer.echo(prefix + "Keeping latest " + str(keep) + " run(s).")

        totals: dict[str, int] = {}

        # FK-safe order — matches reverse.py
        for model, label in [
            (MatchDecision, "match_decision"),
            (MergeReview, "merge_review"),
            (ScoredPair, "scored_pairs"),
            (CandidatePair, "candidate_pairs"),
            (MergeEdge, "merge_edges"),
            (ClusterAssignment, "cluster_assignment"),
            (ResolutionInput, "resolution_input"),
            (EntityCrosswalk, "entity_crosswalk"),
            (AddressCrosswalk, "address_crosswalk"),
            (CampaignCrosswalk, "campaign_crosswalk"),
        ]:
            if dry_run:
                n = _count_rows(session, model, "run_id", stale_ids)
            else:
                n = _remove_rows(session, model, "run_id", stale_ids)
            totals[label] = n
            if n > 0:
                typer.echo("  " + prefix + label + ": " + str(n) + " rows")

        if purge_headers:
            if dry_run:
                n = _count_rows(session, MatchRun, "id", stale_ids)
            else:
                n = _remove_rows(session, MatchRun, "id", stale_ids)
            totals["match_run"] = n
            if n > 0:
                typer.echo("  " + prefix + "match_run: " + str(n) + " rows")

        total_rows = sum(totals.values())

        if not dry_run:
            session.commit()
            logger.info("cf resolve prune: committed. totals=" + repr(totals))
            typer.echo("\nDone. Pruned " + str(total_rows) + " total rows.")
        else:
            typer.echo(
                "\n[DRY RUN] Would prune " + str(total_rows) + " total rows. "
                "Run without --dry-run to apply."
            )
