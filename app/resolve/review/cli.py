"""Reviewer CLI for the merge_review queue.

Usage
-----
    python -m app.resolve.review.cli list [--run N] [--type person] [--limit N]
    python -m app.resolve.review.cli show <review_id>
    python -m app.resolve.review.cli approve <review_id> --reviewer NAME [--notes TEXT]
    python -m app.resolve.review.cli reject  <review_id> --reviewer NAME [--notes TEXT]

Database connection follows the same rules as the main resolve CLI: reads
``DATABASE_URL`` / ``POSTGRES_*`` env vars when available, falls back to an
in-memory SQLite when none are set.  Pass ``--sqlite`` to force SQLite
explicitly (useful for testing).

The ``show`` sub-command calls ``render_explanation`` from the task-3b explain
module to render a human-readable explanation waterfall.

Task: 3z | Branch: resolve/phase-3/task-3z-integration
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from sqlmodel import Session, create_engine

from app.logger import Logger
from app.resolve.models.resolution import MergeReview
from app.resolve.review.explain import render_explanation as _render_explanation
from app.resolve.review.queue import (
    AlreadyDecidedError,
    approve,
    get_review,
    list_pending,
    reject,
)

logger = Logger(__name__)


# ---------------------------------------------------------------------------
# Internal sub-command implementations (also callable from tests)
# ---------------------------------------------------------------------------


def _run_list(
    session: Session,
    *,
    run_id: int | None,
    entity_type: str | None,
    limit: int | None,
) -> None:
    """Print a tabular summary of pending review items."""
    rows = list_pending(session, run_id=run_id, entity_type=entity_type, limit=limit)

    if not rows:
        print("No pending items in the review queue.")
        return

    header = f"{'ID':>6}  {'Run':>6}  {'Score':>7}  {'Type A':<18}  {'A ID':<20}  {'B ID':<20}"
    print(header)
    print("-" * len(header))
    for row in rows:
        score_str = f"{row.score:.4f}" if row.score is not None else "  n/a "
        run_str = str(row.run_id) if row.run_id is not None else "  -"
        print(
            f"{row.id!s:>6}  {run_str:>6}  {score_str:>7}  "
            f"{row.source_a_type.value:<18}  {row.source_a_id:<20}  {row.source_b_id:<20}"
        )

    print(f"\n{len(rows)} pending item(s) shown.")


def _run_show(session: Session, review_id: int) -> None:
    """Print a side-by-side view of the pair and its explanation."""
    row = get_review(session, review_id)

    print(f"Review ID : {row.id}")
    print(f"Run ID    : {row.run_id}")
    print(f"Status    : {row.status.value}")
    print(f"Score     : {row.score if row.score is not None else 'n/a'}")
    print()
    print(f"  A  {row.source_a_type.value} / {row.source_a_id}")
    print(f"  B  {row.source_b_type.value} / {row.source_b_id}")
    print()

    if row.reviewer:
        print(f"Reviewer  : {row.reviewer}")
        print(f"Decided   : {row.decided_at}")
        print(f"Notes     : {row.notes or ''}")
        print()

    _print_explanation(row)


def _print_explanation(row: MergeReview) -> None:
    """Print the explanation block via the task-3b renderer."""
    if row.explanation_json is None:
        print("(no explanation available)")
        return

    try:
        print(_render_explanation(row.explanation_json))
    except Exception as exc:
        logger.error(f"Failed to render explanation for review {row.id}: {exc}")
        # Fallback: pretty-print raw JSON so reviewers still see data.
        try:
            data: Any = json.loads(row.explanation_json)
            print(json.dumps(data, indent=2))
        except (json.JSONDecodeError, TypeError):
            print(row.explanation_json)


def _run_approve(
    session: Session,
    review_id: int,
    *,
    reviewer: str,
    notes: str = "",
) -> int:
    """Approve a review item and print confirmation. Returns 0 on success, 1 on error."""
    try:
        result = approve(session, review_id, reviewer=reviewer, notes=notes)
        print(
            f"Approved review {result.id} "
            f"(reviewer={result.reviewer!r}, decided_at={result.decided_at})"
        )
        return 0
    except KeyError as exc:
        logger.error(f"Error: {exc}")
        return 1
    except AlreadyDecidedError as exc:
        logger.error(f"Error: {exc}")
        return 1


def _run_reject(
    session: Session,
    review_id: int,
    *,
    reviewer: str,
    notes: str = "",
) -> int:
    """Reject a review item and print confirmation. Returns 0 on success, 1 on error."""
    try:
        result = reject(session, review_id, reviewer=reviewer, notes=notes)
        print(
            f"Rejected review {result.id} "
            f"(reviewer={result.reviewer!r}, decided_at={result.decided_at})"
        )
        return 0
    except KeyError as exc:
        logger.error(f"Error: {exc}")
        return 1
    except AlreadyDecidedError as exc:
        logger.error(f"Error: {exc}")
        return 1


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.resolve.review.cli",
        description="Manage the merge_review human-review queue.",
    )
    parser.add_argument(
        "--sqlite",
        action="store_true",
        default=False,
        help="Force an in-memory SQLite database (for local testing).",
    )

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")
    sub.required = True

    # ------------------------------------------------------------------
    # list
    # ------------------------------------------------------------------
    list_p = sub.add_parser("list", help="List pending review items.")
    list_p.add_argument("--run", dest="run_id", type=int, default=None, help="Filter by run ID.")
    list_p.add_argument(
        "--type",
        dest="entity_type",
        default=None,
        metavar="TYPE",
        help="Filter by entity type: person | committee | entity.",
    )
    list_p.add_argument("--limit", type=int, default=None, help="Maximum rows to display.")

    # ------------------------------------------------------------------
    # show
    # ------------------------------------------------------------------
    show_p = sub.add_parser("show", help="Show a pair side by side.")
    show_p.add_argument("review_id", type=int, help="ID of the MergeReview row.")

    # ------------------------------------------------------------------
    # approve
    # ------------------------------------------------------------------
    approve_p = sub.add_parser("approve", help="Approve a review item.")
    approve_p.add_argument("review_id", type=int, help="ID of the MergeReview row.")
    approve_p.add_argument("--reviewer", required=True, help="Reviewer name / ID.")
    approve_p.add_argument("--notes", default="", help="Optional notes.")

    # ------------------------------------------------------------------
    # reject
    # ------------------------------------------------------------------
    reject_p = sub.add_parser("reject", help="Reject a review item.")
    reject_p.add_argument("review_id", type=int, help="ID of the MergeReview row.")
    reject_p.add_argument("--reviewer", required=True, help="Reviewer name / ID.")
    reject_p.add_argument("--notes", default="", help="Optional notes.")

    return parser


# ---------------------------------------------------------------------------
# Database bootstrap
# ---------------------------------------------------------------------------


def _get_engine(*, use_sqlite: bool = False):
    """Return a SQLAlchemy engine; SQLite for local/test use."""
    from app.resolve.cli import resolve_engine_url
    from app.resolve.run import ensure_resolution_schema

    db_url = resolve_engine_url(use_sqlite=use_sqlite)
    engine = create_engine(db_url)
    ensure_resolution_schema(engine)
    return engine


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.  Returns 0 on success, 1 on error."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    engine = _get_engine(use_sqlite=args.sqlite)

    with Session(engine) as session:
        if args.command == "list":
            _run_list(
                session,
                run_id=args.run_id,
                entity_type=args.entity_type,
                limit=args.limit,
            )
        elif args.command == "show":
            try:
                _run_show(session, args.review_id)
            except KeyError as exc:
                logger.error(f"Error: {exc}")
                return 1
        elif args.command == "approve":
            return _run_approve(
                session, args.review_id, reviewer=args.reviewer, notes=args.notes
            )
        elif args.command == "reject":
            return _run_reject(
                session, args.review_id, reviewer=args.reviewer, notes=args.notes
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
