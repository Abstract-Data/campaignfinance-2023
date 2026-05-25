"""resolve CLI — entry point for the resolution pipeline.

Usage
-----
    python -m app.resolve run --state TX
    python -m app.resolve run --state TX --config /path/to/config.json
    python -m app.resolve run --state TX --pass-type address
    python -m app.resolve run --state TX --sqlite

The ``run`` sub-command executes the full Phase 2 stage list (stages 1→7)
via :func:`app.resolve.run.ResolutionRun.run`.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import URL

logger = logging.getLogger(__name__)

_SQLITE_URL = "sqlite://"

# Canonical name → 2-letter code for states supported in this project.
_STATE_NAME_TO_CODE: dict[str, str] = {
    "texas": "TX",
    "oklahoma": "OK",
}


def _resolve_state_code(state: str) -> str:
    """Return a 2-letter uppercase state code from a name or code."""
    lower = state.lower()
    if lower in _STATE_NAME_TO_CODE:
        return _STATE_NAME_TO_CODE[lower]
    code = state.upper()
    if len(code) != 2 or not code.isalpha():
        raise ValueError(
            f"Unrecognised state {state!r}. "
            "Pass a 2-letter code (TX) or a supported state name (texas)."
        )
    return code


# ---------------------------------------------------------------------------
# Database URL resolution
# ---------------------------------------------------------------------------


def postgres_env_configured() -> bool:
    """Return True when Postgres connection settings are present in the environment."""
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url:
        return not database_url.startswith("sqlite")
    return bool(os.environ.get("POSTGRES_USER") and os.environ.get("POSTGRES_DB"))


def build_postgres_url_from_env() -> str:
    """Build a PostgreSQL URL from ``DATABASE_URL`` or ``POSTGRES_*`` env vars.

      Uses SQLAlchemy ``URL.create`` so special characters in credentials are
    encoded correctly (never interpolated via f-string).
    """
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if database_url:
        return database_url

    user = os.environ["POSTGRES_USER"]
    password = os.environ.get("POSTGRES_PASSWORD", "")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    database = os.environ["POSTGRES_DB"]

    url = URL.create(
        drivername="postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=port,
        database=database,
    )
    return url.render_as_string(hide_password=False)


def resolve_database_url(*, use_sqlite: bool = False) -> str:
    """Return the database URL for a resolution run.

    SQLite is used only when ``use_sqlite`` is True or when no Postgres
    configuration is present (no ``DATABASE_URL`` and no ``POSTGRES_*`` vars).
    """
    if use_sqlite:
        return _SQLITE_URL
    if not postgres_env_configured():
        return _SQLITE_URL
    return build_postgres_url_from_env()


def validate_database_connection(db_url: str) -> None:
    """Verify that *db_url* is reachable; raise on failure."""
    from sqlmodel import create_engine

    engine = create_engine(db_url)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    finally:
        engine.dispose()


def resolve_engine_url(*, use_sqlite: bool = False) -> str:
    """Resolve and validate the database URL for the CLI.

    Raises
    ------
    RuntimeError
        When Postgres is configured but the connection cannot be opened.
    """
    db_url = resolve_database_url(use_sqlite=use_sqlite)
    if db_url.startswith("postgresql"):
        try:
            validate_database_connection(db_url)
        except Exception as exc:
            raise RuntimeError(
                "PostgreSQL is configured but the connection failed. "
                "Refusing to fall back to SQLite (data loss risk). "
                f"Original error: {exc}"
            ) from exc
    return db_url


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def _load_config(config_path: str | None) -> dict[str, Any]:
    """Load config from a JSON file, or return an empty dict."""
    if config_path is None:
        return {}
    path = Path(config_path)
    with path.open("r") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a JSON object; got {type(data).__name__}")
    return data


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.resolve",
        description="Resolution pipeline CLI.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # ------------------------------------------------------------------
    # run
    # ------------------------------------------------------------------
    run_p = sub.add_parser("run", help="Run the resolution pipeline for a state.")
    run_p.add_argument(
        "--state",
        required=True,
        metavar="STATE",
        help="State code or name (e.g. TX or texas).",
    )
    run_p.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="Path to a JSON config file.  Defaults to built-in defaults.",
    )
    run_p.add_argument(
        "--pass-type",
        default="entity",
        choices=["entity", "address", "campaign"],
        dest="pass_type",
        help="Which entity dimension to resolve (default: entity).",
    )
    run_p.add_argument(
        "--sqlite",
        action="store_true",
        help="Use in-memory SQLite instead of Postgres (local smoke tests).",
    )
    run_p.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging.",
    )

    # ------------------------------------------------------------------
    # review
    # ------------------------------------------------------------------
    review_p = sub.add_parser("review", help="Manage the human-review queue.")
    review_p.add_argument(
        "--sqlite",
        action="store_true",
        default=False,
        help="Force an in-memory SQLite database (for local testing).",
    )
    review_sub = review_p.add_subparsers(dest="review_command", metavar="REVIEW_COMMAND")
    review_sub.required = True

    review_list_p = review_sub.add_parser("list", help="List pending review items.")
    review_list_p.add_argument(
        "--run", dest="run_id", type=int, default=None, help="Filter by run ID."
    )
    review_list_p.add_argument(
        "--type",
        dest="entity_type",
        default=None,
        metavar="TYPE",
        help="Filter by entity type: person | committee | entity.",
    )
    review_list_p.add_argument("--limit", type=int, default=None, help="Maximum rows to display.")

    review_show_p = review_sub.add_parser("show", help="Show a pair side by side with explanation.")
    review_show_p.add_argument("review_id", type=int, help="ID of the MergeReview row.")

    review_approve_p = review_sub.add_parser("approve", help="Approve a review item.")
    review_approve_p.add_argument("review_id", type=int, help="ID of the MergeReview row.")
    review_approve_p.add_argument("--reviewer", required=True, help="Reviewer name / ID.")
    review_approve_p.add_argument("--notes", default="", help="Optional notes.")

    review_reject_p = review_sub.add_parser("reject", help="Reject a review item.")
    review_reject_p.add_argument("review_id", type=int, help="ID of the MergeReview row.")
    review_reject_p.add_argument("--reviewer", required=True, help="Reviewer name / ID.")
    review_reject_p.add_argument("--notes", default="", help="Optional notes.")

    # ------------------------------------------------------------------
    # unmerge
    # ------------------------------------------------------------------
    unmerge_p = sub.add_parser("unmerge", help="Revert a completed resolution run.")
    unmerge_p.add_argument(
        "--run",
        dest="run_id",
        type=int,
        required=True,
        help="ID of the match_run to revert.",
    )
    unmerge_p.add_argument(
        "--sqlite",
        action="store_true",
        default=False,
        help="Force an in-memory SQLite database (for local testing).",
    )

    return parser


# ---------------------------------------------------------------------------
# 'run' sub-command
# ---------------------------------------------------------------------------


def _get_run_stages():
    """Return the full Phase 2 stage callables for ``run`` (stages 1→7)."""
    from app.resolve.stages import (
        run_blocking_stage,
        run_classify_stage,
        run_cluster_stage,
        run_fastpath_stage,
        run_score_stage,
        run_survivorship_stage,
        stage1_build_resolution_input,
    )

    return [
        stage1_build_resolution_input,  # stage 1 — standardize            (1c)
        run_blocking_stage,  # stage 2 — candidate pair blocking (1e)
        run_fastpath_stage,  # stage 3 — deterministic fast-path (1f)
        run_score_stage,  # stage 4 — Splink scoring          (2a)
        run_classify_stage,  # stage 5 — band classification     (2b)
        run_cluster_stage,  # stage 6 — connected components    (2c)
        run_survivorship_stage,  # stage 7 — survivorship + publish  (1g+2d)
    ]


def _run_command(args: argparse.Namespace) -> int:
    """Execute the ``run`` sub-command.  Returns an exit code."""
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, force=True)
    else:
        logging.basicConfig(level=logging.INFO)

    try:
        state_code = _resolve_state_code(args.state)
    except ValueError as exc:
        logger.error("%s", exc)
        return 1

    try:
        config = _load_config(args.config)
    except (OSError, ValueError) as exc:
        logger.error("Failed to load config: %s", exc)
        return 1

    config.setdefault("pass_type", args.pass_type)
    config.setdefault("state_code", state_code)

    # Phase 2 starting thresholds — stored in match_run.config_json.
    config.setdefault("auto_threshold", 0.99)
    config.setdefault("review_threshold", 0.80)
    config.setdefault("max_cluster_size", 200)
    config.setdefault("seed", 42)

    try:
        db_url = resolve_engine_url(use_sqlite=args.sqlite)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    # Deferred imports keep parse-time overhead low.
    from sqlmodel import Session, create_engine

    from app.resolve.run import ResolutionRun, ensure_resolution_schema

    engine = create_engine(db_url)
    ensure_resolution_schema(engine)

    stages = _get_run_stages()

    resolution_run = ResolutionRun(state_code=state_code, config=config)

    with Session(engine) as session:
        resolution_run.run(session, stages)

    logger.info(
        "Resolution run complete: run_id=%s state=%s",
        resolution_run.run_id,
        state_code,
    )
    return 0


# ---------------------------------------------------------------------------
# 'review' sub-command
# ---------------------------------------------------------------------------


def _review_command(args: argparse.Namespace) -> int:
    """Execute the ``review`` sub-command.  Returns an exit code."""
    from app.resolve.review.cli import (
        _run_approve,
        _run_list,
        _run_reject,
        _run_show,
    )

    try:
        db_url = resolve_engine_url(use_sqlite=args.sqlite)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    from sqlmodel import Session, create_engine

    from app.resolve.run import ensure_resolution_schema

    engine = create_engine(db_url)
    ensure_resolution_schema(engine)

    with Session(engine) as session:
        if args.review_command == "list":
            _run_list(
                session,
                run_id=args.run_id,
                entity_type=args.entity_type,
                limit=args.limit,
            )
        elif args.review_command == "show":
            try:
                _run_show(session, args.review_id)
            except KeyError as exc:
                print(f"Error: {exc}", file=sys.stderr)
                return 1
        elif args.review_command == "approve":
            _run_approve(session, args.review_id, reviewer=args.reviewer, notes=args.notes)
        elif args.review_command == "reject":
            _run_reject(session, args.review_id, reviewer=args.reviewer, notes=args.notes)

    return 0


# ---------------------------------------------------------------------------
# 'unmerge' sub-command
# ---------------------------------------------------------------------------


def _unmerge_command(args: argparse.Namespace) -> int:
    """Execute the ``unmerge`` sub-command.  Returns an exit code."""
    try:
        db_url = resolve_engine_url(use_sqlite=args.sqlite)
    except RuntimeError as exc:
        logger.error("%s", exc)
        return 1

    from sqlmodel import Session, create_engine

    from app.resolve.reverse import can_unmerge, unmerge_run
    from app.resolve.run import ensure_resolution_schema

    engine = create_engine(db_url)
    ensure_resolution_schema(engine)

    with Session(engine) as session:
        ok, reason = can_unmerge(session, args.run_id)
        if not ok:
            logger.error("Cannot unmerge run_id=%d: %s", args.run_id, reason)
            return 1
        reversal = unmerge_run(session, args.run_id)

    logger.info(
        "Unmerged run_id=%d: removed %d entity crosswalk rows, rebuilt %d canonical rows",
        reversal.run_id,
        reversal.entity_crosswalk_removed,
        reversal.canonical_rows_rebuilt,
    )
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.  Returns a process exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return _run_command(args)
    if args.command == "review":
        return _review_command(args)
    if args.command == "unmerge":
        return _unmerge_command(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
