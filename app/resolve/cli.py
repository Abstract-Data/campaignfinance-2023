"""resolve CLI — entry point for the resolution pipeline.

Usage
-----
    python -m app.resolve run --state TX
    python -m app.resolve run --state TX --config /path/to/config.json
    python -m app.resolve run --state TX --pass-type address

The stage list is injected by ``task-1z``.  Until then, an empty list
runs a no-op pipeline (open match_run, complete it, exit 0).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

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
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser


# ---------------------------------------------------------------------------
# 'run' sub-command
# ---------------------------------------------------------------------------


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

    # Deferred imports keep parse-time overhead low.
    from sqlmodel import Session, SQLModel, create_engine

    from app.resolve.run import ResolutionRun

    # Resolve DB URL from project settings; fall back to in-memory SQLite.
    try:
        from app.states.postgres_config import PostgresConfig

        pg = PostgresConfig()
        db_url = (
            f"postgresql+psycopg2://{pg.user}:{pg.password}" f"@{pg.host}:{pg.port}/{pg.database}"
        )
    except Exception:
        logger.warning("Could not load PostgresConfig; using in-memory SQLite.")
        db_url = "sqlite://"

    engine = create_engine(db_url)

    # Ensure resolution tables exist.
    import app.resolve.models.canonical  # noqa: F401  (registers tables)
    import app.resolve.models.resolution  # noqa: F401

    SQLModel.metadata.create_all(engine)

    # task-1z injects the concrete stage list; until then run no-op.
    stages: list = []

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
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.  Returns a process exit code."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        return _run_command(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
