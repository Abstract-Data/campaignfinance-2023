"""Production loader CLI — discovers files and dispatches to the core pipeline.

Usage
-----
::

    uv run python scripts/loaders/production_loader.py testing texas_sample
    uv run python scripts/loaders/production_loader.py production texas

Positional arguments:
    preset      LoaderConfig preset: development | testing | production
    state       State name (e.g. texas)
    --dry-run   Discover files but skip DB writes
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow ``uv run python scripts/loaders/production_loader.py`` from project root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.db_resolve import resolve_runtime_database_url
from app.core.loader_pipeline import (
    _FILE_PRIORITY,
    ENRICHMENT_RECORD_TYPES,
    TRANSACTION_RECORD_TYPES,
    _ensure_committee_types,
    _ensure_state,
    _get_session,
    _link_after_load,
    _load_file,
    _persist_cand_link,
    _persist_pldg_row,
    discover_and_load,
)
from app.logger import Logger
from scripts.loaders.loader_config import STATE_GLOB_CONFIGS, get_config

logger = Logger(__name__)

__all__ = [
    "ENRICHMENT_RECORD_TYPES",
    "STATE_GLOB_CONFIGS",
    "TRANSACTION_RECORD_TYPES",
    "_FILE_PRIORITY",
    "_ensure_committee_types",
    "_ensure_state",
    "_get_session",
    "_link_after_load",
    "_load_file",
    "_persist_cand_link",
    "_persist_pldg_row",
    "discover_and_load",
]


if __name__ == "__main__":
    args = sys.argv[1:]
    dry = "--dry-run" in args
    use_sqlite = "--sqlite" in args
    args = [a for a in args if a not in ("--dry-run", "--sqlite")]

    preset = args[0] if len(args) > 0 else "production"
    state = args[1] if len(args) > 1 else "texas"

    db_url = resolve_runtime_database_url(force_sqlite=use_sqlite)

    cfg = get_config(preset)
    results = discover_and_load(state, cfg, dry_run=dry, db_url=db_url)
    logger.info(f"[loader] done: {results}")
