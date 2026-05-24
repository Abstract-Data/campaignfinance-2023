"""Production loader — discovers files via directory-glob and dispatches to
per-record-type ingest builders.

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
from typing import Any

# Allow ``uv run python scripts/loaders/production_loader.py`` from project root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.loaders.loader_config import (
    STATE_GLOB_CONFIGS,
    LoaderConfig,
    get_config,
)

# ---------------------------------------------------------------------------
# Transaction record types handled by the existing unified_sql_processor path
# ---------------------------------------------------------------------------
TRANSACTION_RECORD_TYPES = frozenset({
    "RCPT", "EXPN", "LOAN", "PLDG", "DEBT", "CRED", "TRVL", "ASSET", "CAND",
})


def _get_session(db_url: str | None = None):
    """Create a SQLModel session.  Lazy-imports to avoid import-time DB init."""
    from sqlmodel import Session, create_engine

    from app.core.source_models import (  # noqa: F401 — registers tables
        CommitteePurpose,
        ExpenditureCategory,
        SpacLink,
        UnifiedNotice,
        UnifiedPledge,
        UnifiedReport,
    )
    from sqlmodel import SQLModel

    if db_url is None:
        db_url = "sqlite:///campaignfinance_dev.db"

    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def _dispatch_source_record(
    raw: dict[str, Any],
    record_type: str,
    state_id: int,
    session: Any,
) -> bool:
    """Route a raw source record to the appropriate ingest builder.

    Returns
    -------
    bool
        ``True`` if the record was handled; ``False`` if unknown type.
    """
    from app.core.source_models import RECORD_TYPE_BUILDERS

    builder = RECORD_TYPE_BUILDERS.get(record_type)
    if builder is None:
        return False

    obj = builder(raw, state_id=state_id)
    session.add(obj)
    return True


def _link_after_load(session: Any) -> int:
    """Post-load: link transactions to reports and log results."""
    from app.core.source_models import link_transactions_to_reports

    linked = link_transactions_to_reports(session)
    print(f"[loader] linked {linked} transaction(s) to report(s)")
    return linked


def discover_and_load(
    state: str,
    config: LoaderConfig,
    *,
    dry_run: bool = False,
    state_id: int = 1,
    db_url: str | None = None,
) -> dict[str, int]:
    """Discover all files for *state* and load them into the database.

    Parameters
    ----------
    state:
        Lower-case state name (must be a key in ``STATE_GLOB_CONFIGS``).
    config:
        Loader configuration (batch size, limits, etc.).
    dry_run:
        If ``True``, discover files but skip actual DB writes.
    state_id:
        FK for the state row in ``states`` (default 1 for Texas).
    db_url:
        SQLAlchemy database URL.  Defaults to SQLite dev database.

    Returns
    -------
    dict[str, int]
        Counts of discovered, loaded, and skipped records.
    """
    glob_cfg = STATE_GLOB_CONFIGS.get(state)
    if glob_cfg is None:
        raise ValueError(
            f"No glob config for state {state!r}. "
            f"Available: {', '.join(STATE_GLOB_CONFIGS)}"
        )

    discovered = list(glob_cfg.discover())
    print(f"[loader] discovered {len(discovered)} file(s) for state={state!r}")

    if dry_run:
        for path, rtype in discovered:
            print(f"  {path}  (record_type={rtype!r})")
        return {"discovered": len(discovered), "loaded": 0, "skipped": 0}

    loaded = 0
    skipped = 0

    session = _get_session(db_url)
    try:
        for path, record_type in discovered:
            try:
                n = _load_file(path, record_type, config, state_id=state_id, session=session)
                loaded += n
            except Exception as exc:  # noqa: BLE001
                print(f"[loader] ERROR loading {path}: {exc}", file=sys.stderr)
                skipped += 1
                if not config.retry_failed:
                    raise

        _link_after_load(session)
    finally:
        session.close()

    return {"discovered": len(discovered), "loaded": loaded, "skipped": skipped}


def _load_file(
    path: Path,
    record_type: str | None,
    config: LoaderConfig,
    *,
    state_id: int,
    session: Any,
) -> int:
    """Load a single parquet/CSV file, routing by record type.

    Returns the number of rows loaded.
    """
    import polars as pl

    print(f"[loader] loading {path.name} (record_type={record_type!r}) ...")

    if path.suffix.lower() == ".parquet":
        df = pl.read_parquet(path)
    elif path.suffix.lower() in {".csv", ".txt"}:
        df = pl.read_csv(path, infer_schema_length=0)
    else:
        print(f"[loader] skipping unsupported file type: {path.suffix}", file=sys.stderr)
        return 0

    rows_loaded = 0
    effective_type = record_type

    for row_dict in df.to_dicts():
        # Derive record type from row if not set at file level
        if effective_type is None:
            effective_type = str(row_dict.get("recordType", "")).strip().upper() or None

        if effective_type in TRANSACTION_RECORD_TYPES:
            # Transaction path: handled by existing unified_sql_processor
            # PLDG also needs a pledge detail row (handled in unified_sql_processor)
            print(
                f"[loader] {effective_type} row → transaction processor (not implemented in this loader)",
                file=sys.stderr,
            )
            continue

        if effective_type and _dispatch_source_record(row_dict, effective_type, state_id, session):
            rows_loaded += 1
        else:
            print(
                f"[loader] unrecognized record_type={effective_type!r} in {path.name}",
                file=sys.stderr,
            )

    if rows_loaded:
        session.commit()

    return rows_loaded


if __name__ == "__main__":
    args = sys.argv[1:]
    dry = "--dry-run" in args
    args = [a for a in args if a != "--dry-run"]

    preset = args[0] if len(args) > 0 else "production"
    state = args[1] if len(args) > 1 else "texas"

    cfg = get_config(preset)
    results = discover_and_load(state, cfg, dry_run=dry)
    print(f"[loader] done: {results}")
