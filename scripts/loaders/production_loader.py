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
from collections.abc import Callable
from pathlib import Path
from typing import Any

# Allow ``uv run python scripts/loaders/production_loader.py`` from project root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.constants import RECORD_TYPE_CODES
from app.logger import Logger
from scripts.loaders.file_discovery import discover_state_files
from scripts.loaders.loader_config import (
    STATE_GLOB_CONFIGS,
    LoaderConfig,
    get_config,
)

# Transaction types handled by unified_sql_processor (CAND uses a separate path).
TRANSACTION_RECORD_TYPES = RECORD_TYPE_CODES | frozenset({"CAND"})

logger = Logger(__name__)

_STATE_CODES: dict[str, tuple[str, str]] = {
    "texas": ("TX", "Texas"),
    "oklahoma": ("OK", "Oklahoma"),
}


def _get_session(db_url: str | None = None):
    """Create a SQLModel session with all source + unified tables registered."""
    from sqlmodel import Session, SQLModel, create_engine

    from app.core import models  # noqa: F401 — registers unified_* tables
    from app.core.source_models import (  # noqa: F401 — registers Phase-0 tables
        CommitteePurpose,
        ExpenditureCategory,
        SpacLink,
        UnifiedNotice,
        UnifiedPledge,
        UnifiedReport,
    )

    if db_url is None:
        db_url = "sqlite:///campaignfinance_dev.db"

    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def _ensure_state(session: Any, state_name: str) -> Any:
    """Return the ``states`` row for *state_name*, creating it if needed."""
    from sqlmodel import select

    from app.core.models import State

    code, name = _STATE_CODES.get(state_name.lower(), (state_name[:2].upper(), state_name.title()))
    existing = session.exec(select(State).where(State.code == code)).first()
    if existing:
        return existing

    row = State(code=code, name=name)
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _file_origin_id(state_id: int, path: Path) -> str:
    from app.core.models import FileOrigin

    return FileOrigin.build_key(state_id, path.name)


def _ensure_file_origin(session: Any, state_id: int, path: Path) -> str:
    from app.core.models import FileOrigin

    origin_id = _file_origin_id(state_id, path)
    existing = session.get(FileOrigin, origin_id)
    if existing:
        return origin_id

    session.add(FileOrigin(id=origin_id, state_id=state_id, filename=path.name))
    session.flush()
    return origin_id


def _finalize_transaction_for_persist(
    transaction: Any,
    raw: dict[str, Any],
    *,
    file_origin_id: str | None,
) -> Any:
    """Apply loader-specific FK/metadata overrides before session.add."""
    committee_filer = transaction.committee_id
    if not committee_filer:
        filer = raw.get("filerIdent")
        if filer is not None:
            committee_filer = str(filer).strip() or None

    report_ident = transaction.report_ident
    if not report_ident:
        report_info = raw.get("reportInfoIdent")
        if report_info is not None:
            report_ident = str(report_info).strip() or None

    transaction.committee = None
    transaction.campaign = None
    transaction.persons = []
    transaction.committee_id = committee_filer
    transaction.report_ident = report_ident

    if file_origin_id:
        transaction.file_origin_id = file_origin_id
    return transaction


def _persist_transaction(
    session: Any,
    raw: dict[str, Any],
    *,
    state: str,
    state_id: int,
    state_code: str,
    file_origin_id: str | None,
) -> Any:
    """Process one raw row through ``process_record_stream`` and persist it."""
    from app.core.processor import unified_sql_processor

    transaction = next(
        unified_sql_processor.process_record_stream(
            iter([raw]),
            state,
            state_id=state_id,
            state_code=state_code,
            session=session,
        )
    )
    _finalize_transaction_for_persist(transaction, raw, file_origin_id=file_origin_id)
    session.add(transaction)
    session.flush()
    return transaction


def _persist_pldg_row(
    session: Any,
    raw: dict[str, Any],
    *,
    state: str,
    state_id: int,
    state_code: str,
    file_origin_id: str | None,
) -> Any:
    """Persist a PLDG transaction and its pledge detail in one savepoint."""
    from app.core.source_models.pledges_ingest import build_pledge

    with session.begin_nested():
        txn = _persist_transaction(
            session,
            raw,
            state=state,
            state_id=state_id,
            state_code=state_code,
            file_origin_id=file_origin_id,
        )
        session.add(
            build_pledge(
                txn,
                pledgor_entity=None,
                recipient_entity=None,
                raw=raw,
                state_id=state_id,
            )
        )
    return txn


def _dispatch_source_record(
    raw: dict[str, Any],
    record_type: str,
    state_id: int,
    session: Any,
    *,
    file_origin_id: str | None = None,
) -> bool:
    """Route a raw source record to the appropriate ingest builder."""
    from app.core.source_models import RECORD_TYPE_BUILDERS

    builder = RECORD_TYPE_BUILDERS.get(record_type)
    if builder is None:
        return False

    if record_type == "CVR1":
        obj = builder(raw, state_id=state_id, file_origin_id=file_origin_id)
    else:
        obj = builder(raw, state_id=state_id)
    session.add(obj)
    return True


def _link_after_load(session: Any) -> int:
    """Post-load: link transactions to reports and log results."""
    from app.core.source_models import link_transactions_to_reports, reconcile_report_totals

    linked = link_transactions_to_reports(session)
    logger.info(f"[loader] linked {linked} transaction(s) to report(s)")
    reconcile_report_totals(session)
    return linked


def discover_and_load(
    state: str,
    config: LoaderConfig,
    *,
    dry_run: bool = False,
    state_id: int | None = None,
    db_url: str | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, int]:
    """Discover all files for *state* and load them into the database."""
    glob_cfg = STATE_GLOB_CONFIGS.get(state)
    if glob_cfg is None:
        raise ValueError(
            f"No glob config for state {state!r}. Available: {', '.join(STATE_GLOB_CONFIGS)}"
        )

    discovered = [
        (item.path, item.record_type)
        for item in discover_state_files(state, base_dir=glob_cfg.base_dir)
    ]
    logger.info(f"[loader] discovered {len(discovered)} file(s) for state={state!r}")

    if dry_run:
        for path, rtype in discovered:
            logger.info(f"  {path}  (record_type={rtype!r})")
        return {"discovered": len(discovered), "loaded": 0, "skipped": 0}

    loaded = 0
    skipped = 0

    session = _get_session(db_url)
    try:
        state_row = _ensure_state(session, state)
        resolved_state_id = state_id if state_id is not None else state_row.id
        state_code = state_row.code

        for path, record_type in discovered:
            if should_stop is not None and should_stop():
                logger.info("[loader] shutdown requested; stopping after last file")
                break
            try:
                n = _load_file(
                    path,
                    record_type,
                    config,
                    state=state,
                    state_id=resolved_state_id,
                    state_code=state_code,
                    session=session,
                    # Preset max_records caps rows per file so subset loads still
                    # touch every record type (assets_* no longer exhausts the budget).
                    max_remaining=config.max_records,
                )
                loaded += n
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[loader] ERROR loading {path}: {exc}")
                session.rollback()
                skipped += 1
                if not config.retry_failed:
                    raise

        _link_after_load(session)
    finally:
        session.close()

    return {"discovered": len(discovered), "loaded": loaded, "skipped": skipped}


def _scan_file(path: Path):
    """Return a Polars LazyFrame for *path* (parquet or CSV)."""
    import polars as pl

    if path.suffix.lower() == ".parquet":
        return pl.scan_parquet(path)
    if path.suffix.lower() in {".csv", ".txt"}:
        return pl.scan_csv(path, infer_schema_length=0)
    return None


def _iter_row_batches(path: Path, batch_size: int):
    """Yield DataFrame slices bounded by *batch_size* using lazy scan + streaming collect."""
    lf = _scan_file(path)
    if lf is None:
        return
    df = lf.collect(streaming=True)
    for batch in df.iter_slices(n_rows=batch_size):
        yield batch


def _load_file(
    path: Path,
    record_type: str | None,
    config: LoaderConfig,
    *,
    state: str,
    state_id: int,
    state_code: str,
    session: Any,
    max_remaining: int | None = None,
) -> int:
    """Load a single parquet/CSV file, routing by record type."""
    if _scan_file(path) is None:
        logger.warning(f"[loader] skipping unsupported file type: {path.suffix}")
        return 0

    logger.info(f"[loader] loading {path.name} (record_type={record_type!r}) ...")

    file_origin_id = _ensure_file_origin(session, state_id, path)
    rows_loaded = 0
    effective_type = record_type
    batch_count = 0

    for batch_df in _iter_row_batches(path, config.batch_size):
        for row_dict in batch_df.to_dicts():
            if max_remaining is not None and rows_loaded >= max_remaining:
                break

            if effective_type is None:
                effective_type = str(row_dict.get("recordType", "")).strip().upper() or None

            if effective_type in TRANSACTION_RECORD_TYPES:
                if effective_type == "PLDG":
                    _persist_pldg_row(
                        session,
                        row_dict,
                        state=state,
                        state_id=state_id,
                        state_code=state_code,
                        file_origin_id=file_origin_id,
                    )
                else:
                    _persist_transaction(
                        session,
                        row_dict,
                        state=state,
                        state_id=state_id,
                        state_code=state_code,
                        file_origin_id=file_origin_id,
                    )
                rows_loaded += 1

                batch_count += 1
                if batch_count >= config.commit_frequency:
                    session.commit()
                    batch_count = 0
                continue

            if effective_type and _dispatch_source_record(
                row_dict,
                effective_type,
                state_id,
                session,
                file_origin_id=file_origin_id,
            ):
                rows_loaded += 1
                batch_count += 1
                if batch_count >= config.commit_frequency:
                    session.commit()
                    batch_count = 0
            else:
                logger.warning(
                    f"[loader] unrecognized record_type={effective_type!r} in {path.name}"
                )

        if max_remaining is not None and rows_loaded >= max_remaining:
            break

    if batch_count:
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
    logger.info(f"[loader] done: {results}")
