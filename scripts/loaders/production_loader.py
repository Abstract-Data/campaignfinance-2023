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

# Transaction types handled by unified_sql_processor as their own UnifiedTransaction.
# CAND is intentionally NOT here: a cand_* row is a candidate↔expenditure *linkage*
# (its expendInfoId IS the expenditure's id; the file holds many candidates per
# expenditure and overlaps the expend_* files), so loading it as an EXPENDITURE row
# double-counts and collides on uix_transactions_state_type_sourceid.  CAND is routed
# to the enrichment path (_persist_cand_link) instead.
TRANSACTION_RECORD_TYPES = RECORD_TYPE_CODES

# Record types that enrich an EXISTING transaction rather than creating one.
ENRICHMENT_RECORD_TYPES = frozenset({"CAND"})

# Ingest priority: lower number = processed first.
# FILER must precede all transaction types so that committee rows exist before
# transactions reference them via the committee_id FK.
# CVR1 (reports) must precede transactions so that report_id FK can be set.
_FILE_PRIORITY: dict[str, int] = {
    "FILER": 0,  # committees / filer identity — referenced by every transaction
    "CVR1": 1,  # report cover sheets — referenced by transactions via report_id
    "FINL": 2,  # final report flags — amends CVR1 rows
    "SPAC": 3,  # specific-purpose committee declarations
    # Transaction types follow; relative order among them is not critical.
    "RCPT": 10,
    "EXPN": 11,
    "LOAN": 12,
    "DEBT": 13,
    "PLDG": 14,
    "CRED": 15,
    "TRVL": 16,
    "ASSET": 17,
    "CAND": 18,
    "EXCAT": 90,  # lookup / category tables — can go last
    "CVR2": 91,
    "CVR3": 92,
}

logger = Logger(__name__)

_STATE_CODES: dict[str, tuple[str, str]] = {
    "texas": ("TX", "Texas"),
    "oklahoma": ("OK", "Oklahoma"),
}


def _get_session(db_url: str | None = None):
    """Create a SQLModel session with all source + unified tables registered.

    Defaults to the project PostgreSQL database (``POSTGRES_*`` env / ``.env``
    via :class:`PostgresConfig`).  Pass an explicit ``sqlite://`` URL (or use the
    CLI ``--sqlite`` flag) for local smoke tests.  Tables and the Fix-7 dedup
    unique indexes are created idempotently before returning the session.
    """
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
        from app.states.postgres_config import PostgresConfig

        db_url = PostgresConfig().database_url

    from sqlalchemy import event, text

    engine = create_engine(db_url)

    if db_url.startswith("sqlite"):
        # Enable FK enforcement for SQLite so missing committee / report rows
        # surface as errors rather than silently stored null FKs.
        @event.listens_for(engine, "connect")
        def _set_sqlite_pragma(dbapi_conn, _conn_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    SQLModel.metadata.create_all(engine)

    # Add any unified-model columns missing from a pre-existing table (create_all
    # never alters existing tables).  Dialect-safe + idempotent; runs on sqlite too.
    from app.core.unified_database import ensure_unified_additive_columns

    ensure_unified_additive_columns(engine)

    if not db_url.startswith("sqlite"):
        # Apply the Fix-7 partial unique indexes (raw DDL, idempotent) so dedup is
        # enforced on Postgres.  Mirrors UnifiedDatabaseManager.bootstrap().
        from app.core.unified_database import UnifiedDatabaseManager

        with engine.connect() as conn:
            for ddl in UnifiedDatabaseManager._DEDUP_INDEXES:
                conn.execute(text(ddl))
            conn.commit()

    # expire_on_commit=False keeps the BuilderCache's ORM references usable across
    # batch commits without triggering a reload SELECT on each cached attribute.
    return Session(engine, expire_on_commit=False)


def _ensure_committee_types(session: Any) -> int:
    """Upsert the committee_types seed rows.  Safe to call on every run.

    Returns the number of rows inserted (0 if all already exist).
    """
    from app.core.models.tables import CommitteeType
    from app.core.seeds.committee_types import COMMITTEE_TYPE_SEEDS

    inserted = 0
    for seed in COMMITTEE_TYPE_SEEDS:
        existing = session.get(CommitteeType, seed["code"])
        if not existing:
            session.add(CommitteeType(**seed))
            inserted += 1
    if inserted:
        session.commit()
        logger.info(f"[loader] seeded {inserted} committee_type(s)")
    return inserted


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

    # Set FK columns BEFORE detaching ORM relationships.
    # SQLAlchemy syncs FK columns from relationship state on flush, so nulling
    # a relationship AFTER explicitly setting the FK can overwrite it.
    # Setting the FK first, then expiring the relationship attribute (rather than
    # assigning None) sidesteps the bidirectional-sync race.
    transaction.committee_id = committee_filer
    transaction.report_ident = report_ident

    if file_origin_id:
        transaction.file_origin_id = file_origin_id

    # Detach heavy ORM objects so they are not cascade-added by session.add().
    # Use expire (not None-assignment) to avoid FK nullification from
    # SQLAlchemy's relationship→FK sync on flush.
    from sqlalchemy.orm import attributes as _sa_attrs

    _sa_attrs.set_committed_value(transaction, "committee", None)
    _sa_attrs.set_committed_value(transaction, "campaign", None)
    # Keep transaction.persons — clearing orphans causes NotNullViolation on
    # UnifiedTransactionPerson.transaction_id at flush time.

    return transaction


def _persist_transaction(
    session: Any,
    raw: dict[str, Any],
    *,
    state: str,
    state_id: int,
    state_code: str,
    file_origin_id: str | None,
    record_type: str | None = None,
    cache: Any = None,
    flush: bool = True,
) -> Any:
    """Process one raw row through ``process_record_stream`` and persist it.

    ``flush=False`` defers the INSERT to the next batch flush so SQLAlchemy can
    emit a single multi-row INSERT (executemany) instead of one statement per
    row — the main throughput win on PostgreSQL.  The PLDG path keeps
    ``flush=True`` because ``build_pledge`` reads ``transaction.id``.
    """
    from app.core.processor import unified_sql_processor

    transaction = next(
        unified_sql_processor.process_record_stream(
            iter([raw]),
            state,
            state_id=state_id,
            state_code=state_code,
            session=session,
            record_type=record_type,
            cache=cache,
        )
    )
    _finalize_transaction_for_persist(transaction, raw, file_origin_id=file_origin_id)
    session.add(transaction)
    if flush:
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
    cache: Any = None,
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
            record_type="PLDG",
            cache=cache,
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
    elif record_type in ("FINL", "FILER"):
        # FINL and FILER both need the live session:
        #   FINL  — looks up and mutates an existing UnifiedReport row
        #   FILER — upserts UnifiedCommittee + creates UnifiedCommitteePerson rows
        obj = builder(raw, state_id=state_id, session=session)
        if obj is None:
            return True  # handled (e.g. FINL with no matching report) — not an error
    else:
        obj = builder(raw, state_id=state_id)

    if obj is not None:
        session.add(obj)
    return True


def _link_after_load(session: Any) -> int:
    """Post-load: link transactions to reports and log results.

    Also reports the number of transactions that carry a ``report_ident`` but
    have no matching CVR1 report — so a coverage gap (e.g. a partial load, or a
    transaction whose cover sheet was never filed) is visible rather than
    silently leaving ``report_id`` NULL.
    """
    from sqlalchemy import text as _text

    from app.core.source_models import link_transactions_to_reports, reconcile_report_totals

    linked = link_transactions_to_reports(session)
    logger.info(f"[loader] linked {linked} transaction(s) to report(s)")
    unmatched = session.execute(
        _text(
            """
            SELECT count(*) FROM unified_transactions t
            WHERE t.report_ident IS NOT NULL AND t.report_id IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM unified_reports r
                  WHERE r.report_ident = t.report_ident AND r.state_id = t.state_id
              )
            """
        )
    ).scalar()
    if unmatched:
        logger.warning(
            f"[loader] {unmatched} transaction(s) have a report_ident with no matching "
            "CVR1 report (unlinked) — expected for partial loads"
        )
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

    discovered = sorted(
        [
            (item.path, item.record_type)
            for item in discover_state_files(state, base_dir=glob_cfg.base_dir)
        ],
        key=lambda p_rt: (_FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
    )
    logger.info(f"[loader] discovered {len(discovered)} file(s) for state={state!r}")

    if dry_run:
        for path, rtype in discovered:
            logger.info(f"  {path}  (record_type={rtype!r})")
        return {"discovered": len(discovered), "loaded": 0, "skipped": 0}

    loaded = 0
    skipped = 0
    rejected = 0

    from app.core.load_cache import BuilderCache

    session = _get_session(db_url)
    # One read-through dedup cache for the whole run: repeated person / committee
    # / entity / address lookups become dict hits instead of per-row SELECTs.
    cache = BuilderCache()
    try:
        # ── Ingest order: reference tables first, then transactions ──────────
        # 1. Seed committee_types lookup table (idempotent).
        _ensure_committee_types(session)
        # 2. Ensure state row exists.
        state_row = _ensure_state(session, state)
        resolved_state_id = state_id if state_id is not None else state_row.id
        state_code = state_row.code
        # 3. FILER/CVR1 files are discovered alongside transaction files.
        #    The file_discovery module should order filer files before transaction
        #    files (FILER → CVR1 → RCPT/EXPN/ASSET/…).  If discover_state_files
        #    does not guarantee this, sort explicitly here:
        #       discovered = sorted(discovered, key=lambda p_rt: _FILE_PRIORITY.get(p_rt[1], 99))
        #    See INGEST_ORDER.md for the rationale.

        for path, record_type in discovered:
            if should_stop is not None and should_stop():
                logger.info("[loader] shutdown requested; stopping after last file")
                break
            try:
                n, rej, cache = _load_file(
                    path,
                    record_type,
                    config,
                    state=state,
                    state_id=resolved_state_id,
                    state_code=state_code,
                    session=session,
                    cache=cache,
                    # Preset max_records caps rows per file so subset loads still
                    # touch every record type (assets_* no longer exhausts the budget).
                    max_remaining=config.max_records,
                )
                loaded += n
                rejected += rej
            except Exception as exc:  # noqa: BLE001
                # Backstop: _load_file already isolates bad rows into
                # ingest_errors, so reaching here means a whole-file failure.
                logger.error(f"[loader] ERROR loading {path}: {exc}")
                session.rollback()
                # Rollback expires/detaches every pending ORM object the cache
                # holds; drop it so we never hand out stale references.
                cache = BuilderCache()
                skipped += 1
                if not config.retry_failed:
                    raise

        _link_after_load(session)
    finally:
        session.close()

    return {
        "discovered": len(discovered),
        "loaded": loaded,
        "skipped": skipped,
        "rejected": rejected,
    }


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


def _persist_cand_link(
    session: Any,
    raw: dict[str, Any],
    *,
    state: str,
    state_id: int,
    state_code: str,
    cache: Any = None,
) -> str:
    """Enrich an existing EXPENDITURE with the candidate a cand_* row names.

    A cand_* row is not a transaction — its ``expendInfoId`` is the id of an
    expenditure already loaded from the expend_* files.  We resolve the candidate
    person (deduped, so candidates become resolvable entities) and attach a
    ``UnifiedTransactionPerson(role=CANDIDATE)`` to that expenditure.  Idempotent on
    the (transaction_id, person_id, role) natural key.

    Returns a status: ``linked`` / ``unlinked_no_expenditure`` /
    ``skipped_no_candidate`` / ``skipped_no_id``.
    """
    from sqlmodel import select

    from app.core.enums import PersonRole, TransactionType
    from app.core.models import UnifiedTransaction, UnifiedTransactionPerson
    from app.core.processor import unified_sql_processor

    expend_id = str(raw.get("expendInfoId") or "").strip() or None
    if not expend_id:
        return "skipped_no_id"

    builder = unified_sql_processor.get_builder(
        state, state_id, state_code, session=session, cache=cache
    )
    candidate = builder.build_person(raw, PersonRole.CANDIDATE, field_prefix="candidate")
    if candidate is None:
        return "skipped_no_candidate"

    expenditure = session.exec(
        select(UnifiedTransaction).where(
            UnifiedTransaction.state_id == state_id,
            UnifiedTransaction.transaction_type == TransactionType.EXPENDITURE,
            UnifiedTransaction.transaction_id == expend_id,
        )
    ).first()
    if expenditure is None:
        # The annotated expenditure isn't loaded (expected for partial/subset
        # loads where the matching expend_* slice is absent).  Not an error.
        return "unlinked_no_expenditure"

    # Persist the candidate (find-or-create may return a new, unflushed row) so the
    # link FK + the dedup pre-check have a real person_id.
    session.add(candidate)
    session.flush()

    already = session.exec(
        select(UnifiedTransactionPerson).where(
            UnifiedTransactionPerson.transaction_id == expenditure.id,
            UnifiedTransactionPerson.person_id == candidate.id,
            UnifiedTransactionPerson.role == PersonRole.CANDIDATE,
        )
    ).first()
    if already is not None:
        return "linked"  # idempotent: this candidate is already on this expenditure

    session.add(
        UnifiedTransactionPerson(
            transaction=expenditure,
            person=candidate,
            entity=candidate.entity,
            state_id=state_id,
            role=PersonRole.CANDIDATE,
        )
    )
    session.flush()
    return "linked"


def _persist_row(
    session: Any,
    raw: dict[str, Any],
    effective_type: str,
    *,
    state: str,
    state_id: int,
    state_code: str,
    file_origin_id: str | None,
    cache: Any,
    flush: bool,
) -> bool:
    """Process+persist a single row, routing by record type.

    Returns False for an unrecognized record type (so the caller can warn).
    """
    if effective_type in ENRICHMENT_RECORD_TYPES:
        # CAND: link the candidate to its existing expenditure (no new transaction).
        _persist_cand_link(
            session,
            raw,
            state=state,
            state_id=state_id,
            state_code=state_code,
            cache=cache,
        )
        return True
    if effective_type in TRANSACTION_RECORD_TYPES:
        if effective_type == "PLDG":
            _persist_pldg_row(
                session,
                raw,
                state=state,
                state_id=state_id,
                state_code=state_code,
                file_origin_id=file_origin_id,
                cache=cache,
            )
        else:
            _persist_transaction(
                session,
                raw,
                state=state,
                state_id=state_id,
                state_code=state_code,
                file_origin_id=file_origin_id,
                record_type=effective_type,
                cache=cache,
                flush=flush,
            )
        return True
    if effective_type and _dispatch_source_record(
        raw, effective_type, state_id, session, file_origin_id=file_origin_id
    ):
        if flush:
            session.flush()
        return True
    return False


def _record_ingest_error(
    session: Any,
    raw: dict[str, Any],
    record_type: str | None,
    exc: Exception,
    *,
    file_origin_id: str | None,
    state_id: int,
    source_file: str,
) -> None:
    """Write one rejected source row to ``ingest_errors`` (best-effort)."""
    import json as _json

    from app.core.models.tables import IngestError

    session.add(
        IngestError(
            state_id=state_id,
            file_origin_id=file_origin_id,
            record_type=record_type,
            source_file=source_file,
            error_type=type(exc).__name__,
            error_message=str(exc)[:5000],
            raw_data=_json.dumps(raw, default=str),
        )
    )


# Source record types whose insert FKs ``committee_id -> unified_committees.filer_id``
# via a raw filer-ident field.  When a batch fails, rows referencing an absent
# committee (the dominant FK-orphan failure — e.g. a subset whose FILER cap left
# the committee out) are pre-routed to ingest_errors in bulk, so we never attempt
# thousands of doomed inserts one row at a time.
_COMMITTEE_FK_SOURCE_TYPES: dict[str, str] = {"CVR1": "filerIdent", "CVR2": "filerIdent"}


def _partition_orphan_committee_rows(
    session: Any,
    pending: list[tuple[dict[str, Any], str]],
    state_id: int,
) -> tuple[list[tuple[dict[str, Any], str]], list[tuple[dict[str, Any], str]]]:
    """Split *pending* into (committee-orphans, everything-else).

    A row is an orphan when its record type FKs to ``unified_committees.filer_id``
    and the referenced ident is not present in the table.  One bulk SELECT replaces
    thousands of per-row insert/rollback attempts.
    """
    from sqlalchemy import bindparam
    from sqlalchemy import text as _text

    from app.core.source_models.reports_ingest import _optional_str

    refs: set[str] = set()
    for raw, rtype in pending:
        field = _COMMITTEE_FK_SOURCE_TYPES.get(rtype)
        if not field:
            continue
        val = _optional_str(raw.get(field))
        if val:
            refs.add(val)
    if not refs:
        return [], pending

    stmt = _text("SELECT filer_id FROM unified_committees WHERE filer_id IN :ids").bindparams(
        bindparam("ids", expanding=True)
    )
    existing = {r[0] for r in session.execute(stmt, {"ids": list(refs)}).fetchall()}

    orphans: list[tuple[dict[str, Any], str]] = []
    retry: list[tuple[dict[str, Any], str]] = []
    for raw, rtype in pending:
        field = _COMMITTEE_FK_SOURCE_TYPES.get(rtype)
        val = _optional_str(raw.get(field)) if field else None
        if field and val and val not in existing:
            orphans.append((raw, rtype))
        else:
            retry.append((raw, rtype))
    return orphans, retry


def _bulk_record_ingest_errors(
    session: Any,
    rows: list[tuple[dict[str, Any], str]],
    *,
    error_type: str,
    error_message: str,
    file_origin_id: str | None,
    state_id: int,
    source_file: str,
) -> None:
    """Route a homogeneous set of rejected rows to ``ingest_errors`` in one commit."""
    import json as _json

    from app.core.models.tables import IngestError

    for raw, rtype in rows:
        session.add(
            IngestError(
                state_id=state_id,
                file_origin_id=file_origin_id,
                record_type=rtype,
                source_file=source_file,
                error_type=error_type,
                error_message=error_message,
                raw_data=_json.dumps(raw, default=str),
            )
        )
    session.commit()


def _commit_pending(
    session: Any,
    pending: list[tuple[dict[str, Any], str]],
    *,
    state: str,
    state_id: int,
    state_code: str,
    file_origin_id: str | None,
    source_file: str,
    cache: Any,
) -> tuple[int, int, Any]:
    """Process and commit a buffered batch.

    Fast path: process all rows under one transaction and ``commit`` once.  On
    failure the batch is rolled back and recovered in three escalating tiers,
    cheapest first:

    1. **Bulk FK pre-filter** — rows referencing a committee that isn't present
       (the dominant failure, e.g. a subset whose FILER cap excluded it) are
       identified with one SELECT and routed to ``ingest_errors`` in bulk, never
       attempting their doomed inserts.
    2. **Cleaned-batch retry** — with the orphans removed, the remainder is tried
       as one commit (usually succeeds, since the orphan was the only problem).
    3. **Row-by-row isolation** — only the genuinely-bad remainder is replayed one
       row at a time, each offender routed verbatim to ``ingest_errors``.

    This keeps an all-orphan file (which used to crawl at row-by-row speed) to a
    couple of bulk statements, while never silently dropping a row.

    Returns ``(committed, rejected, cache)`` — cache is replaced after a rollback
    because rollback detaches the objects it held.
    """
    from app.core.load_cache import BuilderCache

    if not pending:
        return 0, 0, cache
    try:
        with session.no_autoflush:
            for raw, rtype in pending:
                _persist_row(
                    session,
                    raw,
                    rtype,
                    state=state,
                    state_id=state_id,
                    state_code=state_code,
                    file_origin_id=file_origin_id,
                    cache=cache,
                    flush=False,
                )
        session.commit()
        return len(pending), 0, cache
    except Exception as exc:  # noqa: BLE001
        session.rollback()
        logger.warning(
            f"[loader] batch commit failed in {source_file} ({type(exc).__name__}); recovering"
        )
        cache = BuilderCache()

    rejected = 0
    # Tier 1: bulk-route committee-orphan rows.
    orphans, retry = _partition_orphan_committee_rows(session, pending, state_id)
    if orphans:
        _bulk_record_ingest_errors(
            session,
            orphans,
            error_type="ForeignKeyViolation",
            error_message="referenced committee_id absent from unified_committees.filer_id",
            file_origin_id=file_origin_id,
            state_id=state_id,
            source_file=source_file,
        )
        rejected += len(orphans)
        # Tier 2: with orphans gone the remainder is usually clean — try once.
        if retry:
            try:
                cache = BuilderCache()
                with session.no_autoflush:
                    for raw, rtype in retry:
                        _persist_row(
                            session,
                            raw,
                            rtype,
                            state=state,
                            state_id=state_id,
                            state_code=state_code,
                            file_origin_id=file_origin_id,
                            cache=cache,
                            flush=False,
                        )
                session.commit()
                return len(retry), rejected, cache
            except Exception:  # noqa: BLE001
                session.rollback()
                cache = BuilderCache()

    # Tier 3: isolate whatever remains row-by-row.
    committed = 0
    for raw, rtype in retry:
        try:
            with session.no_autoflush:
                _persist_row(
                    session,
                    raw,
                    rtype,
                    state=state,
                    state_id=state_id,
                    state_code=state_code,
                    file_origin_id=file_origin_id,
                    cache=cache,
                    flush=True,
                )
            session.commit()
            committed += 1
        except Exception as row_exc:  # noqa: BLE001
            session.rollback()
            cache = BuilderCache()
            try:
                _record_ingest_error(
                    session,
                    raw,
                    rtype,
                    row_exc,
                    file_origin_id=file_origin_id,
                    state_id=state_id,
                    source_file=source_file,
                )
                session.commit()
            except Exception:  # noqa: BLE001
                session.rollback()
            rejected += 1
    return committed, rejected, cache


def _load_file(
    path: Path,
    record_type: str | None,
    config: LoaderConfig,
    *,
    state: str,
    state_id: int,
    state_code: str,
    session: Any,
    cache: Any = None,
    max_remaining: int | None = None,
) -> tuple[int, int, Any]:
    """Load a single parquet/CSV file.  Returns ``(loaded, rejected, cache)``."""
    from app.core.source_models import RECORD_TYPE_BUILDERS

    if _scan_file(path) is None:
        logger.warning(f"[loader] skipping unsupported file type: {path.suffix}")
        return 0, 0, cache

    logger.info(f"[loader] loading {path.name} (record_type={record_type!r}) ...")

    file_origin_id = _ensure_file_origin(session, state_id, path)
    # Make the file_origin durable so ingest_errors' FK is valid even if the very
    # first batch is the one that fails.
    session.commit()

    loaded = 0
    rejected = 0
    attempted = 0
    effective_type = record_type
    pending: list[tuple[dict[str, Any], str]] = []

    for batch_df in _iter_row_batches(path, config.batch_size):
        for row_dict in batch_df.to_dicts():
            if max_remaining is not None and attempted >= max_remaining:
                break
            if effective_type is None:
                effective_type = str(row_dict.get("recordType", "")).strip().upper() or None

            if (
                effective_type in TRANSACTION_RECORD_TYPES
                or effective_type in ENRICHMENT_RECORD_TYPES
                or effective_type in RECORD_TYPE_BUILDERS
            ):
                pending.append((row_dict, effective_type))
                attempted += 1
            else:
                logger.warning(
                    f"[loader] unrecognized record_type={effective_type!r} in {path.name}"
                )

            if len(pending) >= config.commit_frequency:
                committed, rej, cache = _commit_pending(
                    session,
                    pending,
                    state=state,
                    state_id=state_id,
                    state_code=state_code,
                    file_origin_id=file_origin_id,
                    source_file=path.name,
                    cache=cache,
                )
                loaded += committed
                rejected += rej
                pending = []

        if max_remaining is not None and attempted >= max_remaining:
            break

    committed, rej, cache = _commit_pending(
        session,
        pending,
        state=state,
        state_id=state_id,
        state_code=state_code,
        file_origin_id=file_origin_id,
        source_file=path.name,
        cache=cache,
    )
    loaded += committed
    rejected += rej
    if rejected:
        logger.warning(f"[loader] {path.name}: {rejected} row(s) routed to ingest_errors")
    return loaded, rejected, cache


if __name__ == "__main__":
    args = sys.argv[1:]
    dry = "--dry-run" in args
    use_sqlite = "--sqlite" in args
    args = [a for a in args if a not in ("--dry-run", "--sqlite")]

    preset = args[0] if len(args) > 0 else "production"
    state = args[1] if len(args) > 1 else "texas"

    # Default target is PostgreSQL.  SQLite is opt-in only: --sqlite forces it,
    # otherwise an unreachable Postgres prompts the user (interactive) or errors
    # (non-interactive) — never a silent SQLite fallback.
    from app.core.db_resolve import resolve_runtime_database_url

    db_url = resolve_runtime_database_url(force_sqlite=use_sqlite)

    cfg = get_config(preset)
    results = discover_and_load(state, cfg, dry_run=dry, db_url=db_url)
    logger.info(f"[loader] done: {results}")
