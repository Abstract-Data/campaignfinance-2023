"""Representative-subset loader for fast end-to-end validation on PostgreSQL.

The full TX dataset is ~40.7M transaction rows — infeasible to load row-by-row
through the ORM in a session.  This loads a bounded, representative slice that
exercises every record type, committee/report linking, and committee_persons,
so the corrected pipeline and the resolve stage can be validated end-to-end.

Caps are differential: filers/reports load fuller (so transactions can link to a
real committee + report), transactions are capped per file, and the huge
``contribs`` set is limited to a couple of files.

Run:  uv run python scripts/loaders/subset_load.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.load_cache import BuilderCache
from app.logger import Logger
from scripts.loaders.file_discovery import discover_state_files
from scripts.loaders.loader_config import STATE_GLOB_CONFIGS, LoaderConfig
from scripts.loaders.production_loader import (
    _FILE_PRIORITY,
    TRANSACTION_RECORD_TYPES,
    _ensure_committee_types,
    _ensure_state,
    _get_session,
    _link_after_load,
    _load_file,
)

logger = Logger(__name__)

# Per-record-type row caps (None = uncapped for that file).
SOURCE_CAPS: dict[str, int | None] = {
    "FILER": 8000,   # committees + officer committee_persons
    "CVR1": 80000,   # reports — fuller so transactions can link
    "FINL": 8000,
    "CVR2": 5000,
    "CVR3": 5000,
    "SPAC": 5000,
    "EXCAT": None,
}
TXN_CAP = 4000          # rows per transaction file
MAX_CONTRIBS_FILES = 2  # only the first N contribs_* files (each ~170k rows)


def main(state: str = "texas") -> None:
    glob_cfg = STATE_GLOB_CONFIGS[state]
    discovered = sorted(
        ((d.path, d.record_type) for d in discover_state_files(state, base_dir=glob_cfg.base_dir)),
        key=lambda p_rt: (_FILE_PRIORITY.get(p_rt[1] or "", 50), str(p_rt[0])),
    )

    # Thin the huge contribs set down to a couple of files.
    contribs_seen = 0
    plan: list[tuple[Path, str, int | None]] = []
    for path, rtype in discovered:
        if path.name.startswith("contribs_"):
            contribs_seen += 1
            if contribs_seen > MAX_CONTRIBS_FILES:
                continue
        if rtype in TRANSACTION_RECORD_TYPES:
            cap = TXN_CAP
        else:
            cap = SOURCE_CAPS.get(rtype or "", 5000)
        plan.append((path, rtype, cap))

    logger.info(f"[subset] {len(plan)} files planned for state={state!r}")

    # commit_frequency high so INSERTs batch into large executemany statements.
    config = LoaderConfig(batch_size=5000, commit_frequency=5000)
    session = _get_session(None)
    cache = BuilderCache()
    loaded = 0
    t0 = time.time()
    try:
        _ensure_committee_types(session)
        state_row = _ensure_state(session, state)
        rejected = 0
        for path, rtype, cap in plan:
            try:
                n, rej, cache = _load_file(
                    path, rtype, config,
                    state=state, state_id=state_row.id, state_code=state_row.code,
                    session=session, cache=cache, max_remaining=cap,
                )
                loaded += n
                rejected += rej
                logger.info(
                    f"[subset] {path.name} (+{n}, rej {rej}) total={loaded} "
                    f"elapsed={time.time()-t0:.0f}s"
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(f"[subset] ERROR {path.name}: {exc}")
                session.rollback()
                cache = BuilderCache()
        _link_after_load(session)
    finally:
        session.close()
    logger.info(
        f"[subset] done: loaded={loaded} rejected={rejected} in {time.time()-t0:.0f}s"
    )


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "texas")
