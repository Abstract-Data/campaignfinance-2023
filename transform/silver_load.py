"""Silver loader shim — the "EL" of the ELT spike.

Discovers Texas bronze parquet, validates each row with the EXISTING Pydantic
validators (``TECContribution`` / ``TECExpense`` — no validation logic rewritten),
and lands the clean rows into the ``silver`` schema. Dirty rows are rejected and
counted, exactly as the prompt requires ("validate at the boundary, transform in
SQL"). dbt then transforms silver -> gold -> public.unified_*.

Silver columns are written as uniform TEXT (staging casts ::numeric/::date) so a
parallel, multi-process load can append to one pre-created table without type
races. Validation is the bottleneck at full volume (~40M rows), so ``--all-files``
fans file validation out across CPU cores.

Usage:
    uv run python -m transform.silver_load                      # bootstrap + small sample
    uv run python -m transform.silver_load --max-files 3        # more files per type
    uv run python -m transform.silver_load --limit 5000         # cap rows per type
    uv run python -m transform.silver_load --all-files          # full volume, parallel
"""

from __future__ import annotations

import argparse
import math
import os
import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import pandas as pd
import polars as pl
from sqlalchemy import create_engine

from app.states.texas.validators.texas_contributions import TECContribution
from app.states.texas.validators.texas_expenses import TECExpense
from scripts.loaders.file_discovery import discover_state_files
from transform._spikedb import SILVER_SCHEMA, bootstrap_unified, ensure_state, spike_url

# record_type -> (validator model, silver table name). These two keep the existing
# Pydantic validators (already tested).
SILVER_TARGETS = {
    "RCPT": (TECContribution, "tx_contributions"),
    "EXPN": (TECExpense, "tx_expenditures"),
}

# record_type -> silver table for the remaining transaction types. These land RAW
# (all-TEXT, no per-row validator) because the per-type TEC validators are
# inconsistent (snake_case/camelCase/missing); the raw TEC columns are clean
# camelCase and dbt staging does safe casting. FILER (committee master) lands
# separately via load_filers.
RAW_TARGETS = {
    "LOAN": "tx_loans",
    "DEBT": "tx_debts",
    "PLDG": "tx_pledges",
    "CRED": "tx_credits",
    "TRVL": "tx_travel",
    "ASSET": "tx_assets",
    "CAND": "tx_candidate",
}

# Flush validated rows to Postgres every this many — bounds memory per worker.
_FLUSH_ROWS = 50_000


@dataclass
class LoadResult:
    record_type: str
    table: str
    files: int = 0
    valid: int = 0
    rejected: int = 0
    reject_samples: list[str] = field(default_factory=list)


def _clean_row(row: dict, *, file_origin: str, download_date: str) -> dict:
    """Render the row as the validators expect: all-string, empty-as-"" (CSV
    semantics). The TEC validators filter empties with ``!= ""``; parquet nulls
    arrive as None, so we map None -> "" to keep that filter working and stamp
    provenance. (The validators' own clear_blank_strings then re-nulls as needed.)
    """
    cleaned = {k: ("" if v is None else v) for k, v in row.items()}
    cleaned["file_origin"] = file_origin
    cleaned["download_date"] = download_date
    return cleaned


def _to_text_frame(rows: list[dict]) -> pd.DataFrame:
    """Lower-case columns and stringify every value (None preserved). Silver is
    uniform TEXT so parallel appends never race on an inferred column type and
    staging does the casting."""
    df = pd.DataFrame(rows)
    df.columns = [c.lower() for c in df.columns]
    df = df.astype(object).where(pd.notnull(df), None)
    for col in df.columns:
        df[col] = [None if v is None else str(v) for v in df[col]]
    return df


def _ensure_empty_table(engine, model, table: str) -> None:
    """Create an empty all-TEXT silver table from the validator field names so
    parallel workers can append into a stable, pre-existing schema."""
    cols = [name.lower() for name in model.model_fields]
    empty = pd.DataFrame({c: pd.Series(dtype=object) for c in cols})
    empty.to_sql(table, engine, schema=SILVER_SCHEMA, if_exists="replace", index=False)


def _append_batch(rows: list[dict], table: str, engine) -> None:
    if not rows:
        return
    # chunksize keeps params-per-INSERT under Postgres' 65535 limit (method='multi').
    _to_text_frame(rows).to_sql(
        table, engine, schema=SILVER_SCHEMA,
        if_exists="append", index=False, chunksize=1000, method="multi",
    )


def _frac_head(frame, fraction: float | None):
    """Slice a frame to the first ``fraction`` of its rows (>=1), or return it whole."""
    if fraction is None:
        return frame
    return frame.head(max(1, math.ceil(frame.height * fraction)))


def _validate_file(
    path: Path, record_type: str, *, limit: int | None, fraction: float | None, engine
) -> tuple[int, int, list[str]]:
    """Validate one parquet file and append valid rows to its silver table.
    ``fraction`` takes the first N% of rows; ``limit`` caps valid rows.
    Returns (valid, rejected, reject_samples)."""
    model, table = SILVER_TARGETS[record_type]
    today = date.today().isoformat()
    valid = rejected = 0
    samples: list[str] = []
    batch: list[dict] = []
    origin = path.name
    try:
        frame = _frac_head(pl.read_parquet(path), fraction)
    except Exception as exc:  # noqa: BLE001 — a bad file must not kill the whole run
        return 0, 0, [f"{origin}: FILE READ ERROR: {type(exc).__name__}: {str(exc)[:100]}"]
    for raw in frame.iter_rows(named=True):
        if limit is not None and valid >= limit:
            break
        candidate = _clean_row(raw, file_origin=origin, download_date=today)
        try:
            obj = model.model_validate(candidate)
        except Exception as exc:  # noqa: BLE001 — dirty-row reject is expected
            rejected += 1
            if len(samples) < 3:
                samples.append(f"{origin}: {type(exc).__name__}: {str(exc)[:100]}")
            continue
        batch.append(obj.model_dump())
        valid += 1
        if len(batch) >= _FLUSH_ROWS:
            _append_batch(batch, table, engine)
            batch = []
    _append_batch(batch, table, engine)
    return valid, rejected, samples


# Worker entry point for the process pool (must be module-level / picklable).
def _validate_file_worker(
    task: tuple[str, str, str, int | None, float | None],
) -> tuple[str, str, int, int, list[str]]:
    path_str, record_type, url, limit, fraction = task
    engine = create_engine(url)
    try:
        valid, rejected, samples = _validate_file(
            Path(path_str), record_type, limit=limit, fraction=fraction, engine=engine
        )
    finally:
        engine.dispose()
    return record_type, Path(path_str).name, valid, rejected, samples


def _files_for(record_type: str, max_files: int | None) -> list[Path]:
    files = [
        f.path for f in discover_state_files("texas")
        if f.record_type == record_type and f.path.suffix == ".parquet"
    ]
    return files if max_files is None else files[:max_files]


def load_raw_type(
    record_type: str, table: str, *, max_files: int | None, limit: int | None,
    fraction: float | None, engine,
) -> tuple[int, int]:
    """Land a record type's parquet RAW (all-TEXT) into silver — no validation.
    Returns (files, rows). Single-process (these types are few files)."""
    files = _files_for(record_type, max_files)
    today = date.today().isoformat()
    frames = []
    for path in files:
        frame = _frac_head(pl.read_parquet(path), fraction)
        if limit is not None:
            frame = frame.head(limit)
        frame = frame.with_columns([
            pl.lit(path.name).alias("file_origin"),
            pl.lit(today).alias("download_date"),
        ])
        frames.append(frame)
    if not frames:
        return 0, 0
    merged = pl.concat(frames, how="diagonal")
    frame_out = _to_text_frame(merged.to_dicts())
    frame_out.to_sql(
        table, engine, schema=SILVER_SCHEMA,
        if_exists="replace", index=False, chunksize=1000, method="multi",
    )
    return len(files), len(frame_out)


# Committee master columns we keep from the FILER file (raw — no validation; the
# TECFiler validator is nested/address-required and rejects most rows, and committee
# reference data does not need row validation). filerIdent stays the raw 8-char id.
# treas* columns drive officer (TREASURER_OF) extraction in the association layer.
_FILER_COLS = [
    "filerIdent", "filerName", "filerTypeCd", "committeeStatusCd", "filerNameOrganization",
    "treasPersentTypeCd", "treasNameOrganization", "treasNameLast", "treasNameFirst",
    "treasNameSuffixCd", "treasStreetAddr1", "treasStreetCity", "treasStreetStateCd",
    "treasStreetPostalCode",
]


def load_filers(engine, *, max_files: int | None = None) -> int:
    """Land the FILER committee master into silver.tx_filers (one row per filer id).

    Populates committee name / type / status so unified_committees is enriched and
    committee_id matches the canonical zero-padded TEC filer id. Returns row count.
    """
    files = [
        f.path for f in discover_state_files("texas")
        if f.record_type == "FILER" and f.path.suffix == ".parquet"
    ]
    if max_files is not None:
        files = files[:max_files]
    frames = []
    for path in files:
        df = pl.read_parquet(path)
        keep = [c for c in _FILER_COLS if c in df.columns]
        frames.append(df.select(keep))
    if not frames:
        return 0
    merged = pl.concat(frames, how="diagonal").unique(subset=["filerIdent"], keep="first")
    # Build pandas via dicts (avoids the polars->pandas pyarrow dependency).
    pdf = pd.DataFrame(merged.to_dicts())
    pdf.columns = [c.lower() for c in pdf.columns]
    pdf = pdf.astype(object).where(pd.notnull(pdf), None)
    pdf.to_sql("tx_filers", engine, schema=SILVER_SCHEMA, if_exists="replace",
               index=False, chunksize=2000, method="multi")
    return len(pdf)


def run(
    *, max_files: int | None, limit: int | None, bootstrap: bool = True,
    workers: int = 1, fraction: float | None = None,
) -> dict:
    """Validate + land TX RCPT/EXPN into silver. ``workers > 1`` fans file
    validation out across processes (the EL bottleneck at full volume)."""
    url = bootstrap_unified() if bootstrap else spike_url()
    state_id = ensure_state(url)

    # Pre-create the empty silver tables once (workers only append). Drop the gold
    # views first so replacing silver isn't blocked by a prior build's dependents.
    from sqlalchemy import text
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS gold CASCADE"))
    for _rt, (model, table) in SILVER_TARGETS.items():
        _ensure_empty_table(engine, model, table)

    # Committee master (reference data — small, single-process).
    filer_rows = load_filers(engine, max_files=max_files)

    results = {rt: LoadResult(record_type=rt, table=SILVER_TARGETS[rt][1]) for rt in SILVER_TARGETS}
    tasks: list[tuple[str, str, str, int | None, float | None]] = []
    for rt in SILVER_TARGETS:
        files = _files_for(rt, max_files)
        results[rt].files = len(files)
        tasks.extend((str(p), rt, url, limit, fraction) for p in files)

    t0 = time.time()
    if workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for rt, name, valid, rejected, samples in pool.map(_validate_file_worker, tasks):
                r = results[rt]
                r.valid += valid
                r.rejected += rejected
                r.reject_samples = (r.reject_samples + samples)[:5]
                print(f"  done {rt} {name}: valid={valid:,} rejected={rejected:,} "
                      f"total={r.valid:,} elapsed={time.time() - t0:.0f}s", flush=True)
    else:
        for path_str, rt, _url, lim, frac in tasks:
            valid, rejected, samples = _validate_file(
                Path(path_str), rt, limit=lim, fraction=frac, engine=engine
            )
            r = results[rt]
            r.valid += valid
            r.rejected += rejected
            r.reject_samples = (r.reject_samples + samples)[:5]
            print(f"  done {rt} {Path(path_str).name}: valid={valid:,} rejected={rejected:,} "
                  f"total={r.valid:,} elapsed={time.time() - t0:.0f}s", flush=True)

    # Raw-typed landing for the remaining transaction record types (no validator).
    raw_rows: dict[str, int] = {}
    for rt, table in RAW_TARGETS.items():
        nfiles, nrows = load_raw_type(
            rt, table, max_files=max_files, limit=limit, fraction=fraction, engine=engine
        )
        raw_rows[rt] = nrows
        print(f"  raw  {rt} {table}: files={nfiles} rows={nrows:,} "
              f"elapsed={time.time() - t0:.0f}s", flush=True)
    engine.dispose()

    return {"state_id": state_id, "url": url, "results": list(results.values()),
            "filer_rows": filer_rows, "raw_rows": raw_rows, "seconds": time.time() - t0}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Land validated TX rows into the silver schema.")
    parser.add_argument("--max-files", type=int, default=2, help="parquet files per record type")
    parser.add_argument("--all-files", action="store_true", help="load every file (full volume)")
    parser.add_argument("--limit", type=int, default=None, help="cap rows per file")
    parser.add_argument("--fraction", type=float, default=None,
                        help="load this fraction (0-1) of each file's rows, e.g. 0.25 for a 25%% load")
    parser.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 1),
                        help="parallel validation processes (default: cores-1)")
    parser.add_argument("--no-bootstrap", action="store_true", help="skip DB/schema creation")
    args = parser.parse_args(argv)

    max_files = None if args.all_files else args.max_files
    summary = run(
        max_files=max_files, limit=args.limit, fraction=args.fraction,
        bootstrap=not args.no_bootstrap, workers=args.workers,
    )
    print(f"\nSilver load → {summary['url']}  (state_id={summary['state_id']}, "
          f"{summary['seconds']:.0f}s, workers={args.workers})")
    for r in summary["results"]:
        print(f"  {r.record_type:5s} {r.table:16s} files={r.files} valid={r.valid:,} rejected={r.rejected:,}")
        for s in r.reject_samples:
            print(f"        reject: {s}")
    print(f"\nNext: uv run dbt build --project-dir transform/dbt --profiles-dir transform/dbt "
          f"--vars '{{state_id: {summary['state_id']}}}'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
