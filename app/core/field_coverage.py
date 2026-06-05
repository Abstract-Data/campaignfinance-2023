"""Per-state, per-record-type field-coverage catalog.

Answers, for each TEC record type in a state, the question "what does the source
carry, what do we map, and what real data are we dropping?".  The catalog is
**source-driven**: it enumerates every column in each record type's source file,
cross-references the ``UnifiedFieldLibrary`` mappings, and records whether the
column is mapped, structural, or an unmapped-but-populated gap.

Statuses
--------
- ``MAPPED``              — column has a field-library mapping (we capture it).
- ``STRUCTURAL``          — routing / identity column handled outside the field
                            library (recordType, filerIdent, …); not lost.
- ``HANDLED``             — consumed by a record-type-specific ingest builder
                            (officers via filer_ingest, SPAC via spac_ingest,
                            FINL flags) rather than the generic field library.
- ``UNMAPPED_POPULATED``  — source carries real data we have **no mapping for**:
                            a coverage gap worth reviewing.
- ``UNMAPPED_EMPTY``      — column exists but is (near-)always blank in source;
                            nothing to capture.

Re-run after any loader/field-library change: a column moving into
``UNMAPPED_POPULATED`` flags data the pipeline started dropping.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from sqlmodel import Field, Session, SQLModel, delete

from scripts.loaders.file_discovery import discover_state_files

# Columns the loader consumes by routing/identity rather than a field mapping.
_STRUCTURAL_COLUMNS: frozenset[str] = frozenset(
    {
        "recordType",
        "formTypeCd",
        "schedFormTypeCd",
        "infoOnlyFlag",
        "filerIdent",
        "filerTypeCd",
        "filerName",
        "downloadDate",
        "download_date",
    }
)

_STATE_CODES = {"texas": "TX", "oklahoma": "OK"}

# Columns consumed by a record-type-specific ingest builder (not the generic
# field library), matched by source-column prefix.  Keeps genuine officer / SPAC
# / final-report data out of the UNMAPPED_POPULATED gap list.
_HANDLED_PREFIXES: dict[str, tuple[str, ...]] = {
    # filer_ingest builds the committee, its address, and officer rows.
    "FILER": ("treas", "assttreas", "chair", "filerStreet", "filerMailing", "filerPrimary"),
    # spac_ingest builds the SPAC ↔ supported-candidate link.
    "SPAC": ("spac", "candidate", "supported", "candtreas", "spactreas", "cta"),
    # build_final_report flips UnifiedReport.is_final from these flags.
    "FINL": ("final",),
    # build_report captures the cover-sheet totals + period dates.
    "CVR1": (
        "total", "unitemized", "loanBalance", "contribsMaintained",
        "cashOnHand", "period", "filedDt",
    ),
}


def _is_handled(record_type: str, column: str) -> bool:
    return any(column.startswith(p) for p in _HANDLED_PREFIXES.get(record_type, ()))


class FieldCoverage(SQLModel, table=True):
    """One row per (state, record_type, source_column)."""

    __tablename__ = "field_coverage"

    id: int | None = Field(default=None, primary_key=True)
    state_code: str = Field(max_length=2, index=True)
    record_type: str = Field(max_length=16, index=True)
    source_column: str = Field(max_length=128)
    unified_field: str | None = Field(default=None, max_length=128)
    mapped: bool = Field(default=False)
    source_fill_pct: float = Field(default=0.0)
    status: str = Field(max_length=24, index=True)
    last_audited_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


def _fill_pct(frame: pl.DataFrame, column: str) -> float:
    """Percentage of rows where *column* is non-null and non-blank."""
    n = frame.height
    if n == 0:
        return 0.0
    series = frame[column].drop_nulls()
    if series.dtype == pl.Utf8:
        series = series.filter(series.str.strip_chars() != "")
    return round(100.0 * series.len() / n, 1)


def audit_field_coverage(
    session: Session,
    state: str = "texas",
    *,
    base_dir: Path | None = None,
    sample_rows: int = 5000,
    populated_threshold_pct: float = 1.0,
) -> int:
    """Rebuild the ``field_coverage`` rows for *state*.  Returns rows written.

    ``populated_threshold_pct`` is a **percentage on a 0–100 scale** (matching
    ``_fill_pct`` and the stored ``source_fill_pct``), NOT a 0–1 fraction.  An
    unmapped column whose sampled fill is ``>=`` this percentage is classified
    ``UNMAPPED_POPULATED`` (real data we drop); below it, ``UNMAPPED_EMPTY``.  The
    default ``1.0`` means "at least 1% of sampled rows carry a value" — a low bar
    on purpose, so the audit surfaces any unmapped column carrying real data.
    """
    from app.core.unified_field_library import field_library

    state_code = _STATE_CODES.get(state.lower(), state[:2].upper())
    mappings = {
        m.state_field: m.unified_field for m in field_library.get_state_mappings(state)
    }

    discovered = discover_state_files(state, base_dir=base_dir)
    # One representative file per record type.
    file_for_type: dict[str, Path] = {}
    for item in discovered:
        if item.record_type not in file_for_type and item.record_type != "UNKNOWN":
            file_for_type[item.record_type] = item.path

    session.exec(delete(FieldCoverage).where(FieldCoverage.state_code == state_code))

    rows: list[FieldCoverage] = []
    for record_type, path in sorted(file_for_type.items()):
        # discover_state_files yields both .parquet and .csv (parquet preferred
        # only when both exist for a type), so a CSV-only type must not hit
        # scan_parquet — it would raise on the non-parquet header.
        if path.suffix.lower() == ".csv":
            frame = pl.scan_csv(path, infer_schema_length=0).head(sample_rows).collect()
        else:
            frame = pl.scan_parquet(path).head(sample_rows).collect()
        for column in frame.columns:
            fill = _fill_pct(frame, column)
            unified = mappings.get(column)
            if unified is not None:
                status = "MAPPED"
            elif column in _STRUCTURAL_COLUMNS:
                status = "STRUCTURAL"
            elif _is_handled(record_type, column):
                status = "HANDLED"
            elif fill >= populated_threshold_pct:
                status = "UNMAPPED_POPULATED"
            else:
                status = "UNMAPPED_EMPTY"
            rows.append(
                FieldCoverage(
                    state_code=state_code,
                    record_type=record_type,
                    source_column=column,
                    unified_field=unified,
                    mapped=unified is not None,
                    source_fill_pct=fill,
                    status=status,
                )
            )

    session.add_all(rows)
    session.commit()
    return len(rows)
