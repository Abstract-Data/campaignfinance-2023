"""Convert Texas TEC CSV/txt extracts to sibling parquet files."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from app.logger import Logger

logger = Logger(__name__)

_ENCODINGS = ("utf-8", "cp1252", "latin-1")
_METADATA_STEM_PREFIXES = ("CFS-Codes", "CFS-ReadMe")
_CSV_ENCODING_RETRY_ERRORS = (UnicodeDecodeError,)


@dataclass
class ConvertResult:
    converted: int
    skipped: int
    failed: list[tuple[Path, str]]

    @property
    def ok(self) -> bool:
        return not self.failed


def _is_metadata_file(path: Path) -> bool:
    return path.stem.startswith(_METADATA_STEM_PREFIXES)


def _read_delimited_file(path: Path) -> pl.DataFrame:
    read_kwargs: dict[str, object] = {
        "infer_schema_length": 0,
        "try_parse_dates": False,
    }
    if path.suffix.lower() == ".txt":
        read_kwargs["truncate_ragged_lines"] = True

    last_error: Exception | None = None
    for encoding in _ENCODINGS:
        try:
            return pl.read_csv(path, encoding=encoding, **read_kwargs)
        except _CSV_ENCODING_RETRY_ERRORS as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    msg = f"Could not read {path}"
    raise ValueError(msg)


def convert_folder(
    folder: Path,
    *,
    overwrite: bool = False,
    keep_csv: bool = True,
    on_progress: Callable[[Path], None] | None = None,
) -> ConvertResult:
    converted = 0
    skipped = 0
    failed: list[tuple[Path, str]] = []

    sources = sorted({*folder.glob("*.csv"), *folder.glob("*.txt")})

    for source_path in sources:
        if _is_metadata_file(source_path):
            skipped += 1
            if on_progress is not None:
                on_progress(source_path)
            continue

        parquet_path = source_path.with_suffix(".parquet")
        try:
            if parquet_path.exists() and not overwrite:
                skipped += 1
            else:
                frame = _read_delimited_file(source_path)
                frame.write_parquet(parquet_path)
                converted += 1
                if not keep_csv:
                    source_path.unlink()
        except Exception as exc:
            logger.error(f"Failed to convert {source_path}: {exc}")
            failed.append((source_path, str(exc)))
        if on_progress is not None:
            on_progress(source_path)

    return ConvertResult(converted=converted, skipped=skipped, failed=failed)
