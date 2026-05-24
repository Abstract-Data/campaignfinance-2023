"""Verify Texas TEC parquet coverage before running the resolution pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import polars as pl

# Filename prefix → TEC record type (confirmed against tmp/texas/CFS-ReadMe.txt).
PREFIX_MAP: dict[str, str] = {
    "contribs": "RCPT",
    "cont_ss": "RCPT",
    "cont_t": "RCPT",
    "expend": "EXPN",
    "expn_t": "EXPN",
    "expn_catg": "EXCAT",
    "cover": "CVR1",
    "cover_ss": "CVR1",
    "cover_t": "CVR1",
    "notices": "CVR2",
    "purpose": "CVR3",
    "credits": "CRED",
    "debts": "DEBT",
    "loans": "LOAN",
    "pledges": "PLDG",
    "pldg_ss": "PLDG",
    "pldg_t": "PLDG",
    "travel": "TRVL",
    "assets": "ASSET",
    "cand": "CAND",
    "filers": "FILER",
    "finals": "FINL",
    "spacs": "SPAC",
}

REQUIRED_RECORD_TYPES: frozenset[str] = frozenset({"RCPT", "EXPN", "LOAN", "FILER", "CVR1"})

_ALL_RECORD_TYPES: tuple[str, ...] = tuple(sorted(set(PREFIX_MAP.values())))
_SORTED_PREFIXES: tuple[str, ...] = tuple(
    sorted(PREFIX_MAP, key=len, reverse=True),
)

CoverageStatus = Literal["present", "missing", "empty"]


@dataclass
class CoverageRow:
    record_type: str
    files: list[Path]
    row_count: int
    status: CoverageStatus


@dataclass
class CoverageReport:
    rows: list[CoverageRow]

    @property
    def ok(self) -> bool:
        return all(
            row.status == "present"
            for row in self.rows
            if row.record_type in REQUIRED_RECORD_TYPES
        )


def _match_prefix(stem: str) -> str | None:
    for prefix in _SORTED_PREFIXES:
        if stem == prefix or stem.startswith(f"{prefix}_"):
            return prefix
    return None


def _count_rows(files: list[Path]) -> int:
    return sum(pl.read_parquet(path).height for path in files)


def verify_coverage(folder: Path) -> CoverageReport:
    grouped: dict[str, list[Path]] = {record_type: [] for record_type in _ALL_RECORD_TYPES}

    for path in sorted(folder.glob("*.parquet")):
        prefix = _match_prefix(path.stem)
        if prefix is None:
            continue
        record_type = PREFIX_MAP[prefix]
        grouped[record_type].append(path)

    rows: list[CoverageRow] = []
    for record_type in _ALL_RECORD_TYPES:
        files = grouped[record_type]
        if not files:
            rows.append(
                CoverageRow(
                    record_type=record_type,
                    files=[],
                    row_count=0,
                    status="missing",
                )
            )
            continue

        row_count = _count_rows(files)
        status: CoverageStatus = "present" if row_count > 0 else "empty"
        rows.append(
            CoverageRow(
                record_type=record_type,
                files=files,
                row_count=row_count,
                status=status,
            )
        )

    return CoverageReport(rows=rows)
