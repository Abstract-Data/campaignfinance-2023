"""Tests for Texas parquet coverage verification (task C)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.states.texas.texas_coverage import (
    REQUIRED_RECORD_TYPES,
    CoverageReport,
    verify_coverage,
)

_REQUIRED_PREFIX_FILES: dict[str, str] = {
    "RCPT": "contribs_01.parquet",
    "EXPN": "expend_01.parquet",
    "LOAN": "loans.parquet",
    "FILER": "filers.parquet",
    "CVR1": "cover.parquet",
}


def _write_parquet(path: Path, rows: int) -> None:
    pl.DataFrame({"id": list(range(rows))}).write_parquet(path)


def _seed_required_types(folder: Path, *, rows: int = 1) -> None:
    for filename in _REQUIRED_PREFIX_FILES.values():
        _write_parquet(folder / filename, rows)


@pytest.fixture
def coverage_folder(tmp_path: Path) -> Path:
    folder = tmp_path / "texas"
    folder.mkdir()
    return folder


def test_all_required_types_present_ok_true(coverage_folder: Path) -> None:
    _seed_required_types(coverage_folder, rows=2)

    report = verify_coverage(coverage_folder)

    assert isinstance(report, CoverageReport)
    assert report.ok is True
    for record_type in REQUIRED_RECORD_TYPES:
        row = next(r for r in report.rows if r.record_type == record_type)
        assert row.status == "present"
        assert row.row_count > 0
        assert row.files


def test_missing_required_type_ok_false(coverage_folder: Path) -> None:
    for record_type, filename in _REQUIRED_PREFIX_FILES.items():
        if record_type == "LOAN":
            continue
        _write_parquet(coverage_folder / filename, rows=1)

    report = verify_coverage(coverage_folder)

    assert report.ok is False
    loan_row = next(r for r in report.rows if r.record_type == "LOAN")
    assert loan_row.status == "missing"
    assert loan_row.files == []
    assert loan_row.row_count == 0


def test_zero_row_parquet_is_empty(coverage_folder: Path) -> None:
    _seed_required_types(coverage_folder, rows=1)
    _write_parquet(coverage_folder / "contribs_01.parquet", rows=0)

    report = verify_coverage(coverage_folder)

    assert report.ok is False
    rcpt_row = next(r for r in report.rows if r.record_type == "RCPT")
    assert rcpt_row.status == "empty"
    assert rcpt_row.row_count == 0


def test_missing_non_required_type_does_not_flip_ok(coverage_folder: Path) -> None:
    _seed_required_types(coverage_folder, rows=1)

    report = verify_coverage(coverage_folder)

    assert report.ok is True
    asset_row = next(r for r in report.rows if r.record_type == "ASSET")
    assert asset_row.status == "missing"
