"""Tests for Texas CSV/txt → parquet conversion (task B)."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from app.states.texas.texas_converter import convert_folder


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    csv_path = tmp_path / "sample.csv"
    csv_path.write_text("name,value\nAlice,1\nBob,2\n", encoding="utf-8")
    return csv_path


def test_valid_csv_converts_to_parquet(sample_csv: Path, tmp_path: Path) -> None:
    result = convert_folder(tmp_path)

    parquet_path = tmp_path / "sample.parquet"
    assert parquet_path.exists()
    assert result.converted == 1
    assert result.skipped == 0
    assert result.failed == []
    assert result.ok is True

    frame = pl.read_parquet(parquet_path)
    assert frame.height == 2


def test_overwrite_false_skips_existing(sample_csv: Path, tmp_path: Path) -> None:
    parquet_path = tmp_path / "sample.parquet"
    pl.DataFrame({"name": ["existing"], "value": [99]}).write_parquet(parquet_path)

    result = convert_folder(tmp_path, overwrite=False)

    assert result.converted == 0
    assert result.skipped == 1
    assert result.ok is True
    frame = pl.read_parquet(parquet_path)
    assert frame.height == 1
    assert frame["value"][0] == 99


def test_keep_csv_false_removes_source(sample_csv: Path, tmp_path: Path) -> None:
    result = convert_folder(tmp_path, keep_csv=False)

    assert result.converted == 1
    assert result.ok is True
    assert not sample_csv.exists()
    assert (tmp_path / "sample.parquet").exists()


def test_malformed_csv_recorded_in_failed_other_files_still_processed(
    tmp_path: Path,
) -> None:
    good_csv = tmp_path / "good.csv"
    good_csv.write_text("name,value\nAlice,1\n", encoding="utf-8")
    bad_csv = tmp_path / "bad.csv"
    bad_csv.write_text('name,value\n"unclosed quote,1\n', encoding="utf-8")

    result = convert_folder(tmp_path)

    assert result.converted == 1
    assert result.ok is False
    assert len(result.failed) == 1
    assert result.failed[0][0] == bad_csv
    assert (tmp_path / "good.parquet").exists()
    assert not (tmp_path / "bad.parquet").exists()


def test_on_progress_called_once_per_file(tmp_path: Path) -> None:
    (tmp_path / "a.csv").write_text("x\n1\n", encoding="utf-8")
    (tmp_path / "b.txt").write_text("y\n2\n", encoding="utf-8")

    seen: list[Path] = []
    convert_folder(tmp_path, on_progress=seen.append)

    assert len(seen) == 2
    assert {p.name for p in seen} == {"a.csv", "b.txt"}
