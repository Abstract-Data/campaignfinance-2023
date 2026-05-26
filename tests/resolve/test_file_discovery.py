"""Task 0f — file discovery tests for tmp/<state>/ directory glob."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.loaders.file_discovery import (
    FILENAME_RECORD_TYPES,
    discover_state_files,
)


@pytest.fixture()
def texas_dir(tmp_path: Path) -> Path:
    """Minimal texas-style directory covering key prefixes and _ss/_t variants."""
    names = [
        "contribs_01_20260524.parquet",
        "cont_ss_20260524.parquet",
        "cont_t_20260524.parquet",
        "cover_20260524.parquet",
        "cover_ss_20260524.parquet",
        "cover_t_20260524.parquet",
        "expend_01_20260524.parquet",
        "expn_t_20260524.parquet",
        "expn_catg_20260524.parquet",
        "notices_20260524.parquet",
        "purpose_20260524.parquet",
        "pledges_20260524.parquet",
        "pldg_ss_20260524.parquet",
        "pldg_t_20260524.parquet",
        "spacs_20260524.parquet",
        "assets_20260524.parquet",
        "unknown_prefix_20260524.parquet",
    ]
    for name in names:
        (tmp_path / name).write_bytes(b"fake")
    return tmp_path


def test_discover_state_files_returns_every_file(texas_dir: Path) -> None:
    discovered = discover_state_files("texas", base_dir=texas_dir)
    assert len(discovered) == len(list(texas_dir.glob("*.parquet")))


@pytest.mark.parametrize(
    ("filename", "expected_type"),
    [
        ("cont_ss_20260524.parquet", "RCPT"),
        ("cont_t_20260524.parquet", "RCPT"),
        ("cover_t_20260524.parquet", "CVR1"),
        ("pldg_t_20260524.parquet", "PLDG"),
        ("expn_catg_20260524.parquet", "EXCAT"),
        ("notices_20260524.parquet", "CVR2"),
        ("purpose_20260524.parquet", "CVR3"),
        ("spacs_20260524.parquet", "SPAC"),
    ],
)
def test_discover_state_files_maps_expected_record_types(
    texas_dir: Path,
    filename: str,
    expected_type: str,
) -> None:
    discovered = {
        d.path.name: d.record_type for d in discover_state_files("texas", base_dir=texas_dir)
    }
    assert discovered[filename] == expected_type


def test_discover_state_files_unknown_prefix_is_tagged_unknown(texas_dir: Path) -> None:
    discovered = {
        d.path.name: d.record_type for d in discover_state_files("texas", base_dir=texas_dir)
    }
    assert discovered["unknown_prefix_20260524.parquet"] == "UNKNOWN"


def test_filename_record_types_includes_ss_and_t_variants() -> None:
    for prefix in ("cont_ss", "cont_t", "cover_ss", "cover_t", "pldg_ss", "pldg_t", "expn_t"):
        assert prefix in FILENAME_RECORD_TYPES


def test_discover_real_texas_directory_when_present() -> None:
    """Integration smoke test against tmp/texas when data is present locally."""
    real_dir = Path("tmp/texas")
    if not real_dir.is_dir():
        pytest.skip("tmp/texas not present")

    discovered = discover_state_files("texas", base_dir=real_dir)
    names = {d.path.name for d in discovered}
    assert "cont_ss_20260524.csv" in names or any("cont_ss" in n for n in names)
    assert "cover_t_20260524.csv" in names or any("cover_t" in n for n in names)
