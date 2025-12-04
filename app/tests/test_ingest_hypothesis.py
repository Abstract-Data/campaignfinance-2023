from __future__ import annotations

import csv
import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Tuple

import sys
import importlib

sys.modules.setdefault("logger", importlib.import_module("app.logger"))
sys.modules.setdefault("states", importlib.import_module("app.states"))
sys.modules.setdefault("funcs", importlib.import_module("app.funcs"))

import polars as pl
import pytest
from hypothesis import given, settings, strategies as st, HealthCheck

from app.ingest.file_reader import FieldSpec, FieldType, GenericFileReader, SchemaDefinition
from app.states.texas import TEXAS_CONFIGURATION, TexasDownloader
from app.states.texas.texas_downloader import TECDownloader
from app.abcs.abc_state_config import StateConfig


def _build_test_schema() -> Tuple[SchemaDefinition, List[str]]:
    headers = [
        "Transaction ID",
        "Amount",
        "Transaction Date",
        "Committee Name",
        "Committee Filer ID",
    ]
    schema = SchemaDefinition(
        name="test_schema",
        fields={
            "transaction_id": FieldSpec(
                name="transaction_id",
                aliases=("transaction_id", "Transaction ID"),
                field_type=FieldType.IDENTIFIER,
            ),
            "amount": FieldSpec(
                name="amount",
                aliases=("Amount",),
                field_type=FieldType.DECIMAL,
            ),
            "transaction_date": FieldSpec(
                name="transaction_date",
                aliases=("Transaction Date",),
                field_type=FieldType.DATE,
            ),
            "committee_name": FieldSpec(
                name="committee_name",
                aliases=("Committee Name",),
                field_type=FieldType.STRING,
            ),
            "committee_filer_id": FieldSpec(
                name="committee_filer_id",
                aliases=("Committee Filer ID", "Org ID"),
                field_type=FieldType.STRING,
            ),
        },
    )
    return schema, headers


def _expected_records_from_schema(
    schema: SchemaDefinition,
    headers: List[str],
    raw_records: List[Dict[str, str]],
) -> List[Dict[str, object]]:
    mapping = schema.map_headers(headers)
    converted: List[Dict[str, object]] = []
    for record in raw_records:
        converted_record: Dict[str, object] = {}
        for header in headers:
            canonical = mapping[header]
            converted_record[canonical] = schema.fields[canonical].convert(record[header])
        converted.append(converted_record)
    return converted


record_strategy = st.fixed_dictionaries(
    {
        "Transaction ID": st.text(min_size=1, max_size=12),
        "Amount": st.decimals(
            min_value=Decimal("-100000"),
            max_value=Decimal("100000"),
            places=2,
            allow_nan=False,
            allow_infinity=False,
        ).map(lambda value: f"{value:f}"),
        "Transaction Date": st.dates(min_value=dt.date(2000, 1, 1), max_value=dt.date(2030, 12, 31)).map(
            lambda value: value.strftime("%Y-%m-%d")
        ),
        "Committee Name": st.text(min_size=1, max_size=40),
        "Committee Filer ID": st.text(min_size=1, max_size=20),
    }
)


@given(st.lists(record_strategy, min_size=1, max_size=5))
@settings(max_examples=25, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_generic_file_reader_handles_csv(tmp_path: Path, records: List[Dict[str, str]]) -> None:
    schema, headers = _build_test_schema()
    reader = GenericFileReader(schema=schema, add_metadata=True, strict=True)

    file_path = tmp_path / "sample.csv"
    with file_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records)

    expected = _expected_records_from_schema(schema, headers, records)
    output = list(reader.read_records(file_path))

    assert len(output) == len(expected)
    for produced, expect in zip(output, expected, strict=True):
        for key, value in expect.items():
            assert produced[key] == value
        assert produced["file_origin"] == file_path.stem
        assert produced["download_date"] == dt.datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d")


@given(st.lists(record_strategy, min_size=1, max_size=5))
@settings(max_examples=25, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_generic_file_reader_handles_parquet(tmp_path: Path, records: List[Dict[str, str]]) -> None:
    schema, headers = _build_test_schema()
    reader = GenericFileReader(schema=schema, add_metadata=True, strict=True)

    data_columns = {header: [record[header] for record in records] for header in headers}
    df = pl.DataFrame(data_columns)
    file_path = tmp_path / "sample.parquet"
    df.write_parquet(file_path)

    expected = _expected_records_from_schema(schema, headers, records)
    output = list(reader.read_records(file_path))

    assert len(output) == len(expected)
    for produced, expect in zip(output, expected, strict=True):
        for key, value in expect.items():
            assert produced[key] == value
        assert produced["file_origin"] == file_path.stem
        assert produced["download_date"] == dt.datetime.fromtimestamp(file_path.stat().st_mtime).strftime("%Y-%m-%d")


file_columns = ["col_a", "col_b", "col_c", "col_d"]


@st.composite
def texas_category_files(draw: st.DrawFn) -> Dict[str, List[Tuple[List[str], List[Dict[str, str]]]]]:
    categories = draw(
        st.sets(st.sampled_from(["contributions", "expenses", "filers"]), min_size=1, max_size=2)
    )
    result: Dict[str, List[Tuple[List[str], List[Dict[str, str]]]]] = {}
    for category in categories:
        file_count = draw(st.integers(min_value=1, max_value=3))
        files: List[Tuple[List[str], List[Dict[str, str]]]] = []
        for _ in range(file_count):
            columns = draw(
                st.sets(st.sampled_from(file_columns), min_size=1, max_size=len(file_columns))
            )
            column_list = list(columns)
            row_strategy = st.fixed_dictionaries({col: st.text(min_size=1, max_size=10) for col in column_list})
            rows = draw(st.lists(row_strategy, min_size=1, max_size=3))
            files.append((column_list, rows))
        result[category] = files
    return result


@given(texas_category_files())
@settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_texas_consolidate_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, category_files):
    monkeypatch.setattr(StateConfig, "TEMP_FOLDER", property(lambda self: tmp_path))
    tmp_path.mkdir(parents=True, exist_ok=True)

    # Ensure downloader is initialised with patched temp folder
    TECDownloader(config=TEXAS_CONFIGURATION)

    original_files: Dict[str, List[Path]] = {}
    expected_columns: Dict[str, set[str]] = {}

    for category, file_entries in category_files.items():
        original_files[category] = []
        expected_columns[category] = set()
        for index, (columns, rows) in enumerate(file_entries, start=1):
            data = {col: [row[col] for row in rows] for col in columns}
            if not data:
                data = {"col_a": ["placeholder"]}
                columns = ["col_a"]
            df = pl.DataFrame(data)
            file_path = tmp_path / f"{category}_{index:02d}.parquet"
            df.write_parquet(file_path)
            original_files[category].append(file_path)
            expected_columns[category].update(columns)

    if not category_files:
        pytest.skip("No category files generated")

    TECDownloader.consolidate_files()

    for files in original_files.values():
        for file_path in files:
            assert not file_path.exists(), "Original parquet files should be removed after consolidation"

    for category, files in category_files.items():
        consolidated = list(tmp_path.glob(f"{category}_*.parquet"))
        assert len(consolidated) == 1, f"Expected a single consolidated file for {category}"

        df = pl.read_parquet(consolidated[0])
        expected_cols = expected_columns[category] | {"file_origin"}
        assert expected_cols.issubset(set(df.columns)), "Consolidated file missing expected columns"

        origins = set(df["file_origin"].to_list())
        expected_origins = {
            Path(file_path).stem for file_path in original_files[category]
        }
        assert expected_origins.issubset(origins), "Consolidated data missing file_origin markers"

