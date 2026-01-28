"""
DataFrame Utility Functions

Provides reusable utilities for working with Polars DataFrames,
particularly for schema alignment and column operations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Set, List, Optional

import polars as pl

from app.logger import Logger

logger = Logger(__name__)


def align_columns(
    df: pl.DataFrame,
    required_columns: Set[str],
    fill_type: pl.DataType = pl.String,
) -> pl.DataFrame:
    """
    Ensure DataFrame has all required columns, adding missing ones as null.

    Args:
        df: Input DataFrame
        required_columns: Set of column names that must exist
        fill_type: Data type for missing columns (default: String)

    Returns:
        DataFrame with all required columns present
    """
    missing = required_columns - set(df.columns)
    if missing:
        logger.debug("Adding missing columns to DataFrame", extra={"missing_count": len(missing)})
        df = df.with_columns([
            pl.lit(None).cast(fill_type).alias(col)
            for col in missing
        ])
    return df


def get_all_columns_from_files(files: List[Path]) -> Set[str]:
    """
    Get union of all column names from multiple parquet files.

    Args:
        files: List of paths to parquet files

    Returns:
        Set of all unique column names across all files
    """
    all_columns: Set[str] = set()
    for file in files:
        try:
            schema = pl.read_parquet_schema(file)
            all_columns.update(schema.keys())
        except Exception as e:
            logger.warning("Error reading parquet schema", extra={"file": str(file), "error": str(e)})
            continue
    return all_columns


def get_columns_by_suffix(columns: List[str], suffix: str) -> List[str]:
    """
    Filter columns that end with a specific suffix.

    Args:
        columns: List of column names
        suffix: Suffix to filter by (e.g., 'Dt', 'Amount', 'Ident')

    Returns:
        List of column names matching the suffix
    """
    return [c for c in columns if c.endswith(suffix)]


def get_columns_by_prefix(columns: List[str], prefix: str) -> List[str]:
    """
    Filter columns that start with a specific prefix.

    Args:
        columns: List of column names
        prefix: Prefix to filter by (e.g., 'contributor', 'payee')

    Returns:
        List of column names matching the prefix
    """
    return [c for c in columns if c.startswith(prefix)]


def consolidate_parquet_files(
    files: List[Path],
    output_path: Optional[Path] = None,
    file_origin_column: str = 'file_origin',
) -> pl.DataFrame:
    """
    Consolidate multiple parquet files into a single DataFrame with schema alignment.

    Args:
        files: List of parquet file paths to consolidate
        output_path: Optional path to write the consolidated parquet file
        file_origin_column: Name of column to track source file (default: 'file_origin')

    Returns:
        Consolidated DataFrame with aligned schema
    """
    if not files:
        logger.warning("No files provided for consolidation")
        return pl.DataFrame()

    # Get all columns from all files
    all_columns = get_all_columns_from_files(files)
    all_columns.add(file_origin_column)

    # Read and align first file
    first_file = files[0]
    df = pl.read_parquet(first_file).with_columns(
        pl.lit(first_file.stem).alias(file_origin_column)
    )
    df = align_columns(df, all_columns)

    # Process remaining files
    for file in files[1:]:
        try:
            file_df = pl.read_parquet(file).with_columns(
                pl.lit(file.stem).alias(file_origin_column)
            )
            file_df = align_columns(file_df, all_columns)
            file_df = file_df.select(df.columns)  # Ensure column order matches
            df = df.vstack(file_df)
            logger.debug("Added file to consolidated DataFrame", extra={"file": file.stem})
        except Exception as e:
            logger.warning("Error processing file during consolidation", extra={"file": str(file), "error": str(e)})
            continue

    if output_path:
        df.write_parquet(output_path, compression='lz4')
        logger.info("Wrote consolidated parquet file", extra={"output": str(output_path), "row_count": len(df)})

    return df


def cast_columns_by_suffix(
    df: pl.DataFrame,
    suffix_type_map: dict[str, pl.DataType],
) -> pl.DataFrame:
    """
    Cast columns based on their suffix to specified types.

    Args:
        df: Input DataFrame
        suffix_type_map: Mapping of suffix to target data type
            e.g., {'Dt': pl.Date, 'Amount': pl.Float64, 'Ident': pl.Int64}

    Returns:
        DataFrame with columns cast to appropriate types
    """
    columns = df.columns
    cast_expressions = []

    for suffix, dtype in suffix_type_map.items():
        matching_cols = get_columns_by_suffix(columns, suffix)
        for col in matching_cols:
            cast_expressions.append(pl.col(col).cast(dtype))

    if cast_expressions:
        df = df.with_columns(cast_expressions)

    return df
