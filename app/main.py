#!/usr/bin/env python3
"""
Campaign Finance Data Analysis Entry Point

This module provides the primary interface for analyzing campaign finance data
after it has been loaded into the database. For data loading operations, use
the scripts in scripts/loaders/.

For experimental analysis code, see scripts/analysis/experiments.py.

Usage:
    # As a module
    from app.main import load_texas_dataframes, analyze_donors
    dfs = load_texas_dataframes()

    # As a script
    uv run python -m app.main

Example Analyses:
    # Search for specific donors
    dfs = load_texas_dataframes()
    contribution_df = dfs['contribs']
    donor_records = contribution_df.filter(
        pl.col('contributorNameLast') == "SMITH"
    ).collect()
"""

from __future__ import annotations

from typing import Dict

import polars as pl

from app.states.texas import TexasDownloader


def load_texas_dataframes() -> Dict[str, pl.LazyFrame]:
    """
    Load Texas campaign finance data as Polars LazyFrames.

    Returns:
        Dictionary mapping category names to LazyFrames:
        - 'contribs': Contribution records
        - 'expend': Expenditure records
        - 'filers': Filer information
        - 'reports': Filing reports
        - 'travel': Travel-related records
    """
    download = TexasDownloader()
    return download.dataframes()


def search_contributions(
    dataframes: Dict[str, pl.LazyFrame],
    last_name: str,
    first_name: str | None = None,
) -> pl.DataFrame:
    """
    Search for contributions by donor name.

    Args:
        dataframes: Dictionary of LazyFrames from load_texas_dataframes()
        last_name: Donor last name to search for
        first_name: Optional first name to filter by

    Returns:
        DataFrame of matching contribution records
    """
    contribution_df = dataframes['contribs']

    query = contribution_df.filter(
        pl.col('contributorNameLast') == last_name.upper()
    )

    if first_name:
        query = query.filter(
            pl.col('contributorNameFirst').str.contains(first_name.upper())
        )

    return query.collect()


def search_expenditures(
    dataframes: Dict[str, pl.LazyFrame],
    filer_ids: list[int] | set[int],
) -> pl.DataFrame:
    """
    Search for expenditures by filer IDs.

    Args:
        dataframes: Dictionary of LazyFrames from load_texas_dataframes()
        filer_ids: List or set of filer identifiers

    Returns:
        DataFrame of matching expenditure records
    """
    expenditure_df = dataframes['expend']

    return (
        expenditure_df
        .filter(pl.col('filerIdent').is_in(filer_ids))
        .collect()
    )


def main():
    """Main entry point for interactive analysis."""
    print("Loading Texas campaign finance data...")
    dfs = load_texas_dataframes()

    print(f"Available datasets: {list(dfs.keys())}")
    print("\nUse load_texas_dataframes() to get the data for analysis.")
    print("See scripts/analysis/experiments.py for example analyses.")

    return dfs


if __name__ == "__main__":
    main()
