#!/usr/bin/env python3
"""
Experimental Analysis Scripts for Campaign Finance Data

This module contains experimental analysis code for exploring campaign finance data.
These are work-in-progress scripts used for ad-hoc analysis and research.

Note: This code is intentionally kept as-is for reference and future development.
Some sections are commented out as they represent various analysis approaches.

Key analyses included:
- Tim Dunn / Farris Wilks donor network analysis
- Cross-filer vendor analysis
- Contribution pattern analysis
"""

from __future__ import annotations


import pandas as pd
import polars as pl

from app.states.texas import TexasDownloader


def load_texas_data():
    """Load Texas campaign finance data using the downloader."""
    download = TexasDownloader()
    return download.dataframes()


def analyze_enterprise_donors():
    """
    Analyze donations from key donors associated with "The Enterprise" network.

    This includes Tim Dunn, Farris Wilks, Don Dyer, and Doug Deason.
    """
    dfs = load_texas_data()
    contribution_df = dfs['contribs']
    expenditure_df = dfs['expend']

    # Key donor searches
    tim_dunn = (
        contribution_df
        .filter(
            pl.col('contributorNameLast') == "DUNN",
            pl.col('contributorNameFirst').str.contains("TIM")
        )
        .collect()
        .to_pandas()
    )

    farris_wilks = (
        contribution_df
        .filter(
            pl.col('contributorNameLast') == "WILKS",
            pl.col('contributorNameFirst').str.contains("FARRIS")
        )
        .collect()
        .to_pandas()
    )

    don_dyer = (
        contribution_df
        .filter(
            pl.col('contributorNameLast') == "DYER",
            pl.col('contributorNameFirst').str.contains("DON")
        )
        .collect()
        .to_pandas()
    )

    doug_deason = (
        contribution_df
        .filter(
            pl.col('contributorNameLast') == "DEASON",
            pl.col('contributorNameFirst').str.contains("DOUG")
        )
        .collect()
        .to_pandas()
    )

    # Combine donation data
    donation_list = pd.concat([tim_dunn, farris_wilks, don_dyer])
    campaign_ids_set = set(donation_list['filerIdent'].unique())

    # Get expenses for campaigns these donors supported
    donation_campaign_expenses = (
        expenditure_df
        .filter(pl.col('filerIdent').is_in(campaign_ids_set))
        .collect()
        .to_pandas()
    )

    # Create combined payee name
    donation_campaign_expenses['new_PayeeName'] = (
        donation_campaign_expenses['payeeNameOrganization']
        .fillna('')
        .where(
            donation_campaign_expenses['payeeNameOrganization'].notna(),
            (
                donation_campaign_expenses['payeeNameFirst'].fillna('') + ' ' +
                donation_campaign_expenses['payeeNameLast'].fillna('')
            ).str.strip()
        )
    )

    donation_campaign_expenses['expendAmount'] = (
        donation_campaign_expenses['expendAmount'].astype(float)
    )

    # Group by filer and payee
    expenses_groupby = (
        donation_campaign_expenses
        .groupby(['filerName', 'new_PayeeName'])
        .agg({'expendAmount': 'sum'})
        .reset_index()
    )

    return {
        'tim_dunn': tim_dunn,
        'farris_wilks': farris_wilks,
        'don_dyer': don_dyer,
        'doug_deason': doug_deason,
        'campaign_ids': campaign_ids_set,
        'expenses_by_vendor': expenses_groupby,
    }


# =============================================================================
# Legacy/Experimental Code (commented out for reference)
# =============================================================================

# The following code sections were moved from main.py and represent various
# experimental analyses. They are kept for reference and potential future use.

"""
# Wilks family donor network analysis
CONTRIB_FIRST_AND_LAST = pl.format("{} {}", pl.col('contributorNameFirst'), pl.col('contributorNameLast'))
CONTRIB_NAME_ORG = pl.coalesce(CONTRIB_FIRST_AND_LAST, pl.col('contributorNameOrganization')).alias('contributorName')

VENDOR_FIRST_AND_LAST = pl.format("{} {}", pl.col('payeeNameFirst'), pl.col('payeeNameLast'))
VENDOR_NAME_AND_ORG = pl.coalesce(VENDOR_FIRST_AND_LAST, pl.col('payeeNameOrganization')).alias('payeeName')

# Find all donors who gave to same filers as Wilks family
# wilks = df.sql(
#     '''SELECT * FROM self WHERE
#     STARTS_WITH(contributorNameLast, 'Wilks')
#     AND (contributorNameFirst LIKE '%JoAnn%' OR contributorNameFirst LIKE '%Farris%' OR contributorNameFirst LIKE '%Dan%')'''
# )

# Group by analysis
# group_by = wilks.group_by(
#         pl.col('filerIdent'),
#         pl.col('filerName'),
#         CONTRIB_NAME_ORG,
#         pl.col('contributionDt').dt.year().alias('year'),
#     ).agg(
#         pl.col('contributionAmount').cast(pl.Float64).alias('amount').sum()
#     ).collect().to_pandas()

# Cross-tabulation for donor overlap analysis
# all_donors_to_same = all_donors_matching_ids.group_by(
#     [CONTRIB_NAME_ORG, 'year', 'filerIdent',]).agg(
#         pl.col('contributionAmount').cast(pl.Float64).alias('total').sum(),
#     ).collect().to_pandas()

# df2 = (
#     pd.crosstab(
#         index=[
#             all_donors_to_same['contributorName'],
#             all_donors_to_same['filerIdent'],
#             all_donors_to_same['filerName'].astype(str),
#         ],
#         columns=all_donors_to_same['year'],
#         values=all_donors_to_same['total'],
#         aggfunc='sum',
#         margins=True,
#         margins_name='total',
#         dropna=True
#     ).reset_index()
#     .merge(count_by_contributor, on='contributorName', how='left')
#     .fillna(pd.NA)
#     .assign(num_same_as_wilks=lambda x: x['num_same_as_wilks'].round().astype(pd.Int64Dtype()))
#     .query(f'num_same_as_wilks >= {filer_ids_count / 5} and total >= {w_median}')
#     .set_index(['contributorName', 'filerIdent'])
# )
"""


# =============================================================================
# Texas data loading and processing experiments
# =============================================================================

"""
# Old loader patterns
# download = TexasDownloader()
# download.download()
# filers = TECCategory("filers")
# contributions = TECCategory("contributions")
# expenses = TECCategory("expenses")

# Category validation patterns
# filers.read()
# filers.validate()
# errors = filers.validation.show_errors()
# filers.load_to_db(filers.validation.passed, limit=100000)

# Oklahoma data experiments
# ok_expenses = OklahomaCategory('expenses')
# expense_files = list(ok_expenses.files)
# ok_expenses.read()
# expenses_passed_records = list(ok_expenses.validation.passed_records(ok_expenses.records))
"""


# =============================================================================
# Field analysis experiments
# =============================================================================

"""
# Unique field discovery across all data types
prefix_to_remove = ['lender', 'guarantor', 'payee', 'candidate', 'treas', 'chair', 'contributor', 'expend', 'assttreas', ]
unique_fields = set()
numerical_field_dict = {}
all_field_dict = {}

# Field normalization patterns
# all_field_dict_rm_prefix = set(
#     key.replace(prefix, "")
#     for v in all_field_dict.values()
#     for key in v
#     for prefix in prefix_to_remove
#     if key.startswith(prefix) and key != prefix
# )
"""


if __name__ == "__main__":
    print("Running enterprise donor analysis...")
    results = analyze_enterprise_donors()
    print(f"Found {len(results['campaign_ids'])} unique campaign filers")
    print(f"Tim Dunn donations: {len(results['tim_dunn'])}")
    print(f"Farris Wilks donations: {len(results['farris_wilks'])}")
