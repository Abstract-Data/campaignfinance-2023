#! /usr/bin/env python3
from __future__ import annotations

import re

from states.texas import (
    TexasDownloader
    # texas_engine as engine,
)
from states.texas.validators.texas_filers import TECFilerName
# from states.oklahoma import OklahomaCategory, oklahoma_validators, oklahoma_snowpark_session

# from sqlmodel import select, SQLModel, text, within_group, Session, any_, col, and_, or_
import pandas as pd
# from tqdm import tqdm
# import matplotlib.pyplot as plt
# from sqlalchemy.exc import SQLAlchemyError
# import itertools
# from joblib import Parallel, delayed
# import pydantic.dataclasses as pydantic_
# from pydantic import Field
# from pydantic import BaseModel
# from typing import List, Dict, Tuple, Iterator, Optional, Sequence, Type, Any, overload
# from collections import Counter
# from icecream import ic
# from collections import namedtuple
# from enum import Enum
# import matplotlib.pyplot as plt
from pathlib import Path
import polars as pl
from contextlib import contextmanager
# from functools import singledispatch

""" Update the model order of imports. Break out each model to a 
separate script so they're created in the correct order."""
# Pandas Display Options
# pd.options.display.max_columns = None
# pd.options.display.max_rows = None
# pd.set_option('display.float_format', '{:.2f}'.format)

""" FUNCTIONS """


# @overload
# def sqlmodel_todict(records: Iterator[SQLModel]) -> map:
#     ...


# @overload
# def sqlmodel_todict(records: List[SQLModel]) -> map:
#     ...


# def sqlmodel_todict(records: Any) -> map:
#     for record in records:
#         yield record.dict()


# SQLModel.metadata.create_all(engine)
download = TexasDownloader()
download.download()
dfs = download.dataframes()


df = dfs['contribs']
cols = df.collect_schema().names()
date_cols = [x for x in cols if x.endswith('Dt')]
identity_cols = [x for x in cols if x.endswith('Ident')]
amount_cols = [x for x in cols if x.endswith('Amount')]
df = df.with_columns(
    [pl.col(col).str.strptime(pl.Date, '%Y%m%d', strict=False).alias(col) for col in date_cols]
    + [pl.col(col).cast(pl.Int32).alias(col) for col in identity_cols]
    + [pl.col(col).cast(pl.Float64).alias(col) for col in amount_cols],
)

wilks = df.sql(
    """SELECT * FROM self WHERE
    STARTS_WITH(contributorNameLast, 'Wilks')
    AND (contributorNameFirst LIKE '%JoAnn%' OR contributorNameFirst LIKE '%Farris%' OR contributorNameFirst LIKE '%Dan%')"""
)
w_ = wilks.select(pl.col('contributionAmount')).collect().to_series()
w_min, w_max, w_median = w_.min(), w_.max(), w_.median()

CONTRIB_FIRST_AND_LAST = pl.format("{} {}", pl.col('contributorNameFirst'), pl.col('contributorNameLast'))
CONTRIB_NAME_ORG = pl.coalesce(CONTRIB_FIRST_AND_LAST, pl.col('contributorNameOrganization')).alias('contributorName')

VENDOR_FIRST_AND_LAST = pl.format("{} {}", pl.col('payeeNameFirst'), pl.col('payeeNameLast'))
VENDOR_NAME_AND_ORG = pl.coalesce(VENDOR_FIRST_AND_LAST, pl.col('payeeNameOrganization')).alias('payeeName')

group_by = wilks.group_by(
        pl.col('filerIdent'),
        pl.col('filerName'),
        CONTRIB_NAME_ORG,
        pl.col('contributionDt').dt.year().alias('year'),
    ).agg(
        pl.col('contributionAmount').cast(pl.Float64).alias('amount').sum()
    ).collect().to_pandas()

unique_filers = group_by[['filerIdent', 'filerName']].drop_duplicates(subset='filerIdent')
filer_ids = unique_filers['filerIdent'].unique()
filer_ids_count = len(filer_ids)

all_donors_matching_ids = (df.filter(
    pl.col('filerIdent')
    .is_in(filer_ids))
    .with_columns(
        pl.col('contributionDt').dt.year().alias('year')
    )
)

count_by_contributor = all_donors_matching_ids.group_by(
    CONTRIB_NAME_ORG,
).agg(
    pl.col('filerIdent').n_unique().alias('num_same_as_wilks'),
).collect().to_pandas()

all_donors_to_same = all_donors_matching_ids.group_by(
    [CONTRIB_NAME_ORG, 'year', 'filerIdent',]).agg(
        pl.col('contributionAmount').cast(pl.Float64).alias('total').sum(),
    ).collect().to_pandas()

all_donors_to_same = unique_filers.merge(all_donors_to_same, on='filerIdent', how='left')
df1 = all_donors_to_same.groupby(
    ['contributorName', 'filerIdent', 'year']
).agg({
    'filerName': 'first',
    'total': 'sum'
}).reset_index()

df2 = (
    pd.crosstab(
        index=[
            all_donors_to_same['contributorName'],
            all_donors_to_same['filerIdent'],
            all_donors_to_same['filerName'].astype(str),
        ],
        columns=all_donors_to_same['year'],
        values=all_donors_to_same['total'],
        aggfunc='sum',
        margins=True,
        margins_name='total',
        dropna=True
    ).reset_index()
    .merge(count_by_contributor, on='contributorName', how='left')
    .fillna(pd.NA)
    .assign(num_same_as_wilks=lambda x: x['num_same_as_wilks'].round().astype(pd.Int64Dtype()))
    .query(f'num_same_as_wilks >= {filer_ids_count / 5} and total >= {w_median}')
    .set_index(['contributorName', 'filerIdent'])
)

exdf = dfs['expend']

ex_cols = exdf.collect_schema().names()
ex_date_cols = [x for x in ex_cols if x.endswith('Dt')]
ex_identity_cols = [x for x in ex_cols if x.endswith('Ident')]
ex_amount_cols = [x for x in ex_cols if x.endswith('Amount')]
exdf = exdf.with_columns(
    [pl.col(col).str.strptime(pl.Date, '%Y%m%d', strict=False).alias(col) for col in ex_date_cols]
    + [pl.col(col).cast(pl.Int32).alias(col) for col in ex_identity_cols]
    + [pl.col(col).cast(pl.Float64).alias(col) for col in ex_amount_cols]
)


exdf = (exdf
        .filter(
            pl.col('filerIdent')
            .is_in(filer_ids))
        .with_columns(
            pl.col('expendDt').dt.year().alias('year'))
)
categories = (exdf
              .select(
                pl.col('expendCatCd')
                .unique()
                .alias('expenditure_categories'))
              .collect())

count_by_vendor = exdf.group_by(
    VENDOR_NAME_AND_ORG,
).agg(
    pl.col('filerIdent').n_unique().alias('num_same_as_wilks'),
).collect().to_pandas()

all_vendors_to_same = exdf.group_by([
    VENDOR_NAME_AND_ORG, 'filerIdent', 'year']).agg(
    pl.col('expendAmount').cast(pl.Float64).sum().round().alias('total'),
    pl.col('expendCatCd').unique().alias('expenditure_categories').cast(pl.String),
).collect().to_pandas()
all_vendors_to_same['year'] = all_vendors_to_same['year'].astype(pd.Int64Dtype())

merge_filer_data = unique_filers.merge(all_vendors_to_same, on='filerIdent', how='left')

ex_ct = (
          pd.crosstab(
              index=[
                  merge_filer_data['payeeName'],
                  merge_filer_data['filerIdent'],
                  merge_filer_data['filerName'].astype(str),
              ],
              columns=merge_filer_data['year'],
              values=merge_filer_data['total'],
              aggfunc='sum',
              margins=True,
              margins_name='total',
              dropna=True
          ).reset_index()
    .merge(count_by_vendor, on='payeeName', how='left')
    .fillna(pd.NA)
    .assign(num_same_as_wilks=lambda x: x['num_same_as_wilks'].round().astype(pd.Int64Dtype()))
    .query(f'num_same_as_wilks >= {filer_ids_count / 5} and total >= {w_median}')
    .set_index(['payeeName'])
    .sort_values(by='total', ascending=False)
)


vendor_total_ct = (
    pd.crosstab(
        index=merge_filer_data['payeeName'],
        columns=merge_filer_data['year'],
        values=merge_filer_data['total'],
        aggfunc='sum',
        margins=True,
        margins_name='total',
        dropna=True
    ).reset_index()
    .merge(count_by_vendor, on='payeeName', how='left')
    .fillna(pd.NA)
    .assign(num_same_as_wilks=lambda x: x['num_same_as_wilks'].round().astype(pd.Int64Dtype()))
    .query(f'num_same_as_wilks >= {filer_ids_count / 5} and total >= {w_median}')
    .set_index(['payeeName'])
    .sort_values(by=['total', 'payeeName'], ascending=False)
)
vendor_total_ct.to_csv(Path.home() / 'Downloads' / 'vendor_totals.csv')
ex_ct.to_csv(Path.home() / 'Downloads' / 'vendor_totals_by_filer.csv')
same_as_wilks_vendors = all_vendors_to_same.filter(
    pl.col('num_same_as_wilks') >= (filer_ids_count / 5))


top_donors = df.with_columns(
    CONTRIB_NAME_ORG,
    pl.col('contributionDt').dt.year().alias('year').cast(pl.String)
).select(
    CONTRIB_NAME_ORG, pl.col('filerIdent'), pl.col('filerName'), pl.col('contributionAmount'), 'year').collect()

top_donors_group = top_donors.group_by([
    'contributorName', 'filerIdent', 'filerName', 'year']).agg(
    pl.col('contributionAmount').sum().round().alias('total')
)

top_ct_by_filer = pd.crosstab(
    index=[
        top_donors_group['contributorName'],
        top_donors_group['filerName']],
    columns=top_donors_group['year'],
    values=top_donors_group['total'],
    aggfunc='sum',
    margins=True,
    margins_name='total',
).query(f'total >= {w_median}')
top_ct_total = pd.crosstab(
    index=top_donors_group['contributorName'],
    columns=top_donors_group['year'],
    values=top_donors_group['total'],
    aggfunc='sum',
    margins=True,
    margins_name='total',
).query(f'total >= 3e6')

# all_donors_to_same = dfs['contributions'].sql(
#     f"""
#     SELECT t1.*
#     FROM self t1
#     INNER JOIN (
#         SELECT filerIdent, contributorNameLast, contributorNameFirst
#         FROM self
#         WHERE filerIdent IN {tuple(filer_ids)}
#         GROUP BY filerIdent, contributorNameLast, contributorNameFirst
#         HAVING COUNT(DISTINCT filerIdent) = {filer_ids_count}
#     ) t2
#     ON t1.contributorNameLast = t2.contributorNameLast
#     AND t1.contributorNameFirst = t2.contributorNameFirst
#     AND t1.filerIdent = t2.filerIdent
#     WHERE t1.filerIdent IN {tuple(filer_ids)}
#     """
# ).collect()
# def categorize_data(_data = data):
#     def filter_by_origin(origin_subset):
#         return (item for item in _data if item['file_origin'].startswith(origin_subset))
#
#     _filers = filter_by_origin('filer')
#     _reports = filter_by_origin('final')
#     _contributions = filter_by_origin('contrib')
#     _expenses = filter_by_origin('expend')
#     _travel = filter_by_origin('travel')
#     _candidates = filter_by_origin('cand')
#
#     return _filers, _reports, _contributions, _expenses, _travel, _candidates
#
# filers, reports, contributions, expenses, travel, candidates = categorize_data()
#
# filer_test = next(filers)
# report_test = next(reports)
# contribution_test = next(contributions)
# expense_test = next(expenses)
# travel_test = next(travel)
# candidate_test = next(candidates)

# filers = (x['filerIdent'] for x in data if 'filers' in x['file_origin'])
# reports = (x['reportInfoIdent'] for x in data if 'reports' in x['file_origin'])
# contributions = (x for x in data.contributions if x['filerIdent'] in reports and x['filderIdent'] == test.filerIdent)
# expenses = (x for x in data.expenses if x['filerIdent'] in reports and x['filderIdent'] == test.filerIdent)

# contrib = next(data.contributions)
# filers = next(data.filers)
# reports = next(data.reports)
# expenses = next(data.expenses)
# travel = next(data.travel)
# candidates = next(data.candidates)
# debts = next(data.debts)
#
# fields = [data.travel,
#           data.contributions,
#           data.expenses,
#           data.filers,
#           data.reports,
#           data.candidates,
#           data.debts
#           ]
# prefix_to_remove = ['lender', 'guarantor', 'payee', 'candidate', 'treas', 'chair', 'contributor', 'expend', 'assttreas', ]
# unique_fields = set()
# numerical_field_dict = {}
# all_field_dict = {}
# for field in fields:
#     if field.DATA:
#         record = next(field.DATA)
#         for key in record.keys():
#             unique_fields.add(key)
#             all_field_dict.setdefault(record['file_origin'], []).append(key)
#             if key[-1].isdigit():
#                 numerical_field_dict.setdefault(record['file_origin'], []).append(key)
#
# all_field_dict_rm_prefix = set(
#     key.replace(prefix, "")
#     for v in all_field_dict.values()
#     for key in v
#     for prefix in prefix_to_remove
#     if key.startswith(prefix) and key != prefix
# )
#
# all_field_dict_after_prefix_removed = {}
# for file, fields in all_field_dict.items():
#     remove_pfx = [x.replace(prefix, "") for x in fields for prefix in prefix_to_remove if x.startswith(prefix) and x != prefix]
#     all_field_dict_after_prefix_removed[file] = remove_pfx
#
# common_fields = set.intersection(*[set(v) for v in all_field_dict_after_prefix_removed.values()])
# unique_fields_rm_prefix = set([x.replace(prefix, "") for x in unique_fields for prefix in prefix_to_remove if x.startswith(prefix) and x != prefix])
# unique_fields_wo_prefix = set([x for x in unique_fields if not any([x.startswith(prefix) for prefix in prefix_to_remove])])
# unique_fields_reduced = unique_fields_rm_prefix.union(unique_fields_wo_prefix)
#
# flags = set([key for v in all_field_dict.values() for key in v if 'Flag' in key])
# set_of_fields = list(next(x) for x in data_generators)
# texas = TexasCategory('filers')
# texas.validate()
# passed = list(texas.validation.passed_records(texas.records))
""" 
TEXAS CAMPAIGN FINANCE DATA LOADER


# download = TexasDownloader()
# download.download()
# filers = TECCategory("filers")
# contributions = TECCategory("contributions")
# expenses = TECCategory("expenses")
# reports = TECCategory("reports")
# travel = TECCategory("travel")
# candidates = TECCategory("candidates")
# debt = TECCategory("debt")


# filers.read()
# filers.validate()
# errors = filers.validation.show_errors()
# filers.load_to_db(filers.validation.passed, limit=100000)


# expenses.read()
# expenses.validate()
# expenses.validation.show_errors()
# expenses_passed = [dict(x) for x in expenses.validation.passed]
# expenses_failed = [dict(x) for x in expenses.validation.failed]
# expenses.load_to_db(expenses.validation.passed, limit=250000)

# records = contributions.read()
# record1 = next(records)

filer_records = filers.read()
report_records = reports.read()


# candidates.records = candidates.read()
# candidates.validate()
# candidates_passed = list(candidates.validation.passed)

# passed = list(reports.validation.passed_records(report_records))
# failed = list(reports.validation.failed_records(report_records))
# f1 = next(failed)
def upsert_records(category: TECCategory, _engine=engine):
    _passed_records = None
    if not category.validation.passed:
        try:
            category.validate()
        except Exception as e:
            ic(e)
            print("Attempting to read records")
            category.records = category.read()
            category.validate()

    _passed_records = category.validation.passed
    with Session(_engine) as session:
        to_load = []
        while True:
            _slice = itertools.islice(_passed_records, 5000)
            if not _slice:
                break
            passed = list(_slice)
            if not passed:
                break
            for _rec in passed:
                does_exist = session.exec(select(category.validation.validator_used).where(
                    category.validation.validator_used.id == _rec.id)).all()
                if len(does_exist) > 0:
                    break
                to_load.append(_rec)
            if len(to_load) == 100000:
                ic("Adding records")
                session.add_all(to_load)
                session.commit()
        if len(to_load) > 0:
            ic("Final record additions")
            session.add_all(to_load)
            session.commit()
            
"""

# filers = TexasCategory("filers")
# filers.read()
# filers.validation.passed = filers.validation.passed_records(filers.records)
# filers.validation.failed = list(filers.validation.failed_records(filers.records))

# contributions = TexasCategory("contributions")
# contributions.read()
# contributions.validation.passed = contributions.validation.passed_records(contributions.records)
# contributions.validation.failed = list(contributions.validation.failed_records(contributions.records))

# expenses = TexasCategory("expenses")
# expenses.read()
# expenses.validation.passed = expenses.validation.passed_records(expenses.records)
# expenses.validation.failed = list(expenses.validation.failed_records(expenses.records))
# expenses.validation.show_errors()

# errors = expenses.validation.errors.summary

# errors.to_csv(Path.home() / 'Downloads' / 'errors.csv')
# reports = TexasCategory("reports")
# travel = TECCategory("travel")
# candidates = TECCategory("candidates")
# debt = TECCategory("debt")

# ok_expenses = OklahomaCategory('expenses')
# expense_files = list(ok_expenses.files)
# ok_expenses.read()
# expenses_passed_records = list(ok_expenses.validation.passed_records(ok_expenses.records))
# expenses_failed_records = list(ok_expenses.validation.failed_records(ok_expenses.records))

# ok_contributions = OklahomaCategory('contributions')
# ok_contributions.records = ok_contributions.read()
# contributions_passed_records = list(ok_contributions.validation.passed_records(ok_contributions.records))
# contributions_failed_records = list(ok_contributions.validation.failed_records(ok_contributions.records))
#
# ok_lobby = OklahomaCategory('lobby')
# ok_lobby.records = ok_lobby.read()
# lobby_passed_records = list(ok_lobby.validation.passed_records(ok_lobby.records))
# lobby_failed_records = list(ok_lobby.validation.failed_records(ok_lobby.records))
# lobby_errors = ok_lobby.validation.show_errors()
#
# session = oklahoma_snowpark.create()
#
# expenses = list(sqlmodel_todict(expenses_passed_records))
# contributions = list(sqlmodel_todict(contributions_passed_records))
# lobby = list(sqlmodel_todict(lobby_passed_records))
#
# expense_df = session.create_dataframe(expenses).write.mode('overwrite').save_as_table('CF_EXPENSES')
# contribution_df = session.create_dataframe(contributions).write.mode('overwrite').save_as_table('CF_CONTRIBUTIONS')
# lobby_df = session.create_dataframe(lobby).write.mode('overwrite').save_as_table('CF_LOBBY')
