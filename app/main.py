#! /usr/bin/env python3
from __future__ import annotations
from states.texas import (
    TexasCategory,
    TexasDownloader,
    texas_validators as validators,
    texas_engine as engine,
)
from states.oklahoma import OklahomaCategory, oklahoma_validators, oklahoma_snowpark_session

from sqlmodel import select, SQLModel, text, within_group, Session, any_, col, and_, or_
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from sqlalchemy.exc import SQLAlchemyError
import itertools
from joblib import Parallel, delayed
import pydantic.dataclasses as pydantic_
from pydantic import Field
from pydantic import BaseModel
from typing import List, Dict, Tuple, Iterator, Optional, Sequence, Type, Any, overload
from collections import Counter
from icecream import ic
from collections import namedtuple
from enum import Enum
import matplotlib.pyplot as plt
from pathlib import Path
from functools import singledispatch

""" Update the model order of imports. Break out each model to a 
separate script so they're created in the correct order."""
# Pandas Display Options
pd.options.display.max_columns = None
pd.options.display.max_rows = None
pd.set_option('display.float_format', '{:.2f}'.format)

""" FUNCTIONS """


@overload
def sqlmodel_todict(records: Iterator[SQLModel]) -> map:
    ...


@overload
def sqlmodel_todict(records: List[SQLModel]) -> map:
    ...


def sqlmodel_todict(records: Any) -> map:
    for record in records:
        yield record.dict()


# SQLModel.metadata.create_all(engine)

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

filers = TexasCategory("filers")
filers.read()
filers.validation.passed = filers.validation.passed_records(filers.records)
filers.validation.failed = list(filers.validation.failed_records(filers.records))

contributions = TexasCategory("contributions")
contributions.read()
contributions.validation.passed = contributions.validation.passed_records(contributions.records)
contributions.validation.failed = list(contributions.validation.failed_records(contributions.records))

expenses = TexasCategory("expenses")
expenses.read()
expenses.validation.passed = expenses.validation.passed_records(expenses.records)
expenses.validation.failed = list(expenses.validation.failed_records(expenses.records))
expenses.validation.show_errors()

errors = expenses.validation.errors.summary

errors.to_csv(Path.home() / 'Downloads' / 'errors.csv')
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
