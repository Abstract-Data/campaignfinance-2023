#! /usr/bin/env python3
from states.texas.texas import TECFileDownloader, TECCategories, TECFileValidator
from states.texas.validators import TECExpense, TECContribution, TECFiler
from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import Base, engine, SessionLocal
from states.texas.models import TECExpenseRecord, TECFilerRecord, TECContributionRecord
from states.texas.texas_search import TECQueryBuilder
import pandas as pd
from tqdm import tqdm

# Pandas Display Options
pd.options.display.max_columns = None
pd.options.display.max_rows = None

download = TECFileDownloader()
folder = TECCategories()

# tinderholt = TECQueryBuilder().campaign('TINDERHOLT').expense().search()
# tinderholt.by_year()
# df = tinderholt.to_dataframe()
# filer_passed, filer_failed = folder.validate_category(folder.filers)
# records = folder.load(folder.expenses)
exp_passed, exp_failed = folder.validate_category(folder.expenses)
folder.update_database(folder.expenses)

# passed, failed = folder.validate_category(folder.expenses, to_db=True, update=True)

# tony_tinderholt = TECSearchQuery()

# luke_macias = TECSearchQuery()
# luke_macias_results = luke_macias.by_year()
# luke_macias.export_file(luke_macias_results.reset_index(), 'expense', 'luke macias')
#
# macias_strategy = TECSearchQuery()
# macias_strategy_results = macias_strategy.by_year().reset_index()
# macias_strategy.export_file(macias_strategy_results.reset_index(), 'expense', 'macias_strategy')
# search = TECSearchQuery()
# results = search.search()

# viewer = TECResults(results)
# viewer.print_results()
# by_year = viewer.by_year(**search.data._fields)



# Create a list of all nameOrganizations and filer names that have the same filerIdent







# TODO: Fix Filer Records to remove duplicates
#
# download.read()
#
# filers = TECValidator()
#
# psql = PostgresLoader(Base)
# psql.build(engine=engine)

# folder.load("expenses")
# filers.validate(records=folder.filers, validator=TECFiler)


def sort_uniques(records: list, field: str = "filerIdent"):
    _list = [dict(x) for x in records]
    _unique_records = {x[field]: x for x in _list}
    print(f"Duplicates: {len(_list) - len(_unique_records)}")
    return [_unique_records[x] for x in _unique_records]

# unique_filers = sort_uniques(filers.passed)
# filers.validate(records=unique_filers, validator=TECFiler, load_to_db=True)
#
#
# expenses.validate(records=folder.expenses, validator=TECExpense)
# unique_expenses = sort_uniques(expenses.passed, field="expendInfoId")
#
# expense_loader = PostgresLoader(Base)
# expense_loader.build(engine=engine)
# expense_loader.create(values=unique_expenses, table=TECExpenseRecord)
# expense_loader.load(session=SessionLocal)

# folder.generate()
#
# # records = [x for x in folder.contributions]
# # each_record = [y for x in records for y in x]
# contributions = TECValidator()
# contributions.validate(records=folder.contributions, validator=TECContribution)
# contributions.validate(records=folder.contributions, validator=TECContribution)
# contributions_loader = PostgresLoader(Base)
# contributions_loader.load(records=contributions.passed, session=SessionLocal, table=TECContributionRecord)
# contributions_loader.load(session=SessionLocal)

# with SessionLocal() as session:
#     macias = session.query("MACIAS_YEAR").all()

# expenses.validate(records=folder.expenses, validator=TECExpense)
# unique_expenses = {x.expendInfoId: x for x in expenses.passed}
# unique_expense_list = [unique_expenses[x] for x in unique_expenses]
# expenses.validate(records=unique_expense_list, validator=TECExpense)

# TODO: Load these in, add a 'filer' constraint in the 'if' clause for load()
# folder.load('contributions')
#
# contributions = TECValidator()
# contributions.validate(records=folder.contributions, validator=TECContribution)

# contributions_sql = PostgresLoader(Base)
# contributions_sql.create(values=contributions.passed, table=TECExpenseRecord)
# contributions_sql.load(session=SessionLocal)

# folder.load('filers')
