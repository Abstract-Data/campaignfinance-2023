#! /usr/bin/env python3
from states.texas.texas import TECFileDownloader, TECCategory
from states.texas.database import Base, engine, SessionLocal
# from states.texas.updated_models.finalreports import FinalReportModel
# from states.texas.updated_models.expensedata import PayeeModel, ExpenditureModel
from states.texas.updated_models.filers import FilerNameModel
# from states.texas.updated_models.contributiondata import ContributionDataModel, ContributorDetailModel
# from states.texas.texas_search import TECQueryBuilder
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt


""" Update the model order of imports. Break out each model to a 
separate script so they're created in the correct order."""
# Pandas Display Options
pd.options.display.max_columns = None
pd.options.display.max_rows = None


Base.metadata.create_all(bind=engine)

# download = TECFileDownloader()
# download.download()
filers = TECCategory("filers")
contributions = TECCategory("contributions")
expenses = TECCategory("expenses")
reports = TECCategory("reports")

filers.validate()
contributions.validate()
expenses.validate()

# filers_filer_passed = [dict(x) for x in filers.validators.filer_passed]
# filers_filer_failed = [dict(x) for x in filers.validators.filer_failed]
#
# treasurer_passed = [dict(x) for x in filers.validators.treasurer_passed]
# treasurer_failed = [dict(x) for x in filers.validators.treasurer_failed]
#
# assistant_treasurer_passed = [dict(x) for x in filers.validators.assistant_treasurer_passed]
# # assistant_treasurer_failed = [dict(x) for x in filers.validators.assistant_treasurer_failed]
#
# chair_passed = [dict(x) for x in filers.validators.chair_passed]
# chair_failed = [dict(x) for x in filers.validators.chair_failed]
#
#
# filer_name_passed = [dict(x) for x in filers.validators.filer_name_passed]
# filer_name_failed = [dict(x) for x in filers.validators.filer_name_failed]
#
# payee_passed = [dict(x) for x in expenses.validators.payee_passed]
# payee_failed = [dict(x) for x in expenses.validators.payee_failed]
#
# expenditure_passed = [dict(x) for x in expenses.validators.expenditure_passed]
# expenditure_failed = [dict(x) for x in expenses.validators.expenditure_failed]


# PostgresLoader.load(session=SessionLocal, records=filers_filer_passed, table=Base.metadata.tables['texas_filers'])

# expenses_passed = [dict(x) for x in expenses.validators.expenditure_passed]
# expenses_failed = [dict(x) for x in expenses.validators.expenditure_failed]

# contributors_passed = [dict(x) for x in contributions.validators.contributor_details_passed]
# contributors_failed = iter([dict(x) for x in contributions.validators.contributor_details_failed])
# contributions_passed = [dict(x) for x in contributions.validators.contribution_data_passed]
# contributions_failed = len([dict(x) for x in contributions.validators.contribution_data_failed])

# contributions_passed = [dict(x) for x in contributions.validators.contribution_data_passed]
# contributors_passed = [dict(x) for x in contributions.validators.contributor_details_passed]
# contributors_failed = [dict(x) for x in contributions.validators.contributor_details_passed]


# expenses = TECCategory("expenses")
# expense_records = [dict(x) for x in expenses.read()]
#
# payee_passed, payee_failed = [], []
# expenditure_passed, expenditure_failed = [], []
# for record in tqdm(expense_records):
#     try:
#         payee = Payee(**record)
#         payee_passed.append(payee)
#     except Exception as e:
#         record['error_type'] = e
#         payee_failed.append(record)
#         print(e)
#
#     try:
#         expenditure = Expenditure(**record)
#         expenditure_passed.append(expenditure)
#     except Exception as e:
#         record['error_type'] = e
#         expenditure_failed.append(record)
#         print(e)


# filers.validate()
# filers.update_database()

# expenses = TECCategory("expenses")
# expenses.validate()
# expenses.update_database()
#
# contributions = TECCategory("contributions")
# contributions.validate()
#
# blue_texas = [dict(x) for x in contributions.passed if x['filerIdent'] == 84088]
# bt_df = pd.DataFrame(blue_texas)
# bt_df['contributionYear'] = pd.to_datetime(bt_df['contributionDt']).dt.year
# bt_df['contributionNameOrg'] = bt_df['contributorNameOrganization'].fillna(
#     bt_df['contributorNameLast'] + ', ' + bt_df['contributorNameFirst']
# )
#
# bt_by_year = pd.crosstab(
#     index=bt_df['contributionNameOrg'],
#     columns=bt_df['contributionYear'],
#     values=bt_df['contributionAmt'],
#     aggfunc='sum'
# )

# Drop empty columns
# bt_by_year = bt_by_year.dropna(axis=1, how='all')

# Graph total contributions by year
# bt_by_year.sum().plot(kind='bar', title='Texas Blue Action: Contributions by Year')

# contributions.create_models()
# contributions.add_to_database()
# contributions_passed = [dict(x) for x in contributions.passed]


# tinderholt = TECQueryBuilder().campaign('TINDERHOLT').expense().search()
# tinderholt.by_year()
# df = tinderholt.to_dataframe()
# filer_passed, filer_failed = folder.validate_category(folder.filers)
# records = folder.load(folder.expenses)
# exp_passed, exp_failed, exp_files = folder.validate_category(folder.expenses)

# test = [x for x in exp_files]
# errors = folder.add_to_database(folder.expenses)
# folder.add_to_database(folder.contributions)
# filers = [x for x in folder.read(folder.expenses)]

# passed, failed = folder.run_validation(folder.expenses)
#
# passed_list = [dict(x) for x in passed]
# failed_list = [dict(x) for x in failed]


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
