from states.texas.texas import TECFileDownloader, TECCategories, TECValidator
from states.texas.validators import TECExpense, TECContribution, TECFiler
from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import Base, engine, SessionLocal
from states.texas.models import TECExpenseRecord, TECFilerRecord, TECContributionRecord

download = TECFileDownloader()
folder = TECCategories()
folder.generate()

contributions_validate = folder.validate(folder.contributions)


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
