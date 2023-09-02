from states.texas.texas import TECFileDownloader, TECCategories, TECValidator
from states.texas.validators import TECExpenses
from db_loaders.postgres_loader import PostgresLoader
from states.texas.database import Base, engine, SessionLocal
from states.texas.models import TECExpenseRecord

download = TECFileDownloader()
folder = TECCategories()
expenses = TECValidator()
download.read()

folder.load('expenses')
expenses.validate(records=folder.expenses, validator=TECExpenses)

expenses_sql = PostgresLoader(Base)
expenses_sql.build(engine=engine)
expenses_sql.create(values=expenses.passed, table=TECExpenseRecord)
expenses_sql.load(session=SessionLocal)


