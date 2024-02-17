from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from dotenv import load_dotenv
import os
from pathlib import Path

""" Database connection notes:
- Must use psycopg2-binary for postgresql
- Must use snowflake.sqlalchemy for snowflake
"""
path = Path(__file__).parent / "texas.env"
print(path)
load_dotenv(path)

LOCAL_POSTGRES_USR = os.environ["LOCAL_POSTGRES_USR"]

LOCAL_POSTGRES_PWD = os.environ["LOCAL_POSTGRES_PWD"]

LOCAL_DATABASE_URL = f"postgresql://{LOCAL_POSTGRES_USR}:{LOCAL_POSTGRES_PWD}@localhost:5432/campaignfinance"


engine = create_engine(LOCAL_DATABASE_URL, pool_size=200, echo=True)

SessionLocal: sessionmaker = sessionmaker(
    autocommit=False, autoflush=False, bind=engine
)


class Base(DeclarativeBase):
    pass
