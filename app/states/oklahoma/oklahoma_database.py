from dotenv import load_dotenv
import os
from pathlib import Path
from typing import ClassVar, Dict
from snowflake.snowpark import Session
from sqlmodel import create_engine

""" Database connection notes:
- Must use psycopg2-binary for postgresql
- Must use snowflake.sqlalchemy for snowflake

"""


def create_connections():
    ENV_PATH = Path(__file__).parent / 'oklahoma.env'
    load_dotenv(ENV_PATH)

    # SNOWFLAKE_ORM_PARAMS: str = (
    #     'snowflake://<user>:<password>@<account_identifier>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role'
    #     '=<role_name>'.format(
    #         user=os.environ['SNOWFLAKE_COUNTERPOINT_USR'],
    #         password=os.environ['SNOWFLAKE_COUNTERPOINT_PWD'],
    #         account_identifier=os.environ['SNOWFLAKE_COUNTERPOINT_ACCOUNT'],
    #         database_name='COUNTERPOINT',
    #         schema_name='COUNTERPOINT2024',
    #         warehouse_name=os.environ['SNOWFLAKE_COUNTERPOINT_WAREHOUSE'],
    #         role_name=os.environ['SNOWFLAKE_COUNTERPOINT_ROLE']
    #     )
    # )

    SNOWFLAKE_SNOWPARK_PARAMS: Dict[str, str] = {
        "account": os.environ['SNOWFLAKE_COUNTERPOINT_ACCOUNT'],
        "user": os.environ['SNOWFLAKE_COUNTERPOINT_USR'],
        "password": os.environ['SNOWFLAKE_COUNTERPOINT_PWD'],
        "database": "COUNTERPOINT",
        "schema": "COUNTERPOINT2024",
        "warehouse": os.environ['SNOWFLAKE_COUNTERPOINT_WAREHOUSE'],
        "role": os.environ['SNOWFLAKE_COUNTERPOINT_ROLE'],
    }

    snowpark_session = Session.builder.configs(SNOWFLAKE_SNOWPARK_PARAMS)

    # snowflake_orm_engine = create_engine(SNOWFLAKE_ORM_PARAMS)
    return snowpark_session


if __name__ != '__main__':
    oklahoma_snowpark = create_connections()
