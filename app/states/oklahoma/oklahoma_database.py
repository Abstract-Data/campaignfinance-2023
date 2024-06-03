from snowflake.snowpark import Session
from op import OnePasswordItem

""" Database connection notes:
- Must use psycopg2-binary for postgresql
- Must use snowflake.sqlalchemy for snowflake

"""


def create_connection():
    return Session.builder.configs(
        OnePasswordItem(
            name='snowflake_counterpoint')
        .database_params
    )


if __name__ != '__main__':
    oklahoma_snowpark_session = create_connection()
