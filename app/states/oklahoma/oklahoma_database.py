"""Database connection notes.

- Must use psycopg2-binary for postgresql
- Must use snowflake.sqlalchemy for snowflake
"""

from __future__ import annotations

from app.op import OnePasswordItem

oklahoma_snowpark_session = None


def create_connection():
    from snowflake.snowpark import Session

    return Session.builder.configs(
        OnePasswordItem.create_sync(name='snowflake_counterpoint').database_params
    )


if __name__ == '__main__':
    oklahoma_snowpark_session = create_connection()
