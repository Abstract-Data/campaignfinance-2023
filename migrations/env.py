"""Alembic environment for campaignfinance.

Resolves the database URL from the same source the app uses (``PostgresConfig`` /
``unified_database``), and binds Alembic's ``target_metadata`` to the project's SQLModel
metadata AFTER importing every model module so all tables are registered. This keeps
``alembic`` and ``cf load`` pointed at one schema definition.

URL precedence: ``alembic -x dburl=...`` (tests / one-off) > ``sqlalchemy.url`` in alembic.ini
(normally unset) > ``PostgresConfig().database_url`` (the app default).
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool
from sqlmodel import SQLModel

# Import model modules for their table-registration side effects, so SQLModel.metadata is
# complete before autogenerate / create_all run. Mirrors production_loader._get_session.
import app.core.models  # noqa: F401
import app.core.source_models  # noqa: F401

config = context.config
target_metadata = SQLModel.metadata


def _database_url() -> str:
    x_args = context.get_x_argument(as_dictionary=True)
    if x_args.get("dburl"):
        return x_args["dburl"]
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return configured
    from app.states.postgres_config import PostgresConfig

    return PostgresConfig().database_url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(
        section, prefix="sqlalchemy.", poolclass=pool.NullPool
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
    connectable.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
