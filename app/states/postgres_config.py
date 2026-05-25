"""
PostgreSQL configuration for the unified campaign finance database.

Loaded via pydantic-settings from ``POSTGRES_*`` environment variables
(see ``.env.example``).
"""

from urllib.parse import quote_plus

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from app.logger import Logger

logger = Logger(__name__)


class PostgresConfig(BaseSettings):
    """PostgreSQL connection and pool settings for SQLAlchemy."""

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = "localhost"
    port: int = 5432
    db: str = "campaign_finance"
    user: str = ""
    password: SecretStr = Field(default_factory=lambda: SecretStr(""))
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600

    @property
    def database_url(self) -> str:
        """SQLAlchemy PostgreSQL URL (password URL-encoded when present)."""
        pw = self.password.get_secret_value()
        user_enc = quote_plus(self.user)
        if pw:
            pw_enc = quote_plus(pw)
            auth = f"{user_enc}:{pw_enc}"
        else:
            auth = user_enc
        return f"postgresql://{auth}@{self.host}:{self.port}/{self.db}"

    def validate_connection(self) -> bool:
        """Return True if a short-lived connection to Postgres succeeds."""
        engine = None
        try:
            engine = create_engine(self.database_url, pool_pre_ping=True)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as exc:
            logger.warning(f"PostgreSQL connection validation failed: {exc}")
            return False
        finally:
            if engine is not None:
                engine.dispose()
