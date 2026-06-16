from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Callable

from onepassword.client import Client
from onepassword.core import core as _op_core
from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

from app.logger import Logger

# onepassword ships per-architecture native cores (lib/aarch64, lib/x86_64) and selects
# the right one via platform.machine() in onepassword.core. Reuse that resolution rather
# than hardcoding an arch path, which raised ModuleNotFoundError on x86_64 CI runners.
Error = _op_core.Error

_logger: Logger | None = None

_SECRET_ATTRS = (
    "account",
    "username",
    "password",
    "server",
    "port",
    "database",
    "schema",
    "type",
    "warehouse",
    "role",
)


def _get_logger() -> Logger:
    global _logger
    if _logger is None:
        _logger = Logger(__name__)
    return _logger


def return_if_not_empty(func: Callable):
    def wrapper(*args, **kwargs):
        value = func(*args, **kwargs)
        if isinstance(value, SecretStr) and value.get_secret_value() != "":
            return value
        return None

    return wrapper


class OnePasswordSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent / '.env', extra='forbid')
    op_service_account_token: str

    async def _get_value(self, secret_ref: str) -> SecretStr | None:
        try:
            client = await Client.authenticate(
                auth=self.op_service_account_token,
                integration_name="My 1Password Integration",
                integration_version="v1.0.0"
            )
            if secret_ref.startswith("op://Dev/"):
                secret_ref = secret_ref[5:]
            else:
                secret_ref = f"op://Dev/{secret_ref}"
            value = await client.secrets.resolve(secret_ref)
            return SecretStr(value)
        except Error as exc:
            _get_logger().error(f"Failed to resolve 1Password secret '{secret_ref}': {exc}")
            raise

    async def refs(self, *secret_refs: str) -> tuple:
        tasks = [self._get_value(secret_ref) for secret_ref in secret_refs]
        return await asyncio.gather(*tasks)


class OnePasswordItem(BaseModel):
    name: str

    def __init__(self, **data):
        super().__init__(**data)
        self._secrets: dict[str, SecretStr | None] = {}

    @classmethod
    async def create(
        cls,
        name: str,
        *,
        config: OnePasswordSettings | None = None,
    ) -> OnePasswordItem:
        """Async factory — use from async contexts."""
        instance = cls(name=name)
        instance._secrets = await instance._fetch_secrets(config)
        return instance

    @classmethod
    def create_sync(
        cls,
        name: str,
        *,
        config: OnePasswordSettings | None = None,
    ) -> OnePasswordItem:
        """Sync factory — use from CLI and other synchronous entrypoints."""
        return asyncio.run(cls.create(name, config=config))

    async def _fetch_secrets(
        self,
        config: OnePasswordSettings | None = None,
    ) -> dict[str, SecretStr | None]:
        settings = config or OnePasswordSettings()
        secret_refs = [f"{self.name}/{attr}" for attr in _SECRET_ATTRS]
        secrets = await settings.refs(*secret_refs)
        return dict(zip(secret_refs, secrets, strict=True))

    @property
    def account(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/account")

    @property
    def usr(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/username")

    @property
    def pwd(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/password")

    @property
    def server(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/server")

    @property
    def port(self) -> SecretStr | None:
        return self._secrets.get(f"{self.name}/port")

    @property
    def database(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/database")

    @property
    def db_schema(self) -> SecretStr | None:
        return self._secrets.get(f"{self.name}/schema")

    @property
    def db_type(self) -> SecretStr | None:
        return self._secrets.get(f"{self.name}/type")

    @property
    def warehouse(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/warehouse")

    @property
    def role(self) -> SecretStr:
        return self._secrets.get(f"{self.name}/role")

    @property
    def database_url(self) -> URL | None:
        if self.db_type.get_secret_value() == "postgresql":
            query = (
                {"currentSchema": self.db_schema.get_secret_value()}
                if self.db_schema
                else None
            )
            return URL.create(
                "postgresql",
                username=self.usr.get_secret_value() if self.usr else None,
                password=self.pwd.get_secret_value() if self.pwd else None,
                host=self.server.get_secret_value() if self.server else None,
                port=int(self.port.get_secret_value()) if self.port else None,
                database=self.database.get_secret_value() if self.database else None,
                query=query,
            )

    @property
    def database_params(self) -> dict[str, str] | None:
        if self.db_type.get_secret_value() == "other":
            params = {'account': self.account.get_secret_value() if self.account else None,
                      'user': self.usr.get_secret_value() if self.usr else None,
                      'password': self.pwd.get_secret_value() if self.pwd else None,
                      'database': self.database.get_secret_value() if self.database else None,
                      'schema': self.db_schema.get_secret_value() if self.db_schema else None,
                      'warehouse': self.warehouse.get_secret_value() if self.warehouse else None,
                      'role': self.role.get_secret_value() if self.role else None
                      }

            for key in list(params.keys()):
                if not params[key]:
                    del params[key]
            return params
