from __future__ import annotations
import asyncio
from onepassword.lib.aarch64.op_uniffi_core import Error
from onepassword.client import Client
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, BaseModel
from pathlib import Path
from typing import Callable, Dict


def return_if_not_empty(func: Callable):
    def wrapper(*args, **kwargs):
        value = func(*args, **kwargs)
        if isinstance(value, SecretStr) and value.get_secret_value() != "":
            return value
        return None

    return wrapper


class OnePasswordSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=Path(__file__).parent / '.env', extra='ignore')
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
        except Error:
            return None

    async def refs(self, *secret_refs: str) -> asyncio.gather:
        tasks = [self._get_value(secret_ref) for secret_ref in secret_refs]
        return await asyncio.gather(*tasks)


class OnePasswordItem(BaseModel):
    name: str
    __config: OnePasswordSettings = OnePasswordSettings()

    def __init__(self, **data):
        super().__init__(**data)
        secret_refs = [
            f"{self.name}/{attr}" for attr in
            [
                "account",
                "username",
                "password",
                "server",
                "port",
                "database",
                "schema",
                "type",
                "warehouse",
                "role"
            ]
        ]
        secrets = asyncio.run(self.__config.refs(*secret_refs))
        self.__secrets = dict(zip(secret_refs, secrets))

    @property
    def account(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/account")

    @property
    def usr(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/username")

    @property
    def pwd(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/password")

    @property
    def server(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/server")

    @property
    def port(self) -> SecretStr | None:
        return self.__secrets.get(f"{self.name}/port")

    @property
    def database(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/database")

    @property
    def db_schema(self) -> SecretStr | None:
        return self.__secrets.get(f"{self.name}/schema")

    @property
    def db_type(self) -> SecretStr | None:
        return self.__secrets.get(f"{self.name}/type")

    @property
    def warehouse(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/warehouse")

    @property
    def role(self) -> SecretStr:
        return self.__secrets.get(f"{self.name}/role")

    @property
    def database_url(self) -> SecretStr | None:
        if self.db_type.get_secret_value() == "postgresql":
            _url = (
                f"postgresql://{self.usr.get_secret_value()}"
                f":{self.pwd.get_secret_value()}"
                f"@{self.server.get_secret_value()}"
                f":{self.port.get_secret_value()}"
                f"/{self.database.get_secret_value()}"
            )
            if self.db_schema:
                _url += f"?currentSchema={self.db_schema.get_secret_value()}"
            return SecretStr(_url)

    @property
    def database_params(self) -> Dict[str, str] | None:
        if self.db_type.get_secret_value() == "other":
            params = {'account': self.account.get_secret_value() if self.account else None,
                      'user': self.usr.get_secret_value() if self.usr else None,
                      'password': self.pwd.get_secret_value() if self.pwd else None,
                      'database': self.database.get_secret_value() if self.database else None,
                      'schema': self.db_schema.get_secret_value() if self.db_schema else None,
                      'warehouse': self.warehouse.get_secret_value() if self.warehouse else None,
                      'role': self.role.get_secret_value() if self.role else None
                      }

            for key in list(params.keys()):  # Create a copy of the dictionary keys
                if not params[key]:
                    del params[key]
            return params
