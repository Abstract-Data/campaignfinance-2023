"""Security and secret-handling tests for ``app.op``."""

import asyncio

import pytest
from pydantic import SecretStr, ValidationError
from sqlalchemy.engine import URL

import app.op as op_module
from app.op import OnePasswordItem, OnePasswordSettings


def _build_item_with_secrets() -> OnePasswordItem:
    item = OnePasswordItem.model_construct(name="warehouse")
    item._secrets = {
        "warehouse/username": SecretStr("readonly_user"),
        "warehouse/password": SecretStr("very-secret-password"),
        "warehouse/server": SecretStr("db.internal"),
        "warehouse/port": SecretStr("5432"),
        "warehouse/database": SecretStr("campaignfinance"),
        "warehouse/schema": SecretStr("analytics"),
        "warehouse/type": SecretStr("postgresql"),
    }
    return item


def test_database_url_masks_password_in_string_views() -> None:
    item = _build_item_with_secrets()

    url = item.database_url
    assert isinstance(url, URL)

    rendered = str(url)
    assert "very-secret-password" not in rendered
    assert "***" in rendered
    assert "currentSchema=analytics" in rendered


def test_settings_forbid_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        OnePasswordSettings(
            op_service_account_token="token",
            unknown_setting="oops",
        )


def test_get_value_logs_and_raises_on_resolution_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeLogger:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def error(self, message: str) -> None:
            self.calls.append(message)

    fake_logger = _FakeLogger()

    class _Boom(RuntimeError):
        pass

    async def _raise_authenticate(**_: str) -> None:
        raise _Boom("resolve failed")

    monkeypatch.setattr(op_module, "Error", _Boom)
    monkeypatch.setattr(op_module.Client, "authenticate", _raise_authenticate)
    monkeypatch.setattr(op_module, "_get_logger", lambda: fake_logger)

    settings = OnePasswordSettings(op_service_account_token="token")

    with pytest.raises(_Boom, match="resolve failed"):
        asyncio.run(settings._get_value("my/item/password"))

    assert fake_logger.calls
    assert "my/item/password" in fake_logger.calls[0]
