"""Tests for centralized logging configuration (P2-OPS-002).

These tests pin the behavior introduced when ``app/logger.py`` is refactored
from a per-instance ``Logger`` dataclass (which builds handlers on every
construction) to a single ``logging.config.dictConfig`` setup.

Required behavior under test:

1. PaperTrail host/port are read from ``PAPERTRAIL_HOST`` / ``PAPERTRAIL_PORT``
   environment variables (not hardcoded).
2. Re-obtaining the same logger (or re-running ``configure_logging``) does not
   duplicate handlers.
3. An unreachable PaperTrail host does not stall configuration / startup.
4. The legacy ``Logger(name)`` shim preserves the public method surface
   (``info``, ``debug``, ``warning``, ``error``, ``critical``, ``exception``,
   ``silent_error``) so existing call sites do not have to change.
"""

from __future__ import annotations

import importlib
import logging
from typing import Iterator

import pytest


@pytest.fixture
def fresh_logging(monkeypatch: pytest.MonkeyPatch) -> Iterator[object]:
    """Reload ``app.logger`` with a clean root-logger / configured-flag state."""
    import app.logger as logger_mod

    project_root = logger_mod.PROJECT_LOGGER_NAME
    original_root_handlers = list(logging.getLogger(project_root).handlers)
    monkeypatch.setattr(logger_mod, "_CONFIGURED", False, raising=False)
    for handler in list(logging.getLogger(project_root).handlers):
        logging.getLogger(project_root).removeHandler(handler)

    try:
        yield logger_mod
    finally:
        for handler in list(logging.getLogger(project_root).handlers):
            logging.getLogger(project_root).removeHandler(handler)
        for handler in original_root_handlers:
            logging.getLogger(project_root).addHandler(handler)
        monkeypatch.setattr(logger_mod, "_CONFIGURED", False, raising=False)


def test_papertrail_host_and_port_come_from_env(
    fresh_logging,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``LoggingSettings`` must read PaperTrail values from the environment."""
    monkeypatch.setenv("PAPERTRAIL_HOST", "logs.example.test")
    monkeypatch.setenv("PAPERTRAIL_PORT", "5555")

    settings = fresh_logging.LoggingSettings()

    assert settings.papertrail_host == "logs.example.test"
    assert settings.papertrail_port == 5555


def test_obtaining_same_logger_twice_does_not_duplicate_handlers(
    fresh_logging,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``configure_logging`` must be idempotent: handler counts must not grow."""
    monkeypatch.setenv("PAPERTRAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("PAPERTRAIL_PORT", "0")

    project_root = fresh_logging.PROJECT_LOGGER_NAME

    fresh_logging.configure_logging()
    first_count = len(logging.getLogger(project_root).handlers)
    logger_a = fresh_logging.get_logger("test_module")

    fresh_logging.configure_logging()
    fresh_logging.configure_logging()
    logger_b = fresh_logging.get_logger("test_module")
    second_count = len(logging.getLogger(project_root).handlers)

    assert logger_a is logger_b
    assert first_count == second_count, f"handler count grew from {first_count} to {second_count}"
    handler_ids = [id(h) for h in logging.getLogger(project_root).handlers]
    assert len(set(handler_ids)) == len(
        handler_ids
    ), "duplicate handler instances attached to project root logger"


def test_unreachable_papertrail_host_does_not_stall(
    fresh_logging,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A DNS-unresolvable PaperTrail host must not raise from configuration."""
    monkeypatch.setenv("PAPERTRAIL_HOST", "this-host-definitely-does-not-exist.invalid")
    monkeypatch.setenv("PAPERTRAIL_PORT", "33096")

    fresh_logging.configure_logging()
    logger = fresh_logging.get_logger("test_unreachable")
    logger.info("smoke test after unreachable papertrail host")


def test_legacy_logger_shim_preserves_public_api(
    fresh_logging,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The thin ``Logger(name)`` shim must keep the original method surface."""
    monkeypatch.setenv("PAPERTRAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("PAPERTRAIL_PORT", "0")

    shim = fresh_logging.Logger("test_legacy_shim")

    for method_name in (
        "info",
        "debug",
        "warning",
        "error",
        "critical",
        "exception",
        "silent_error",
    ):
        assert callable(
            getattr(shim, method_name)
        ), f"shim is missing required method: {method_name}"

    shim.info("info via shim")
    shim.debug("debug via shim")
    shim.warning("warning via shim")
    shim.error("error via shim")
    shim.critical("critical via shim")
    shim.silent_error("silent error via shim")


def test_shim_does_not_rebuild_handlers_per_instance(
    fresh_logging,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Constructing the legacy ``Logger`` shim N times must not grow handlers."""
    monkeypatch.setenv("PAPERTRAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("PAPERTRAIL_PORT", "0")

    project_root = fresh_logging.PROJECT_LOGGER_NAME

    fresh_logging.Logger("test_shim_a")
    baseline = len(logging.getLogger(project_root).handlers)

    for _ in range(10):
        fresh_logging.Logger(f"test_shim_{_}")

    after = len(logging.getLogger(project_root).handlers)
    assert after == baseline, f"shim instantiation leaked handlers: {baseline} -> {after}"


def test_abc_validation_logger_is_cached(
    fresh_logging,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``StateFileValidation.logger`` must not rebuild a logger per access."""
    monkeypatch.setenv("PAPERTRAIL_HOST", "127.0.0.1")
    monkeypatch.setenv("PAPERTRAIL_PORT", "0")

    import sys

    sys.path.insert(0, str(importlib.import_module("app").__path__[0]))
    try:
        from app.abcs.abc_validation import StateFileValidation
    finally:
        sys.path.pop(0)

    from sqlmodel import SQLModel

    class _StubValidator(SQLModel):
        pass

    class _ConcreteValidation(StateFileValidation):
        pass

    instance = _ConcreteValidation(validator_to_use=_StubValidator)
    first = instance.logger
    second = instance.logger
    assert first is second, "abc_validation.StateFileValidation.logger must cache its logger"
