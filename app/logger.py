"""Centralized logging configuration (P2-OPS-002).

This module configures the project's logging exactly once via
:func:`logging.config.dictConfig`. All modules should obtain their logger
through :func:`get_logger` (or the legacy :class:`Logger` shim, which simply
wraps :func:`logging.getLogger`).

Why this matters
----------------
Previously, ``Logger`` was a dataclass whose ``__post_init__`` built a
``SysLogHandler`` (network socket), a ``TimedRotatingFileHandler``, and a
``StreamHandler`` on **every** instantiation, and modules constructed new
``Logger`` objects on every property access. The result: leaking sockets,
duplicate log lines, and process stalls when PaperTrail was unreachable.

This rewrite:

* Reads ``PAPERTRAIL_HOST`` / ``PAPERTRAIL_PORT`` from the environment
  (no hardcoding).
* Configures handlers exactly once on the project root logger
  (``campaignfinance``) — module loggers propagate to it.
* Treats the syslog handler as best-effort: a short socket timeout is set and
  any DNS / connection error is swallowed so startup never stalls on an
  unreachable PaperTrail host.
* Preserves the legacy ``Logger(name)`` API as a thin compatibility shim so
  existing call sites continue to work unchanged.
"""

from __future__ import annotations

import logging
import logging.config
import socket
from logging.handlers import SysLogHandler, TimedRotatingFileHandler
from pathlib import Path
from threading import Lock
from typing import ClassVar

from pydantic_settings import BaseSettings, SettingsConfigDict

# --- module-level constants ------------------------------------------------

PROJECT_LOGGER_NAME = "campaignfinance"
_LOG_DIR = Path(__file__).parent / "logs"
_LOG_FILE = _LOG_DIR / "campaign_finance.log"
_DEFAULT_FORMAT = "%(asctime)s  %(name)s  %(levelname)s: %(message)s"
_SILENT_FORMAT = "%(asctime)s  %(name)s  SILENT %(levelname)s: %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"
# Best-effort socket timeout for the syslog handler so an unreachable
# PaperTrail host cannot stall startup.
_SYSLOG_SOCKET_TIMEOUT_SEC = 2.0

# Re-entry guard for ``configure_logging`` — handlers are attached at most once
# per process. Protected by ``_CONFIG_LOCK`` for safety in multi-threaded
# importers (e.g. test runners).
_CONFIGURED: bool = False
_CONFIG_LOCK: Lock = Lock()


# --- settings --------------------------------------------------------------


class LoggingSettings(BaseSettings):
    """Logging configuration sourced from environment / ``.env``."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    papertrail_host: str = ""
    papertrail_port: int = 0


# --- helpers ---------------------------------------------------------------


def _ensure_log_dir() -> None:
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Best-effort: if we cannot create the log directory, fall back to
        # console-only logging rather than crashing the process.
        pass


def _build_syslog_handler(host: str, port: int) -> logging.Handler | None:
    """Build a ``SysLogHandler`` that will not stall on an unreachable host.

    PaperTrail's syslog endpoint historically uses UDP; we still set a short
    socket timeout and resolve the host inside a try/except so any DNS or
    connection error degrades to "no remote logging" rather than blocking
    startup.
    """
    if not host or port <= 0:
        return None

    original_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(_SYSLOG_SOCKET_TIMEOUT_SEC)
    try:
        handler = SysLogHandler(address=(host, port))
    except (OSError, socket.gaierror, socket.timeout):
        return None
    finally:
        socket.setdefaulttimeout(original_timeout)

    try:
        sock = handler.socket
    except AttributeError:
        sock = None
    if sock is not None:
        try:
            sock.settimeout(_SYSLOG_SOCKET_TIMEOUT_SEC)
        except OSError:
            pass

    handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DATEFMT))
    return handler


def _build_file_handler() -> logging.Handler | None:
    _ensure_log_dir()
    try:
        handler = TimedRotatingFileHandler(_LOG_FILE, when="D", interval=7)
    except OSError:
        return None
    handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DATEFMT))
    return handler


def _build_console_handler() -> logging.Handler:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_DEFAULT_FORMAT, datefmt=_DATEFMT))
    return handler


def configure_logging(settings: LoggingSettings | None = None) -> None:
    """Configure project logging exactly once.

    Safe to call repeatedly: subsequent calls are no-ops and never add
    duplicate handlers. Pass ``settings`` to override the env-derived
    configuration in tests.
    """
    global _CONFIGURED

    with _CONFIG_LOCK:
        if _CONFIGURED:
            return

        cfg = settings or LoggingSettings()
        project_logger = logging.getLogger(PROJECT_LOGGER_NAME)

        for existing in list(project_logger.handlers):
            project_logger.removeHandler(existing)

        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "default": {
                        "format": _DEFAULT_FORMAT,
                        "datefmt": _DATEFMT,
                    },
                    "silent": {
                        "format": _SILENT_FORMAT,
                        "datefmt": _DATEFMT,
                    },
                },
                "loggers": {
                    PROJECT_LOGGER_NAME: {
                        "level": "DEBUG",
                        "propagate": False,
                        "handlers": [],
                    },
                },
            }
        )

        project_logger.setLevel(logging.DEBUG)

        console = _build_console_handler()
        project_logger.addHandler(console)

        file_handler = _build_file_handler()
        if file_handler is not None:
            project_logger.addHandler(file_handler)

        remote = _build_syslog_handler(cfg.papertrail_host, cfg.papertrail_port)
        if remote is not None:
            project_logger.addHandler(remote)

        _CONFIGURED = True


def _qualified_name(name: str) -> str:
    """Return a name under the project root logger namespace.

    ``logging.getLogger("foo")`` would be a sibling of the project root, not a
    child — and so would not inherit the project's handlers. This helper
    ensures every module-supplied name lives under ``campaignfinance.*``.
    """
    if not name:
        return PROJECT_LOGGER_NAME
    if name == PROJECT_LOGGER_NAME:
        return name
    if name.startswith(f"{PROJECT_LOGGER_NAME}."):
        return name
    stem = Path(name).stem if ("/" in name or "\\" in name) else name
    return f"{PROJECT_LOGGER_NAME}.{stem}"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return the cached :class:`logging.Logger` for ``name``.

    The standard library guarantees that ``logging.getLogger(name)`` returns
    the same instance for the same name, so this is naturally idempotent.
    Triggers :func:`configure_logging` on first use.
    """
    configure_logging()
    return logging.getLogger(_qualified_name(name or PROJECT_LOGGER_NAME))


# --- legacy shim -----------------------------------------------------------


class Logger:
    """Backward-compatible facade that delegates to :mod:`logging`.

    Many call sites across the codebase still do::

        from app.logger import Logger
        logger = Logger(__name__)
        logger.info("...")

    This shim preserves that surface area so the broader refactor stays
    bounded to this task's scope. Internally it just wraps the cached
    :class:`logging.Logger` returned by :func:`get_logger`, so no per-instance
    handlers are ever attached.
    """

    project_name: ClassVar[str] = PROJECT_LOGGER_NAME

    def __init__(self, module_name: str) -> None:
        self.module_name = module_name
        self._logger = get_logger(module_name)
        # ``error_logger`` historically shared the same network handler as the
        # primary logger but was used for "silent" reporting. Routing through
        # the same cached logger keeps the API alive without adding handlers.
        self._error_logger = self._logger

    @property
    def logger(self) -> logging.Logger:
        return self._logger

    @property
    def error_logger(self) -> logging.Logger:
        return self._error_logger

    @property
    def logger_name(self) -> str:
        return Path(self.module_name).stem

    def info(self, message: object) -> None:
        self._logger.info(message)

    def debug(self, message: object) -> None:
        self._logger.debug(message)

    def warning(self, message: object) -> None:
        self._logger.warning(message)

    def error(self, message: object) -> None:
        self._logger.error(message)

    def critical(self, message: object) -> None:
        self._logger.critical(message)

    def exception(self, message: object) -> None:
        self._logger.exception(message)

    def silent_error(self, message: object) -> None:
        self._error_logger.error(message)
