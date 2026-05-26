"""Cadence scheduler for recurring production pipeline jobs.

Uses the Python stdlib (:mod:`threading`, :mod:`time`, :mod:`signal`) rather than
APScheduler so production can run scheduled work without an extra dependency.
For host-level scheduling, prefer system cron invoking ``cf`` directly (see
``docs/DEPLOYMENTS.md``).
"""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable

from app.logger import Logger

_logger = Logger(__name__)


class GracefulShutdown:
    """Handle SIGTERM/SIGINT by finishing the current job, then exiting 0."""

    def __init__(self) -> None:
        self._requested = False
        self._lock = threading.Lock()
        self._previous_handlers: dict[int, object] = {}

    @property
    def requested(self) -> bool:
        with self._lock:
            return self._requested

    def request(self) -> None:
        with self._lock:
            self._requested = True

    def install(self) -> None:
        """Register handlers for SIGTERM and SIGINT."""

        def _handler(signum: int, _frame: object | None) -> None:
            _logger.info(f"Shutdown signal {signum} received; finishing current work")
            self.request()

        for sig in (signal.SIGTERM, signal.SIGINT):
            self._previous_handlers[sig] = signal.signal(sig, _handler)

    def restore(self) -> None:
        for sig, handler in self._previous_handlers.items():
            signal.signal(sig, handler)
        self._previous_handlers.clear()


class CadenceScheduler:
    """Run a callable on a fixed interval until shutdown is requested."""

    def __init__(self, shutdown: GracefulShutdown | None = None) -> None:
        self._shutdown = shutdown or GracefulShutdown()

    @property
    def shutdown(self) -> GracefulShutdown:
        return self._shutdown

    def run_periodic(
        self,
        job: Callable[[], None],
        *,
        interval_seconds: float,
    ) -> int:
        """Run *job* repeatedly; return 0 after graceful shutdown."""
        if interval_seconds <= 0:
            msg = "interval_seconds must be positive"
            raise ValueError(msg)

        self._shutdown.install()
        _logger.info(f"Scheduler started (interval={interval_seconds}s)")

        try:
            while not self._shutdown.requested:
                job()
                if self._shutdown.requested:
                    break
                self._sleep_until_shutdown(interval_seconds)
        finally:
            self._shutdown.restore()

        _logger.info("Scheduler stopped gracefully")
        return 0

    def _sleep_until_shutdown(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if self._shutdown.requested:
                return
            remaining = deadline - time.monotonic()
            time.sleep(min(1.0, remaining))
