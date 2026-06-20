"""Unit tests for vectorized ingest progress helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.core.ingest_vectorized.progress import (
    IngestStop,
    family_worker_label,
    progress_enabled,
    run_with_progress,
)


def test_family_worker_label_includes_record_types() -> None:
    class FlatTxnsWorker:
        record_types = frozenset({"RCPT", "EXPN"})
        priority = 10

        def run(self, files_by_type, ctx):  # noqa: ARG002
            return {}

    label = family_worker_label(FlatTxnsWorker())
    assert label == "FlatTxns (EXPN, RCPT)"


def test_run_with_progress_disabled_runs_all_steps() -> None:
    calls: list[int] = []

    def _run(n: int) -> int:
        calls.append(n)
        return n * 2

    out = run_with_progress(
        [1, 2, 3],
        label_fn=str,
        run_fn=_run,
        title="Test",
        show_progress=False,
    )
    assert out == [2, 4, 6]
    assert calls == [1, 2, 3]


def test_progress_enabled_respects_explicit_false() -> None:
    assert progress_enabled(False) is False


def test_progress_enabled_true_forces_on_non_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr", MagicMock(isatty=lambda: False))
    assert progress_enabled(True) is True


def test_progress_enabled_none_follows_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("sys.stderr", MagicMock(isatty=lambda: False))
    assert progress_enabled(None) is False
    monkeypatch.setattr("sys.stderr", MagicMock(isatty=lambda: True))
    assert progress_enabled(None) is True


def test_run_with_progress_stops_early() -> None:
    calls: list[int] = []
    stop_after = {"n": 0}

    def should_stop() -> bool:
        stop_after["n"] += 1
        return stop_after["n"] > 1

    def _run(n: int) -> int:
        calls.append(n)
        return n

    out = run_with_progress(
        [1, 2, 3],
        label_fn=str,
        run_fn=_run,
        title="Test",
        show_progress=False,
        should_stop=should_stop,
    )
    assert out == [1]
    assert calls == [1]


def test_ingest_stop_is_exception() -> None:
    assert issubclass(IngestStop, Exception)
