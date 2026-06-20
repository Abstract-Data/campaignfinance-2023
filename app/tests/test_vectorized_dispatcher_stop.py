"""Tests for vectorized ingest dispatcher stop behavior."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest


def test_run_vectorized_stops_between_families(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from app.core.ingest_vectorized import dispatcher as disp

    fixtures = tmp_path / "texas"
    fixtures.mkdir()
    (fixtures / "filer.parquet").write_bytes(b"")

    class _WorkerA:
        record_types = frozenset({"FILER"})
        priority = 0

        def run(self, files_by_type, ctx):  # noqa: ARG002
            return {"loaded": 1}

    class _WorkerB:
        record_types = frozenset({"RCPT"})
        priority = 10

        def run(self, files_by_type, ctx):  # noqa: ARG002
            return {"loaded": 99}

    monkeypatch.setattr(disp, "FAMILY_WORKERS", [_WorkerA(), _WorkerB()])

    discovered = [
        MagicMock(record_type="FILER", path=fixtures / "filer.parquet"),
        MagicMock(record_type="RCPT", path=fixtures / "rcpt.parquet"),
    ]
    monkeypatch.setattr(
        "scripts.loaders.file_discovery.discover_state_files",
        lambda state, base_dir: discovered,
    )

    state_row = MagicMock(id=1, code="TX")
    monkeypatch.setattr(disp, "_seed", lambda session, state: state_row)
    monkeypatch.setattr(
        disp,
        "run_with_progress",
        lambda items, **kwargs: [
            kwargs["run_fn"](items[0]),
        ],
    )

    calls = {"n": 0}

    def should_stop() -> bool:
        calls["n"] += 1
        return calls["n"] > 1

    engine = MagicMock()
    session = MagicMock()
    monkeypatch.setattr("sqlmodel.Session", lambda *a, **k: session)

    counts = disp.run_vectorized(
        engine,
        fixtures,
        state="texas",
        should_stop=should_stop,
        show_progress=False,
    )

    assert counts["loaded"] == 1
    assert counts["families_run"] == 1
    assert counts.get("stopped") == 1
