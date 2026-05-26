"""Optional end-to-end integration tests for the resolution pipeline.

These tests require prepared Texas data under ``tmp/texas`` and are excluded
from default CI via ``@pytest.mark.integration``. Run locally or via
``.github/workflows/ci-resolve-integration.yml`` (``workflow_dispatch``).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.loaders.file_discovery import discover_state_files

pytestmark = pytest.mark.integration


def test_discover_texas_state_files() -> None:
    """Smoke-check that ``tmp/texas`` contains discoverable parquet/CSV files."""
    discovered = discover_state_files("texas", base_dir=Path("tmp") / "texas")
    assert len(discovered) > 0, "Expected at least one file under tmp/texas"


@pytest.mark.integration(requires_postgres=True)
def test_resolve_run_postgres_smoke() -> None:
    """Run the full resolve pipeline against Postgres when env is configured."""
    from app.resolve.cli import main

    exit_code = main(["run", "--state", "texas"])
    assert exit_code == 0
