"""Task 0f — loader directory-glob discovery tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.loaders.loader_config import (
    GlobPattern,
    LoaderConfig,
    StateGlobConfig,
    get_config,
)
from scripts.loaders.production_loader import discover_and_load

# ---------------------------------------------------------------------------
# LoaderConfig / presets
# ---------------------------------------------------------------------------


def test_get_config_production_returns_loader_config() -> None:
    cfg = get_config("production")
    assert isinstance(cfg, LoaderConfig)
    assert cfg.max_records is None
    assert cfg.batch_size > 0


def test_get_config_testing_limits_records() -> None:
    cfg = get_config("testing")
    assert cfg.max_records is not None
    assert cfg.max_records > 0


def test_get_config_unknown_preset_raises() -> None:
    with pytest.raises(ValueError, match="Unknown preset"):
        get_config("unknown_preset")


# ---------------------------------------------------------------------------
# GlobPattern / StateGlobConfig discovery
# ---------------------------------------------------------------------------


@pytest.fixture()
def texas_sample_dir(tmp_path: Path) -> Path:
    """Create a temporary directory with fake texas-style parquet files."""
    texas = tmp_path / "texas"
    texas.mkdir()

    (texas / "cover_2024q1.parquet").write_bytes(b"fake")
    (texas / "contribs_2024q1.parquet").write_bytes(b"fake")
    (texas / "expenditures_2024q1.parquet").write_bytes(b"fake")
    (texas / "pledges_2024q1.parquet").write_bytes(b"fake")
    (texas / "spac_2024q1.parquet").write_bytes(b"fake")

    sub = texas / "q2"
    sub.mkdir()
    (sub / "cover_2024q2.parquet").write_bytes(b"fake")

    return texas


def test_state_glob_config_discovers_files(texas_sample_dir: Path) -> None:
    cfg = StateGlobConfig(
        state_name="texas",
        base_dir=texas_sample_dir,
        patterns=[
            GlobPattern("**/cover*.parquet", "CVR1"),
            GlobPattern("**/contribs*.parquet"),
        ],
    )

    results = list(cfg.discover())
    paths = [p for p, _ in results]
    record_types = {rt for _, rt in results}

    assert len(results) == 3  # cover root + cover sub + contribs root
    assert any("contribs" in p.name for p in paths)
    assert "CVR1" in record_types


def test_state_glob_config_no_duplicates(texas_sample_dir: Path) -> None:
    """A file matched by multiple patterns appears only once."""
    cfg = StateGlobConfig(
        state_name="texas",
        base_dir=texas_sample_dir,
        patterns=[
            GlobPattern("**/*.parquet"),
            GlobPattern("**/cover*.parquet", "CVR1"),
        ],
    )

    results = list(cfg.discover())
    paths = [p for p, _ in results]
    assert len(paths) == len(set(paths))


def test_state_glob_config_empty_dir(tmp_path: Path) -> None:
    cfg = StateGlobConfig(
        state_name="texas",
        base_dir=tmp_path,
        patterns=[GlobPattern("**/*.parquet")],
    )
    assert list(cfg.discover()) == []


# ---------------------------------------------------------------------------
# discover_and_load dry_run
# ---------------------------------------------------------------------------


def test_discover_and_load_dry_run(texas_sample_dir: Path, monkeypatch) -> None:
    """dry_run=True returns discovered count without loading."""
    from scripts.loaders import loader_config as lc

    monkeypatch.setitem(
        lc.STATE_GLOB_CONFIGS,
        "texas_test",
        StateGlobConfig(
            state_name="texas_test",
            base_dir=texas_sample_dir,
            patterns=[GlobPattern("**/*.parquet")],
        ),
    )

    cfg = get_config("testing")
    result = discover_and_load("texas_test", cfg, dry_run=True)

    assert result["loaded"] == 0
    assert result["discovered"] >= 1


def test_discover_and_load_unknown_state_raises() -> None:
    cfg = get_config("testing")
    with pytest.raises(ValueError, match="No glob config"):
        discover_and_load("nonexistent_state_xyz", cfg, dry_run=True)


def test_persist_pldg_row_rolls_back_transaction_on_pledge_failure(monkeypatch) -> None:
    """PLDG parent transaction and pledge detail share one savepoint (M-2)."""
    from sqlmodel import Session, SQLModel, create_engine, select

    from app.core.models import UnifiedTransaction
    from app.core.source_models.pledges import UnifiedPledge
    from scripts.loaders.production_loader import _persist_pldg_row

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
    )
    # The PLDG path now links/creates a UnifiedCommittee from filerIdent before
    # build_pledge runs, so the parent flush needs the full unified schema — not
    # just the two transaction tables.  Exclude state-namespaced source tables
    # (schema="texas" etc.) which SQLite cannot create.
    SQLModel.metadata.create_all(
        engine,
        tables=[t for t in SQLModel.metadata.tables.values() if t.schema is None],
    )

    raw = {
        "recordType": "PLDG",
        "filerIdent": "00012345",
        "reportInfoIdent": 12345,
        "pledgeAmount": "100.00",
        "pledgeDt": "20240110",
        "pledgeDescr": "Event pledge",
    }

    def _raise_build_pledge(*_args, **_kwargs):
        raise RuntimeError("pledge build failed")

    monkeypatch.setattr(
        "app.core.source_models.pledges_ingest.build_pledge",
        _raise_build_pledge,
    )

    with Session(engine) as session:
        with pytest.raises(RuntimeError, match="pledge build failed"):
            _persist_pldg_row(
                session,
                raw,
                state="texas",
                state_id=1,
                state_code="TX",
                file_origin_id=None,
            )

        assert session.exec(select(UnifiedTransaction)).all() == []
        assert session.exec(select(UnifiedPledge)).all() == []
