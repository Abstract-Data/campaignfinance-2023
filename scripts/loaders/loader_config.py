"""Configuration for the production data loader, including directory-glob discovery.

Task-0f adds ``GlobPattern`` and ``StateGlobConfig`` so the loader can discover
all matching files under ``tmp/<state>/`` without hardcoding individual paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


@dataclass
class LoaderConfig:
    """Runtime tunables for the production loader."""

    batch_size: int = 100
    max_records: int | None = None
    commit_frequency: int = 50
    enable_progress: bool = True
    enable_logging: bool = True
    retry_failed: bool = True
    max_retries: int = 3


@dataclass
class GlobPattern:
    """A single file-discovery rule relative to the project root.

    Attributes
    ----------
    pattern:
        Shell-style glob passed to :func:`pathlib.Path.glob` — may include
        ``**`` for recursive traversal.
    record_type:
        Optional TEC ``recordType`` hint used to route the file to the correct
        ingestion builder (e.g. ``"CVR1"``, ``"PLDG"``).  ``None`` means
        auto-detect from content.
    """

    pattern: str
    record_type: str | None = None


@dataclass
class StateGlobConfig:
    """All file-discovery rules for a single state.

    Example
    -------
    ::

        cfg = StateGlobConfig(
            state_name="texas",
            base_dir=Path("tmp/texas"),
            patterns=[
                GlobPattern("**/cover*.parquet", "CVR1"),
                GlobPattern("**/contribs*.parquet"),
                GlobPattern("**/expenditures*.parquet"),
            ],
        )
        for path, record_type in cfg.discover():
            load_file(path, record_type)
    """

    state_name: str
    base_dir: Path
    patterns: list[GlobPattern] = field(default_factory=list)

    def discover(self) -> Iterator[tuple[Path, str | None]]:
        """Yield ``(path, record_type)`` tuples for every matching file.

        Files are yielded in lexicographic order within each pattern so that
        loader runs are deterministic across invocations.
        """
        seen: set[Path] = set()
        for gp in self.patterns:
            for path in sorted(self.base_dir.glob(gp.pattern)):
                if path.is_file() and path not in seen:
                    seen.add(path)
                    yield path, gp.record_type


class LoaderPresets:
    """Named :class:`LoaderConfig` presets."""

    @staticmethod
    def development() -> LoaderConfig:
        return LoaderConfig(
            batch_size=50,
            max_records=100,
            commit_frequency=5,
        )

    @staticmethod
    def testing() -> LoaderConfig:
        return LoaderConfig(
            batch_size=100,
            max_records=1000,
            commit_frequency=10,
        )

    @staticmethod
    def production() -> LoaderConfig:
        # commit_frequency was 20 — one fsync per 20 rows, which on a real load is
        # the dominant I/O drag (measured ~74 rows/s at cf=20 vs ~111 rows/s at
        # cf=5000 on the 2026-06-14 subset run).  Large commits batch INSERTs into
        # big executemany statements; the ~111 rows/s ceiling beyond that is
        # CPU-bound per-row ORM object construction (UnifiedTransaction + persons +
        # detail + entity/address dedup), which only a COPY-based bulk path removes.
        return LoaderConfig(
            batch_size=2000,
            max_records=None,
            commit_frequency=2000,
        )


def get_config(preset: str = "production") -> LoaderConfig:
    """Return a :class:`LoaderConfig` by preset name."""
    mapping = {
        "development": LoaderPresets.development,
        "testing": LoaderPresets.testing,
        "production": LoaderPresets.production,
    }
    if preset not in mapping:
        raise ValueError(f"Unknown preset {preset!r}. Choose from: {', '.join(mapping)}")
    return mapping[preset]()


# ---------------------------------------------------------------------------
# Default per-state glob configurations
# ---------------------------------------------------------------------------


def get_texas_glob_config(base_dir: Path | None = None) -> StateGlobConfig:
    """Return the standard file-discovery config for the Texas data directory."""
    if base_dir is None:
        base_dir = Path("tmp/texas")
    return StateGlobConfig(
        state_name="texas",
        base_dir=base_dir,
        patterns=[
            GlobPattern("**/cover*.parquet", "CVR1"),
            GlobPattern("**/cover_ss*.parquet", "CVR1"),
            GlobPattern("**/cover_t*.parquet", "CVR1"),
            GlobPattern("**/contribs*.parquet"),
            GlobPattern("**/expenditures*.parquet"),
            GlobPattern("**/pledges*.parquet", "PLDG"),
            GlobPattern("**/spac*.parquet", "SPAC"),
            GlobPattern("**/cvr2*.parquet", "CVR2"),
            GlobPattern("**/cvr3*.parquet", "CVR3"),
            GlobPattern("**/excat*.parquet", "EXCAT"),
        ],
    )


STATE_GLOB_CONFIGS: dict[str, StateGlobConfig] = {
    "texas": get_texas_glob_config(),
}
