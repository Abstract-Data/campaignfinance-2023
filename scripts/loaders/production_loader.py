"""Production loader — discovers files via directory-glob and dispatches to
per-record-type ingest builders.

Usage
-----
::

    uv run python scripts/loaders/production_loader.py testing texas_sample
    uv run python scripts/loaders/production_loader.py production texas

Positional arguments:
    preset      LoaderConfig preset: development | testing | production
    state       State name (e.g. texas)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow ``uv run python scripts/loaders/production_loader.py`` from project root.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.loaders.loader_config import (
    LoaderConfig,
    StateGlobConfig,
    get_config,
    STATE_GLOB_CONFIGS,
)


def discover_and_load(
    state: str,
    config: LoaderConfig,
    *,
    dry_run: bool = False,
) -> dict[str, int]:
    """Discover all files for *state* and load them into the database.

    Parameters
    ----------
    state:
        Lower-case state name (must be a key in ``STATE_GLOB_CONFIGS``).
    config:
        Loader configuration (batch size, limits, etc.).
    dry_run:
        If ``True``, discover files but skip actual DB writes.  Returns
        ``{"discovered": N, "loaded": 0}``.

    Returns
    -------
    dict[str, int]
        Counts of discovered and loaded files.
    """
    glob_cfg = STATE_GLOB_CONFIGS.get(state)
    if glob_cfg is None:
        raise ValueError(
            f"No glob config for state {state!r}. "
            f"Available: {', '.join(STATE_GLOB_CONFIGS)}"
        )

    discovered = list(glob_cfg.discover())
    print(f"[loader] discovered {len(discovered)} file(s) for state={state!r}")

    if dry_run:
        for path, rtype in discovered:
            print(f"  {path}  (record_type={rtype!r})")
        return {"discovered": len(discovered), "loaded": 0}

    loaded = 0
    for path, record_type in discovered:
        try:
            _load_file(path, record_type, config)
            loaded += 1
        except Exception as exc:  # noqa: BLE001
            print(f"[loader] ERROR loading {path}: {exc}", file=sys.stderr)
            if not config.retry_failed:
                raise

    return {"discovered": len(discovered), "loaded": loaded}


def _load_file(
    path: Path,
    record_type: str | None,
    config: LoaderConfig,
) -> None:
    """Load a single file.  Placeholder until full ingest pipeline is wired."""
    print(f"[loader] loading {path.name} (record_type={record_type!r}) ...")
    # Full implementation wired by task-0z integration.


if __name__ == "__main__":
    preset = sys.argv[1] if len(sys.argv) > 1 else "production"
    state = sys.argv[2] if len(sys.argv) > 2 else "texas"

    cfg = get_config(preset)
    results = discover_and_load(state, cfg, dry_run=True)
    print(f"[loader] done: {results}")
