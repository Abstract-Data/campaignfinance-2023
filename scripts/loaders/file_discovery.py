"""Discover state data files and resolve TEC record types from filename prefixes.

Task 0f — globs ``tmp/<state>/`` for ``*.parquet`` and ``*.csv``, mapping each
file to a TEC record type via :data:`FILENAME_RECORD_TYPES`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.logger import Logger

logger = Logger(__name__)

# Repo root (campaignfinance/), not the process cwd — cf load must work from app/.
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _resolve_base_dir(base_dir: Path) -> Path:
    """Resolve *base_dir* against the repo root when it is relative."""
    path = base_dir.expanduser()
    if path.is_absolute():
        return path.resolve()
    return (_REPO_ROOT / path).resolve()

# Filename prefix → TEC record type (confirmed against tmp/texas/CFS-ReadMe.txt).
FILENAME_RECORD_TYPES: dict[str, str] = {
    "contribs": "RCPT",
    "cont_ss": "RCPT",
    "cont_t": "RCPT",
    "expend": "EXPN",
    "expn_t": "EXPN",
    "expn_catg": "EXCAT",
    "cover": "CVR1",
    "cover_ss": "CVR1",
    "cover_t": "CVR1",
    "notices": "CVR2",
    "purpose": "CVR3",
    "credits": "CRED",
    "debts": "DEBT",
    "loans": "LOAN",
    "pledges": "PLDG",
    "pldg_ss": "PLDG",
    "pldg_t": "PLDG",
    "travel": "TRVL",
    "assets": "ASSET",
    "cand": "CAND",
    "filers": "FILER",
    "final": "FINL",
    "finals": "FINL",
    "spacs": "SPAC",
}

_SORTED_PREFIXES: tuple[str, ...] = tuple(
    sorted(FILENAME_RECORD_TYPES, key=len, reverse=True),
)


@dataclass(frozen=True)
class DiscoveredFile:
    """A data file discovered under a state directory."""

    path: Path
    record_type: str


def _match_prefix(stem: str) -> str | None:
    for prefix in _SORTED_PREFIXES:
        if stem == prefix or stem.startswith(f"{prefix}_"):
            return prefix
    return None


def _resolve_record_type(path: Path) -> str:
    prefix = _match_prefix(path.stem)
    if prefix is None:
        logger.warning(f"Unknown filename prefix for {path.name}; tagging as UNKNOWN")
        return "UNKNOWN"
    return FILENAME_RECORD_TYPES[prefix]


def discover_state_files(
    state: str,
    *,
    base_dir: Path | None = None,
) -> list[DiscoveredFile]:
    """Glob ``tmp/<state>/`` for parquet and CSV files.

    Returns one :class:`DiscoveredFile` per matching file, sorted by path for
    deterministic loader runs. Unknown filename prefixes are included with
    ``record_type="UNKNOWN"`` and logged — never silently dropped.
    """
    if base_dir is None:
        base_dir = _REPO_ROOT / "tmp" / state
    else:
        base_dir = _resolve_base_dir(base_dir)

    if not base_dir.is_dir():
        return []

    by_stem: dict[str, DiscoveredFile] = {}

    for pattern in ("*.parquet", "*.csv"):
        for path in sorted(base_dir.glob(pattern)):
            if not path.is_file():
                continue
            stem = path.stem
            candidate = DiscoveredFile(path=path, record_type=_resolve_record_type(path))
            existing = by_stem.get(stem)
            if existing is None:
                by_stem[stem] = candidate
                continue
            # Prefer parquet when both CSV and parquet exist for the same stem.
            if path.suffix.lower() == ".parquet" and existing.path.suffix.lower() == ".csv":
                by_stem[stem] = candidate

    return sorted(by_stem.values(), key=lambda item: item.path)
