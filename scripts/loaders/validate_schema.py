"""Pre-flight schema audit across *every* source file for a state.

Reads only the column schema of each discovered file (parquet/CSV metadata — no
row loading), so it runs in seconds over the full file set, including the ~100
contribs files that ``subset_load`` never opens.

It answers the question that matters before a multi-hour full load: **is every
file of a record type schema-consistent with the others?**  If all 102 RCPT files
share one column set, the handful the subset already loaded validated the schema
for all of them.  If one file differs (a renamed/added/dropped column), that's
drift the full load would hit — and this flags exactly which file and column.

Two checks:
1. DRIFT — per record type, files that do not share the majority column set.
2. UNMAPPED — columns present but not mapped/structural/builder-handled (data the
   loader drops).  These are usually stable/expected; reported as a count per
   record type, with ``--verbose`` to list them.

Exit code is non-zero when drift or an unreadable file is found, so this can gate
a full load.  Run:  uv run python scripts/loaders/validate_schema.py [state] [--verbose]
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.core.field_coverage import _HANDLED_PREFIXES, _STRUCTURAL_COLUMNS
from app.core.unified_field_library import field_library
from scripts.loaders.file_discovery import discover_state_files


def _columns(path: Path) -> list[str]:
    """Column names from file metadata/header only — no row scan."""
    if path.suffix.lower() == ".csv":
        return pl.scan_csv(path).collect_schema().names()
    return pl.scan_parquet(path).collect_schema().names()


def _is_handled(record_type: str, column: str) -> bool:
    return any(column.startswith(p) for p in _HANDLED_PREFIXES.get(record_type, ()))


def main(state: str = "texas", *, verbose: bool = False) -> int:
    mapped = {m.state_field for m in field_library.get_state_mappings(state)}
    discovered = discover_state_files(state)

    # record_type -> list[(filename, frozenset(columns))]
    by_type: dict[str, list[tuple[str, frozenset[str]]]] = defaultdict(list)
    unreadable: list[tuple[str, str]] = []
    files_total = 0

    for item in sorted(discovered, key=lambda d: (d.record_type or "", str(d.path))):
        files_total += 1
        rtype = item.record_type or "UNKNOWN"
        try:
            cols = frozenset(_columns(item.path))
        except Exception as exc:  # noqa: BLE001 — surface unreadable files, don't crash
            unreadable.append((item.path.name, str(exc)[:80]))
            continue
        by_type[rtype].append((item.path.name, cols))

    drift: list[str] = []  # human-readable drift lines
    has_drift = False
    print(
        f"\nSchema audit — state={state!r}: {files_total} files, "
        f"{len(by_type)} record types\n" + "=" * 64
    )

    for rtype in sorted(by_type):
        entries = by_type[rtype]
        schemas = Counter(cols for _, cols in entries)
        majority, _ = schemas.most_common(1)[0]
        n_files = len(entries)

        if len(schemas) == 1:
            status = "✓ consistent"
        else:
            has_drift = True
            status = f"✗ DRIFT — {len(schemas)} distinct schemas"
        # unmapped columns in the majority schema (data we drop)
        unmapped = sorted(
            c
            for c in majority
            if c not in mapped and c not in _STRUCTURAL_COLUMNS and not _is_handled(rtype, c)
        )
        print(
            f"\n{rtype:8s} {n_files:3d} file(s)  {status}  "
            f"| {len(majority)} cols, {len(unmapped)} unmapped(dropped)"
        )

        if len(schemas) > 1:
            for fname, cols in entries:
                if cols == majority:
                    continue
                added = sorted(cols - majority)
                missing = sorted(majority - cols)
                bits = []
                if added:
                    bits.append(f"+{added}")
                if missing:
                    bits.append(f"-{missing}")
                line = f"     DRIFT {fname}: {'  '.join(bits)}"
                print(line)
                drift.append(line)
        if verbose and unmapped:
            print(f"     unmapped: {', '.join(unmapped)}")

    if unreadable:
        print(f"\n⚠ UNREADABLE FILES ({len(unreadable)}):")
        for name, err in unreadable:
            print(f"   {name}: {err}")

    print("\n" + "=" * 64)
    if has_drift or unreadable:
        print("RESULT: ✗ issues found — resolve drift / unreadable files before a full load.")
    else:
        print(
            "RESULT: ✓ every record type is schema-consistent across all its files.\n"
            "        The subset already validated these schemas; a full load adds rows, not shape."
        )
    print("Tip: pass --verbose to list the unmapped (dropped) columns per type.\n")
    return 1 if (has_drift or unreadable) else 0


if __name__ == "__main__":
    argv = sys.argv[1:]
    verbose = "--verbose" in argv or "-v" in argv
    positional = [a for a in argv if not a.startswith("-")]
    raise SystemExit(main(positional[0] if positional else "texas", verbose=verbose))
