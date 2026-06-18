"""Run VACUUM FULL ANALYZE on large tables to reclaim dead-tuple space.

DB Bloat Remediation Wave 3b — space reclamation after raw_data drops
and resolve-run pruning.

IMPORTANT: Only run this on the local/dev database.
For shared/online DBs, use pg_repack instead (see docs/db-reclaim.md).

VACUUM FULL requires an exclusive lock on each table. It is NOT safe to run
inside a transaction or via the ORM session. This script uses a raw psycopg2
connection with autocommit=True.

Usage (from the project root):
    uv run python scripts/db_reclaim.py
    uv run python scripts/db_reclaim.py --skip-analyze
    uv run python scripts/db_reclaim.py --tables unified_transactions,unified_reports
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

console = Console()

# Tables to vacuum, in order from largest to smallest (from Wave 0 baseline report).
# Adjust the order based on the current Phase 0 size report for your DB.
DEFAULT_TABLES = [
    "unified_transactions",
    "unified_reports",
    "match_decision",
    "scored_pairs",
    "candidate_pairs",
    "resolution_input",
    "unified_persons",
    "unified_addresses",
    "unified_entities",
    "unified_committees",
    "entity_crosswalk",
    "address_crosswalk",
    "merge_edges",
    "cluster_assignment",
    "ingest_errors",
]

_SAFE_IDENT = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
)


def _safe_table_name(name: str) -> str | None:
    """Return *name* if it contains only identifier-safe characters, else None."""
    return name if all(c in _SAFE_IDENT for c in name) else None


def _vacuum_stmt(table: str, analyze: bool) -> str:
    """Build the VACUUM statement for a validated table name.

    Table names come from the hardcoded DEFAULT_TABLES list or a caller-supplied
    --tables argument that has already been validated by _safe_table_name().
    The caller MUST validate the name before passing it here.
    """
    action = "VACUUM (FULL, ANALYZE)" if analyze else "VACUUM FULL"
    return action + " " + table


def main(
    tables: Annotated[
        str | None,
        typer.Option(
            "--tables",
            help="Comma-separated list of tables to vacuum. Defaults to all large tables.",
        ),
    ] = None,
    skip_analyze: Annotated[
        bool,
        typer.Option("--skip-analyze", help="Skip ANALYZE after VACUUM FULL."),
    ] = False,
) -> None:
    """Run VACUUM FULL ANALYZE on large tables to reclaim dead-tuple space.

    Uses AUTOCOMMIT mode — cannot run inside a transaction block.
    Only run on local/dev DB. For shared/online DBs use pg_repack.
    """
    import psycopg2

    from app.states.postgres_config import PostgresConfig

    target_tables = [t.strip() for t in tables.split(",")] if tables else DEFAULT_TABLES

    config = PostgresConfig()
    dsn = config.database_url.replace("postgresql://", "postgresql://", 1)

    console.print("[bold]DB Reclaim: VACUUM FULL ANALYZE[/bold]")
    console.print("Target DB: " + config.host + ":" + str(config.port) + "/" + config.db)
    console.print("Tables to vacuum: " + str(len(target_tables)))
    console.print()

    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
    except Exception as exc:
        console.print("[red]Failed to connect to Postgres: " + str(exc) + "[/red]")
        raise typer.Exit(1) from exc

    try:
        with conn.cursor() as cur:
            for table in target_tables:
                safe = _safe_table_name(table)
                if safe is None:
                    console.print("  SKIPPED (invalid table name): " + table)
                    continue

                stmt = _vacuum_stmt(safe, analyze=not skip_analyze)
                console.print("  " + stmt + " ...", end=" ")
                try:
                    cur.execute(stmt)
                    console.print("[green]done[/green]")
                except Exception as exc:
                    console.print("[red]FAILED: " + str(exc) + "[/red]")
    finally:
        conn.close()

    console.print("\n[bold green]VACUUM FULL complete.[/bold green]")
    console.print("Run scripts/db_size_report.py to measure the new DB size.")


if __name__ == "__main__":
    typer.run(main)
