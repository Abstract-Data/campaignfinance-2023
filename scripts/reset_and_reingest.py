"""
Reset and re-ingest Texas TEC data.

Truncates all transaction-related tables (in FK-safe cascade order),
runs bootstrap to apply any schema additions + Fix-7 dedup indexes,
then re-ingests all Texas parquet files via the vectorized engine
(same path as ``cf load`` / ``run_vectorized``).

Usage (from the project root, with the venv active):

    uv run python scripts/reset_and_reingest.py
    uv run python scripts/reset_and_reingest.py --dry-run      # show plan, no DB writes
    uv run python scripts/reset_and_reingest.py --skip-ingest  # truncate + bootstrap only
    uv run python scripts/reset_and_reingest.py --engine orm   # legacy ORM loop (debug only)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ── project root on sys.path ────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402
from sqlalchemy import text  # noqa: E402

from app.core.ingest_vectorized import run_vectorized  # noqa: E402
from app.core.unified_database import get_db_manager  # noqa: E402

console = Console()

# Tables to wipe, leaf-first so FK constraints aren't violated.
# RESTART IDENTITY resets auto-increment sequences.
_TRUNCATE_SQL = """
TRUNCATE TABLE
    unified_transaction_persons,
    unified_contributions,
    unified_expenditures,
    unified_loans,
    unified_debts,
    unified_credits,
    unified_travels,
    unified_assets,
    unified_transaction_versions,
    unified_person_versions,
    unified_committee_versions,
    unified_address_versions,
    unified_campaigns,
    unified_transactions,
    unified_persons,
    unified_addresses,
    unified_committees,
    unified_entities,
    file_origins
RESTART IDENTITY CASCADE;
"""

# Tables that might not exist yet — skip gracefully
_OPTIONAL_TABLES = {"unified_expenditures"}


def _truncate(engine, *, dry_run: bool) -> None:
    console.rule("[bold red]Truncate stale data")
    if dry_run:
        console.print("[yellow]DRY RUN — would execute:[/yellow]")
        console.print(_TRUNCATE_SQL)
        return
    with engine.connect() as conn:
        console.print("Truncating transaction-related tables …")
        try:
            conn.execute(text(_TRUNCATE_SQL))
            conn.commit()
            console.print("[green]Truncate complete.[/green]")
        except Exception as exc:
            console.print(
                f"[yellow]Bulk TRUNCATE failed ({exc}); retrying table-by-table …[/yellow]"
            )
            conn.rollback()
            _truncate_individually(conn)


_TABLE_TRUNCATE_SQLS: dict[str, str] = {
    t: "TRUNCATE TABLE " + t + " RESTART IDENTITY CASCADE;"
    for t in [
        "unified_transaction_persons",
        "unified_contributions",
        "unified_expenditures",
        "unified_loans",
        "unified_debts",
        "unified_credits",
        "unified_travels",
        "unified_assets",
        "unified_transaction_versions",
        "unified_person_versions",
        "unified_committee_versions",
        "unified_address_versions",
        "unified_campaigns",
        "unified_transactions",
        "unified_persons",
        "unified_addresses",
        "unified_committees",
        "unified_entities",
        "file_origins",
    ]
}


def _truncate_individually(conn) -> None:
    """Fall back: truncate each table separately, skipping those that don't exist."""
    for table, sql in _TABLE_TRUNCATE_SQLS.items():
        try:
            conn.execute(text(sql))
            conn.commit()
            console.print(f"  ✓ {table}")
        except Exception as exc:
            conn.rollback()
            if table in _OPTIONAL_TABLES:
                console.print(f"  [dim]skip {table} — {exc}[/dim]")
            else:
                console.print(f"  [yellow]warn {table} — {exc}[/yellow]")


def _bootstrap(manager, *, dry_run: bool) -> None:
    console.rule("[bold blue]Bootstrap (create_all + dedup indexes)")
    if dry_run:
        console.print("[yellow]DRY RUN — would call manager.bootstrap()[/yellow]")
        return
    manager.bootstrap()
    console.print("[green]Bootstrap complete.[/green]")


def _ingest(engine, *, dry_run: bool, state: str = "texas") -> None:
    """Vectorized ingest — mirrors the ``cf load`` default path."""
    console.rule("[bold green]Re-ingest Texas parquet files (vectorized)")
    parquet_dir = ROOT / "tmp" / state
    if not parquet_dir.exists():
        console.print(
            f"[red]Parquet directory not found:[/red] {parquet_dir}\n"
            "Run [bold]cf prepare texas --skip-download[/bold] first."
        )
        sys.exit(1)

    files = sorted(parquet_dir.glob("**/*.parquet"))
    if not files:
        console.print(f"[red]No .parquet files found under {parquet_dir}[/red]")
        sys.exit(1)

    console.print(f"Found {len(files)} parquet file(s) under {parquet_dir.relative_to(ROOT)}")

    if dry_run:
        console.print("[yellow]DRY RUN — would call run_vectorized()[/yellow]")
        return

    results = run_vectorized(engine, parquet_dir, state=state, dry_run=False)
    loaded = int(results.get("loaded", 0))
    console.print(
        f"\n[bold green]Re-ingest complete.[/bold green] "
        f"{loaded} rows loaded across {results.get('families_run', 0)} families."
    )


def _ingest_legacy(manager, *, dry_run: bool) -> None:
    """Legacy ORM per-file loop — kept behind ``--engine orm`` for debug parity."""
    console.rule("[bold green]Re-ingest Texas parquet files (ORM legacy)")
    parquet_dir = ROOT / "tmp" / "texas"
    if not parquet_dir.exists():
        console.print(
            f"[red]Parquet directory not found:[/red] {parquet_dir}\n"
            "Run [bold]cf download texas[/bold] first, or adjust the path above."
        )
        sys.exit(1)

    files = sorted(parquet_dir.glob("**/*.parquet"))
    if not files:
        console.print(f"[red]No .parquet files found under {parquet_dir}[/red]")
        sys.exit(1)

    console.print(f"Found {len(files)} parquet file(s):")
    for f in files:
        console.print(f"  {f.relative_to(ROOT)}")

    if dry_run:
        console.print("[yellow]DRY RUN — skipping DB writes.[/yellow]")
        return

    total_saved = 0
    for f in files:
        console.print(f"\n[bold]Loading[/bold] {f.name} …")
        try:
            count = manager.load_and_save_file(f, "texas")
            total_saved += count
            console.print(f"  [green]✓ {count} transactions[/green]")
        except Exception as exc:
            console.print(f"  [red]✗ {exc}[/red]")

    console.print(
        f"\n[bold green]Re-ingest complete.[/bold green] {total_saved} transactions saved."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset and re-ingest Texas campaign finance data.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would happen without writing."
    )
    parser.add_argument(
        "--skip-ingest", action="store_true", help="Truncate + bootstrap only; skip parquet load."
    )
    parser.add_argument(
        "--engine",
        choices=("vectorized", "orm"),
        default="vectorized",
        help="Ingest engine (default: vectorized, matches cf load).",
    )
    args = parser.parse_args()

    manager = get_db_manager(bootstrap=False)  # We'll call bootstrap manually below

    _truncate(manager.engine, dry_run=args.dry_run)
    _bootstrap(manager, dry_run=args.dry_run)

    if args.skip_ingest:
        console.print("[dim]--skip-ingest set; stopping after bootstrap.[/dim]")
    elif args.engine == "orm":
        _ingest_legacy(manager, dry_run=args.dry_run)
    else:
        _ingest(manager.engine, dry_run=args.dry_run)

    console.rule("[bold]Done")


if __name__ == "__main__":
    main()
