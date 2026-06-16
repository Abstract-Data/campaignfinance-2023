"""
Post-ingest verification queries from the Fix Ingest Pipeline spec checklist.

Usage (from the project root, with the venv active):

    uv run python scripts/verify_ingest.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402
from sqlalchemy import text  # noqa: E402

console = Console()


CHECKS: list[tuple[str, str, str]] = [
    # (label, sql, expected hint)
    (
        "Fix 1 — roles in unified_transaction_persons",
        "SELECT role, COUNT(*) AS n FROM unified_transaction_persons GROUP BY role ORDER BY n DESC;",
        "recipient count should be 0; contributor/payee counts should be non-zero",
    ),
    (
        "Fix 2 — contribution entity types",
        """
        SELECT
            pe.entity_type AS contributor_type,
            re.entity_type AS recipient_type,
            COUNT(*) AS n
        FROM unified_contributions c
        JOIN unified_entities pe ON c.contributor_entity_id = pe.id
        JOIN unified_entities re ON c.recipient_entity_id = re.id
        GROUP BY 1, 2
        ORDER BY n DESC
        LIMIT 10;
        """,
        "contributor_type = person/organization; recipient_type = committee",
    ),
    (
        "Fix 3 — expenditures created",
        "SELECT COUNT(*) AS expenditure_count FROM unified_expenditures;",
        "should be non-zero after Texas EXPN records are loaded",
    ),
    (
        "Fix 4 — entities by state",
        "SELECT state_id, COUNT(*) AS n FROM unified_entities GROUP BY state_id ORDER BY n DESC;",
        "all entities should have a non-null state_id",
    ),
    (
        "Fix 5 — address count vs transaction count",
        """
        SELECT
            (SELECT COUNT(*) FROM unified_transactions) AS transactions,
            (SELECT COUNT(*) FROM unified_addresses)    AS addresses;
        """,
        "addresses should be << transactions (distinct city/state/zip combos, not one per row)",
    ),
    (
        "Fix 6 — no phantom RECIPIENT rows",
        "SELECT role, COUNT(*) AS n FROM unified_transaction_persons WHERE role = 'recipient' GROUP BY role;",
        "should return 0 rows or count=0",
    ),
    (
        "All fixes — no null transaction_date",
        "SELECT COUNT(*) AS null_dates FROM unified_transactions WHERE transaction_date IS NULL;",
        "should be 0",
    ),
    (
        "Fix 7 — unique indexes exist",
        """
        SELECT indexname, tablename
        FROM pg_indexes
        WHERE indexname IN (
            'uix_persons_name_state',
            'uix_persons_org_state',
            'uix_addresses_city_state_zip_nostreet',
            'uix_addresses_full',
            'uix_txperson_txid_personid_role',
            'uix_transactions_source_id',
            'uix_entities_type_name_state'
        )
        ORDER BY tablename, indexname;
        """,
        "should list all 7 dedup indexes",
    ),
]


def main() -> None:
    from app.core.unified_database import get_db_manager

    manager = get_db_manager(bootstrap=False)

    with manager.engine.connect() as conn:
        for label, sql, hint in CHECKS:
            console.rule(f"[bold]{label}")
            console.print(f"[dim]{hint}[/dim]\n")
            try:
                result = conn.execute(text(sql.strip()))
                rows = result.fetchall()
                cols = list(result.keys()) if rows else []

                tbl = Table(*cols, show_header=True, header_style="bold cyan")
                for row in rows:
                    tbl.add_row(*[str(v) for v in row])
                console.print(tbl)
            except Exception as exc:
                console.print(f"[red]Query failed: {exc}[/red]")

    console.rule("[bold green]Verification complete")


if __name__ == "__main__":
    main()
