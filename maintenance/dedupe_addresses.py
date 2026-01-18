"""
Utility script to deduplicate rows in unified_addresses and update all foreign references.

This script groups addresses by the key fields (street_1, city, state, zip_code),
retains the lowest id per group, updates referencing tables to point at the kept row,
and deletes the redundant address rows.
"""

from __future__ import annotations

from typing import List, Tuple
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text

from app.states.postgres_config import create_postgres_database_manager


def find_duplicates(session) -> List[Tuple[str, str, str, str, int]]:
    """Return the top duplicates for logging."""
    rows = session.exec(
        text(
            """
            SELECT street_1, city, state, zip_code, COUNT(*) AS count
            FROM unified_addresses
            GROUP BY street_1, city, state, zip_code
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            """
        )
    ).all()
    return rows


def run() -> None:
    db_manager = create_postgres_database_manager()
    with db_manager.get_session() as session:
        duplicates = find_duplicates(session)
        if not duplicates:
            print("No duplicate addresses found.")
            return

        print("Found duplicate addresses:")
        for street, city, state, zip_code, count in duplicates:
            print(f"  {street!r}, {city!r}, {state!r}, {zip_code!r}: {count}")

        # Update unified_persons
        session.exec(
            text(
                """
                WITH dup AS (
                    SELECT id, MIN(id) OVER (PARTITION BY street_1, city, state, zip_code) AS keep_id
                    FROM unified_addresses
                )
                UPDATE unified_persons up
                SET address_id = dup.keep_id
                FROM dup
                WHERE up.address_id = dup.id
                  AND dup.id <> dup.keep_id
                """
            )
        )

        # Update unified_entities
        session.exec(
            text(
                """
                WITH dup AS (
                    SELECT id, MIN(id) OVER (PARTITION BY street_1, city, state, zip_code) AS keep_id
                    FROM unified_addresses
                )
                UPDATE unified_entities ue
                SET address_id = dup.keep_id
                FROM dup
                WHERE ue.address_id = dup.id
                  AND dup.id <> dup.keep_id
                """
            )
        )

        # Update unified_committees
        session.exec(
            text(
                """
                WITH dup AS (
                    SELECT id, MIN(id) OVER (PARTITION BY street_1, city, state, zip_code) AS keep_id
                    FROM unified_addresses
                )
                UPDATE unified_committees uc
                SET address_id = dup.keep_id
                FROM dup
                WHERE uc.address_id = dup.id
                  AND dup.id <> dup.keep_id
                """
            )
        )

        # Remove duplicate address rows keeping the first id
        session.exec(
            text(
                """
                DELETE FROM unified_addresses ua
                USING (
                    SELECT id,
                           ROW_NUMBER() OVER (PARTITION BY street_1, city, state, zip_code ORDER BY id) AS rn
                    FROM unified_addresses
                ) dup
                WHERE ua.id = dup.id
                  AND dup.rn > 1
                """
            )
        )

        session.commit()

        print("Deduplication completed. Remaining duplicate groups (if any):")
        remaining = find_duplicates(session)
        if not remaining:
            print("  None 🎉")
        else:
            for street, city, state, zip_code, count in remaining:
                print(f"  {street!r}, {city!r}, {state!r}, {zip_code!r}: {count}")


if __name__ == "__main__":
    run()

