"""One-off dedup of legacy duplicate rows in three Bucket-D dimension tables.

Background
----------
Before the Wave-3 unique-index migration
(``0003_upsert_dimension_unique_indexes``), the insert-only load path could
produce duplicate rows in:

* ``unified_campaigns``       — key: ``(normalized_name, primary_committee_id,
                                         election_year, state_id)``
                                  WHERE ``primary_committee_id IS NOT NULL``
* ``unified_campaign_entities`` — key: ``(campaign_id, entity_id, role)``
* ``unified_committee_persons`` — key: ``(committee_id, person_id, role)``

The new unique indexes cannot be created until the existing duplicates are
removed.  Fresh loads after the migration are already protected; this script
cleans *existing* legacy duplicates so the index can be applied to a
pre-existing database.

What it does (single transaction per table group)
-------------------------------------------------
For each table the script:

1. Stages the non-surviving duplicate IDs into a temp table (keeps the lowest
   ``id`` per natural-key group).
2. Purges FK children of the doomed rows (deepest-FK-first; all FKs are
   ``ON DELETE NO ACTION``).
3. Purges the doomed rows themselves.

For ``unified_campaigns`` specifically, ``unified_transactions.campaign_id``
references the campaign via a nullable FK, so doomed campaign IDs are
**repointed** (``UPDATE``) to the surviving ID before deletion.

Safety
------
- DRY-RUN BY DEFAULT: prints the counts it *would* purge and rolls back.
  Pass ``--apply`` to commit.
- All-or-nothing: all three table groups run in one transaction.
- ``--create-indexes`` additionally creates the three unique indexes after the
  cleanup (only meaningful with ``--apply``).

Usage
-----
    uv run python scripts/dedup_dimensions.py \\
        --db-url postgresql+psycopg2://USER@localhost:5432/DBNAME            # dry-run
    uv run python scripts/dedup_dimensions.py --db-url ... --apply --create-indexes

All SQL is static (no identifier interpolation).
"""

from __future__ import annotations

import argparse
import sys

from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# unified_campaign_entities  — natural key: (campaign_id, entity_id, role)
# No FK children; just delete duplicates keeping min(id).
# ---------------------------------------------------------------------------
_CE_CREATE_DOOMED = text("CREATE TEMP TABLE _doomed_ce (id INTEGER PRIMARY KEY) ON COMMIT DROP")
_CE_POPULATE_DOOMED = text(
    """
    INSERT INTO _doomed_ce (id)
    SELECT id FROM (
        SELECT id,
               row_number() OVER (
                   PARTITION BY campaign_id, entity_id, role
                   ORDER BY id
               ) AS rn
        FROM unified_campaign_entities
    ) ranked
    WHERE ranked.rn > 1
    """
)
_CE_COUNT_DOOMED = text("SELECT count(*) FROM _doomed_ce")
_CE_COUNT_GROUPS = text(
    """
    SELECT count(*) FROM (
        SELECT 1 FROM unified_campaign_entities
        GROUP BY campaign_id, entity_id, role
        HAVING count(*) > 1
    ) g
    """
)
_CE_PURGE = text("DELETE FROM unified_campaign_entities WHERE id IN (SELECT id FROM _doomed_ce)")

# ---------------------------------------------------------------------------
# unified_campaigns  — natural key: (normalized_name, primary_committee_id,
#                                    election_year, state_id)
#                       WHERE primary_committee_id IS NOT NULL
#
# FK children:
#   unified_campaign_entities.campaign_id  -> repoint then delete orphans
#   unified_transactions.campaign_id       -> repoint to survivor (nullable FK)
# ---------------------------------------------------------------------------
_CAMP_CREATE_DOOMED = text(
    """
    CREATE TEMP TABLE _doomed_camp (
        doomed_id  INTEGER,
        survivor_id INTEGER
    ) ON COMMIT DROP
    """
)
_CAMP_POPULATE_DOOMED = text(
    """
    INSERT INTO _doomed_camp (doomed_id, survivor_id)
    SELECT id AS doomed_id, survivor_id
    FROM (
        SELECT id,
               first_value(id) OVER (
                   PARTITION BY normalized_name, primary_committee_id,
                                election_year, state_id
                   ORDER BY id
               ) AS survivor_id,
               row_number() OVER (
                   PARTITION BY normalized_name, primary_committee_id,
                                election_year, state_id
                   ORDER BY id
               ) AS rn
        FROM unified_campaigns
        WHERE primary_committee_id IS NOT NULL
    ) ranked
    WHERE rn > 1
    """
)
_CAMP_COUNT_DOOMED = text("SELECT count(*) FROM _doomed_camp")
_CAMP_COUNT_GROUPS = text(
    """
    SELECT count(*) FROM (
        SELECT 1 FROM unified_campaigns
        WHERE primary_committee_id IS NOT NULL
        GROUP BY normalized_name, primary_committee_id, election_year, state_id
        HAVING count(*) > 1
    ) g
    """
)
# Repoint unified_transactions.campaign_id from doomed -> survivor before deleting.
_CAMP_REPOINT_TRANSACTIONS = text(
    """
    UPDATE unified_transactions
    SET campaign_id = dc.survivor_id
    FROM _doomed_camp dc
    WHERE unified_transactions.campaign_id = dc.doomed_id
    """
)
# Delete campaign_entity children whose campaign_id points to a doomed campaign.
_CAMP_PURGE_CE_CHILDREN = text(
    """
    DELETE FROM unified_campaign_entities
    WHERE campaign_id IN (SELECT doomed_id FROM _doomed_camp)
    """
)
_CAMP_PURGE = text("DELETE FROM unified_campaigns WHERE id IN (SELECT doomed_id FROM _doomed_camp)")

# ---------------------------------------------------------------------------
# unified_committee_persons  — natural key: (committee_id, person_id, role)
# FK children: unified_committee_person_versions.committee_person_id
# ---------------------------------------------------------------------------
_CP_CREATE_DOOMED = text("CREATE TEMP TABLE _doomed_cp (id INTEGER PRIMARY KEY) ON COMMIT DROP")
_CP_POPULATE_DOOMED = text(
    """
    INSERT INTO _doomed_cp (id)
    SELECT id FROM (
        SELECT id,
               row_number() OVER (
                   PARTITION BY committee_id, person_id, role
                   ORDER BY id
               ) AS rn
        FROM unified_committee_persons
    ) ranked
    WHERE ranked.rn > 1
    """
)
_CP_COUNT_DOOMED = text("SELECT count(*) FROM _doomed_cp")
_CP_COUNT_GROUPS = text(
    """
    SELECT count(*) FROM (
        SELECT 1 FROM unified_committee_persons
        GROUP BY committee_id, person_id, role
        HAVING count(*) > 1
    ) g
    """
)
_CP_PURGE_VERSIONS = text(
    """
    DELETE FROM unified_committee_person_versions
    WHERE committee_person_id IN (SELECT id FROM _doomed_cp)
    """
)
_CP_PURGE = text("DELETE FROM unified_committee_persons WHERE id IN (SELECT id FROM _doomed_cp)")

# ---------------------------------------------------------------------------
# Index creation (only run with --create-indexes --apply)
# ---------------------------------------------------------------------------
_CREATE_INDEX_CAMPAIGNS = text(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_campaigns_identity
    ON unified_campaigns (normalized_name, primary_committee_id, election_year, state_id)
    WHERE primary_committee_id IS NOT NULL
    """
)
_CREATE_INDEX_COMMITTEE_PERSONS = text(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_committee_person_role
    ON unified_committee_persons (committee_id, person_id, role)
    """
)
_CREATE_INDEX_CAMPAIGN_ENTITIES = text(
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_campaign_entity_role
    ON unified_campaign_entities (campaign_id, entity_id, role)
    """
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Dedup legacy dimension duplicates in unified_campaigns, "
        "unified_campaign_entities, and unified_committee_persons."
    )
    parser.add_argument("--db-url", required=True, help="SQLAlchemy database URL")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Commit the cleanup (default: dry-run + rollback)",
    )
    parser.add_argument(
        "--create-indexes",
        action="store_true",
        help="Also create the three unique indexes after cleanup",
    )
    args = parser.parse_args(argv)

    if "postgresql" not in args.db_url and "postgres" not in args.db_url:
        sys.exit(
            "This dedup script supports PostgreSQL only "
            "(uses ON COMMIT DROP temp tables). Got: " + args.db_url.split("://", 1)[0]
        )

    engine = create_engine(args.db_url)
    conn = engine.connect()
    trans = conn.begin()
    try:
        # --- unified_campaign_entities ---
        print("=== unified_campaign_entities ===")
        conn.execute(_CE_CREATE_DOOMED)
        conn.execute(_CE_POPULATE_DOOMED)
        ce_groups = conn.execute(_CE_COUNT_GROUPS).scalar_one()
        ce_doomed = conn.execute(_CE_COUNT_DOOMED).scalar_one()
        print(f"  duplicate groups: {ce_groups:,}")
        print(f"  rows to purge (keeping lowest id): {ce_doomed:,}")
        ce_res = conn.execute(_CE_PURGE)
        if args.apply:
            print(f"  purged {ce_res.rowcount:,} duplicate campaign_entity rows")

        # --- unified_campaigns ---
        print("=== unified_campaigns ===")
        conn.execute(_CAMP_CREATE_DOOMED)
        conn.execute(_CAMP_POPULATE_DOOMED)
        camp_groups = conn.execute(_CAMP_COUNT_GROUPS).scalar_one()
        camp_doomed = conn.execute(_CAMP_COUNT_DOOMED).scalar_one()
        print(f"  duplicate groups: {camp_groups:,}")
        print(f"  rows to purge (keeping lowest id per group): {camp_doomed:,}")
        txn_res = conn.execute(_CAMP_REPOINT_TRANSACTIONS)
        if txn_res.rowcount:
            print(f"  repointed {txn_res.rowcount:,} unified_transactions.campaign_id")
        ce_child_res = conn.execute(_CAMP_PURGE_CE_CHILDREN)
        if ce_child_res.rowcount:
            print(
                f"  purged {ce_child_res.rowcount:,} unified_campaign_entities "
                f"(children of doomed campaigns)"
            )
        camp_res = conn.execute(_CAMP_PURGE)
        if args.apply:
            print(f"  purged {camp_res.rowcount:,} duplicate unified_campaigns rows")

        # --- unified_committee_persons ---
        print("=== unified_committee_persons ===")
        conn.execute(_CP_CREATE_DOOMED)
        conn.execute(_CP_POPULATE_DOOMED)
        cp_groups = conn.execute(_CP_COUNT_GROUPS).scalar_one()
        cp_doomed = conn.execute(_CP_COUNT_DOOMED).scalar_one()
        print(f"  duplicate groups: {cp_groups:,}")
        print(f"  rows to purge (keeping lowest id): {cp_doomed:,}")
        ver_res = conn.execute(_CP_PURGE_VERSIONS)
        if ver_res.rowcount:
            print(f"  purged {ver_res.rowcount:,} unified_committee_person_versions")
        cp_res = conn.execute(_CP_PURGE)
        if args.apply:
            print(f"  purged {cp_res.rowcount:,} duplicate unified_committee_persons rows")

        if args.create_indexes:
            conn.execute(_CREATE_INDEX_CAMPAIGNS)
            print("created uix_campaigns_identity")
            conn.execute(_CREATE_INDEX_COMMITTEE_PERSONS)
            print("created uix_committee_person_role")
            conn.execute(_CREATE_INDEX_CAMPAIGN_ENTITIES)
            print("created uix_campaign_entity_role")

        if args.apply:
            trans.commit()
            print("\nAPPLIED — committed.")
        else:
            trans.rollback()
            print("\nDRY RUN — rolled back. Re-run with --apply to commit.")
    except Exception:
        trans.rollback()
        raise
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
