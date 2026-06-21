"""dedup legacy dimension duplicates, then add unique indexes for Bucket-D tables

Revision ID: 0003_upsert_dimension_unique_indexes
Revises: dc131e864993
Create Date: 2026-06-20

Three tables that previously had no unique index (``unified_campaigns``,
``unified_campaign_entities``, ``unified_committee_persons``) received
insert-only writes and accumulated duplicate rows before the upsert-all-records
project. The unique indexes required by Bucket-D's ``ON CONFLICT DO NOTHING``
writes (Task 9 / Wave 4) cannot be created until the existing duplicates are
removed.

This revision:

1. Deduplicates ``unified_campaign_entities`` by ``(campaign_id, entity_id, role)``:
   no FK children; keep lowest ``id`` per group, delete the rest.

2. Deduplicates ``unified_campaigns`` by
   ``(normalized_name, primary_committee_id, election_year, state_id)``
   WHERE ``primary_committee_id IS NOT NULL``:
   * Repoints ``unified_transactions.campaign_id`` from each doomed campaign id
     to the surviving (min) id (nullable FK; repoint preserves transaction data).
   * Purges ``unified_campaign_entities`` rows whose ``campaign_id`` still points
     to a doomed campaign.
   * Deletes the doomed campaigns.

3. Deduplicates ``unified_committee_persons`` by ``(committee_id, person_id, role)``:
   deletes ``unified_committee_person_versions`` children first, then deletes
   doomed persons.

4. Creates the three unique indexes:
   * ``uix_campaigns_identity``      — partial on ``primary_committee_id IS NOT NULL``
   * ``uix_committee_person_role``   — full unique
   * ``uix_campaign_entity_role``    — full unique

   The ``state_id`` column is included in ``uix_campaigns_identity`` because
   ``primary_committee_id`` values are scoped to a state's own portal (e.g. TX
   and OK can reuse the same filer-id string), so a cross-state collision on
   ``(normalized_name, primary_committee_id, election_year)`` is possible.

Ordering vs ``downgrade``: ``downgrade`` drops the three indexes only — deleted
duplicate rows cannot be restored.

``campaigns.py:248`` scopes its id-map read by ``state_id``, confirming the
key is state-scoped; Task 9's ``conflict_cols`` must include ``state_id`` to
match this index.

Postgres-only: the temp tables use Postgres syntax; sqlite skips this revision
(sqlite test DBs never run ``_DEDUP_INDEXES``).

The interactive/dry-run equivalent is ``scripts/dedup_dimensions.py``. The SQL
here is inlined (migrations are frozen, self-contained snapshots — they must
not import from scripts that may later change). All SQL is static (no
identifier interpolation).
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_upsert_dimension_unique_indexes"
down_revision: str | None = "widen_alembic_version"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# ---------------------------------------------------------------------------
# unified_campaign_entities dedup — natural key (campaign_id, entity_id, role)
# No FK children; delete duplicates keeping min(id).
# ---------------------------------------------------------------------------
_CE_CREATE_DOOMED = "CREATE TEMP TABLE _doomed_ce (id INTEGER PRIMARY KEY)"
_CE_POPULATE_DOOMED = """
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
_CE_PURGE = "DELETE FROM unified_campaign_entities WHERE id IN (SELECT id FROM _doomed_ce)"
_CE_DROP_DOOMED = "DROP TABLE _doomed_ce"

# ---------------------------------------------------------------------------
# unified_campaigns dedup — natural key (normalized_name, primary_committee_id,
#                                        election_year, state_id)
#                           WHERE primary_committee_id IS NOT NULL
#
# Stores (doomed_id, survivor_id) pairs so children can be repointed.
# ---------------------------------------------------------------------------
_CAMP_CREATE_DOOMED = """
    CREATE TEMP TABLE _doomed_camp (
        doomed_id   INTEGER,
        survivor_id INTEGER
    )
"""
_CAMP_POPULATE_DOOMED = """
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
# Repoint unified_transactions.campaign_id before deleting the campaign.
_CAMP_REPOINT_TRANSACTIONS = """
    UPDATE unified_transactions
    SET campaign_id = dc.survivor_id
    FROM _doomed_camp dc
    WHERE unified_transactions.campaign_id = dc.doomed_id
"""
# Delete any remaining campaign_entity children of doomed campaigns.
_CAMP_PURGE_CE_CHILDREN = """
    DELETE FROM unified_campaign_entities
    WHERE campaign_id IN (SELECT doomed_id FROM _doomed_camp)
"""
_CAMP_PURGE = "DELETE FROM unified_campaigns WHERE id IN (SELECT doomed_id FROM _doomed_camp)"
_CAMP_DROP_DOOMED = "DROP TABLE _doomed_camp"

# ---------------------------------------------------------------------------
# unified_committee_persons dedup — natural key (committee_id, person_id, role)
# FK children: unified_committee_person_versions.committee_person_id
# ---------------------------------------------------------------------------
_CP_CREATE_DOOMED = "CREATE TEMP TABLE _doomed_cp (id INTEGER PRIMARY KEY)"
_CP_POPULATE_DOOMED = """
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
_CP_PURGE_VERSIONS = """
    DELETE FROM unified_committee_person_versions
    WHERE committee_person_id IN (SELECT id FROM _doomed_cp)
"""
_CP_PURGE = "DELETE FROM unified_committee_persons WHERE id IN (SELECT id FROM _doomed_cp)"
_CP_DROP_DOOMED = "DROP TABLE _doomed_cp"

# ---------------------------------------------------------------------------
# Unique index DDL (inlined — must not change after migration is applied)
# ---------------------------------------------------------------------------
_CREATE_INDEX_CAMPAIGNS = """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_campaigns_identity
    ON unified_campaigns (normalized_name, primary_committee_id, election_year, state_id)
    WHERE primary_committee_id IS NOT NULL
"""
_CREATE_INDEX_COMMITTEE_PERSONS = """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_committee_person_role
    ON unified_committee_persons (committee_id, person_id, role)
"""
_CREATE_INDEX_CAMPAIGN_ENTITIES = """
    CREATE UNIQUE INDEX IF NOT EXISTS uix_campaign_entity_role
    ON unified_campaign_entities (campaign_id, entity_id, role)
"""

_DROP_INDEX_CAMPAIGNS = "DROP INDEX IF EXISTS uix_campaigns_identity"
_DROP_INDEX_COMMITTEE_PERSONS = "DROP INDEX IF EXISTS uix_committee_person_role"
_DROP_INDEX_CAMPAIGN_ENTITIES = "DROP INDEX IF EXISTS uix_campaign_entity_role"


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        # sqlite never received the Bucket-D indexes (bootstrap skips _DEDUP_INDEXES
        # off Postgres), so there is nothing to dedup or index here.
        return

    # 1. unified_campaign_entities dedup
    bind.execute(sa.text(_CE_CREATE_DOOMED))
    bind.execute(sa.text(_CE_POPULATE_DOOMED))
    bind.execute(sa.text(_CE_PURGE))
    bind.execute(sa.text(_CE_DROP_DOOMED))

    # 2. unified_campaigns dedup
    bind.execute(sa.text(_CAMP_CREATE_DOOMED))
    bind.execute(sa.text(_CAMP_POPULATE_DOOMED))
    bind.execute(sa.text(_CAMP_REPOINT_TRANSACTIONS))
    bind.execute(sa.text(_CAMP_PURGE_CE_CHILDREN))
    bind.execute(sa.text(_CAMP_PURGE))
    bind.execute(sa.text(_CAMP_DROP_DOOMED))

    # 3. unified_committee_persons dedup
    bind.execute(sa.text(_CP_CREATE_DOOMED))
    bind.execute(sa.text(_CP_POPULATE_DOOMED))
    bind.execute(sa.text(_CP_PURGE_VERSIONS))
    bind.execute(sa.text(_CP_PURGE))
    bind.execute(sa.text(_CP_DROP_DOOMED))

    # 4. Create the three unique indexes
    bind.execute(sa.text(_CREATE_INDEX_CAMPAIGNS))
    bind.execute(sa.text(_CREATE_INDEX_COMMITTEE_PERSONS))
    bind.execute(sa.text(_CREATE_INDEX_CAMPAIGN_ENTITIES))


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name == "sqlite":
        return
    # Deleting duplicate rows is not reversible; downgrade only drops the constraints.
    bind.execute(sa.text(_DROP_INDEX_CAMPAIGNS))
    bind.execute(sa.text(_DROP_INDEX_COMMITTEE_PERSONS))
    bind.execute(sa.text(_DROP_INDEX_CAMPAIGN_ENTITIES))
