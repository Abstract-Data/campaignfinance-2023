"""Revision 0003 must dedup legacy dimension duplicates and then create the three
unique indexes — on an existing (stamped-at-dc131e864993) polluted DB — while
leaving a fresh DB untouched. PG-gated: the partial unique index and temp-table
DDL are Postgres-specific (sqlite skips them).

Simulates the real upgrade path for a pre-0003 database: schema present
(``create_all``) but WITHOUT the three new unique indexes and WITH duplicate
rows in each dimension table. After stamping at ``dc131e864993`` (the prior
head) and running ``upgrade`` to ``0003_upsert_dimension_unique_indexes``:

  * campaign_entity duplicates are removed (keep lowest id per key);
  * doomed campaigns are removed after their campaign_entity children are
    purged and their transaction references are repointed to survivors;
  * committee_person duplicates are removed after their version children are
    purged;
  * all three unique indexes exist and forbid new duplicates.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, inspect, text

_PG_BASE = os.environ.get("BENCH_PG_BASE", "postgresql+psycopg2://localhost:5432")
_REV_0003 = "0003_upsert_dimension_unique_indexes"
_PRIOR_HEAD = "dc131e864993"
_IDX_CAMPAIGNS = "uix_campaigns_identity"
_IDX_CP = "uix_committee_person_role"
_IDX_CE = "uix_campaign_entity_role"


def _fresh_db_name(prefix: str = "cf_dedup_dim") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _pg_available() -> bool:
    try:
        with create_engine(f"{_PG_BASE}/postgres").connect():
            return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not _pg_available(), reason="no local PostgreSQL")


def _admin_exec(stmt) -> None:
    admin = create_engine(f"{_PG_BASE}/postgres")
    raw = admin.raw_connection()
    try:
        pg = raw.driver_connection
        pg.autocommit = True
        cur = pg.cursor()
        cur.execute(stmt)
        cur.close()
    finally:
        raw.close()
        admin.dispose()


def _drop_create(name: str) -> None:
    from psycopg2 import sql

    _admin_exec(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
    _admin_exec(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))


def _drop(name: str) -> None:
    from psycopg2 import sql

    _admin_exec(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))


def _ensure_state_schemas(url: str) -> None:
    eng = create_engine(url)
    try:
        with eng.begin() as conn:
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS texas"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS oklahoma"))
    finally:
        eng.dispose()


def _bootstrap_schema(url: str) -> None:
    """Create all tables via SQLModel.metadata.create_all (no Alembic, no dedup indexes).
    Defensively drop the three new indexes in case a future model ever defines them inline."""
    from sqlmodel import SQLModel

    from app.core import models  # noqa: F401 — register all tables

    eng = create_engine(url)
    try:
        SQLModel.metadata.create_all(eng)
        with eng.begin() as conn:
            conn.execute(text("DROP INDEX IF EXISTS uix_campaigns_identity"))
            conn.execute(text("DROP INDEX IF EXISTS uix_committee_person_role"))
            conn.execute(text("DROP INDEX IF EXISTS uix_campaign_entity_role"))
    finally:
        eng.dispose()


def _index_exists(url: str, index_name: str, table_name: str) -> bool:
    insp = inspect(create_engine(url))
    return any(i["name"] == index_name for i in insp.get_indexes(table_name))


def _all_three_indexes_exist(url: str) -> bool:
    return (
        _index_exists(url, _IDX_CAMPAIGNS, "unified_campaigns")
        and _index_exists(url, _IDX_CP, "unified_committee_persons")
        and _index_exists(url, _IDX_CE, "unified_campaign_entities")
    )


# ---------------------------------------------------------------------------
# Helpers: count duplicate groups per table
# ---------------------------------------------------------------------------
def _camp_dup_groups(conn) -> int:
    return conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT 1 FROM unified_campaigns"
            "  WHERE primary_committee_id IS NOT NULL"
            "  GROUP BY normalized_name, primary_committee_id, election_year, state_id"
            "  HAVING count(*) > 1"
            ") g"
        )
    ).scalar_one()


def _cp_dup_groups(conn) -> int:
    return conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT 1 FROM unified_committee_persons"
            "  GROUP BY committee_id, person_id, role"
            "  HAVING count(*) > 1"
            ") g"
        )
    ).scalar_one()


def _ce_dup_groups(conn) -> int:
    return conn.execute(
        text(
            "SELECT count(*) FROM ("
            "  SELECT 1 FROM unified_campaign_entities"
            "  GROUP BY campaign_id, entity_id, role"
            "  HAVING count(*) > 1"
            ") g"
        )
    ).scalar_one()


# ---------------------------------------------------------------------------
# Main "polluted DB" test
# ---------------------------------------------------------------------------
def test_0003_dedups_polluted_db_then_indexes():
    """Upgrade a pre-0003 DB seeded with one duplicate group per table.
    Verifies:
      - duplicate rows are removed (survivors kept)
      - campaign transaction references repointed
      - all three unique indexes created
      - a second upgrade is a no-op
      - the new indexes prevent fresh duplicates
    """
    from alembic import command
    from sqlmodel import Session

    from app.core.enums import CommitteeRole, TransactionType
    from app.core.models.tables import (
        State,
        UnifiedCampaign,
        UnifiedCampaignEntity,
        UnifiedCommittee,
        UnifiedCommitteePerson,
        UnifiedCommitteePersonVersion,
        UnifiedEntity,
        UnifiedTransaction,
    )
    from app.db_migrate import alembic_config

    db_name = _fresh_db_name()
    _drop_create(db_name)
    url = f"{_PG_BASE}/{db_name}"
    try:
        _ensure_state_schemas(url)
        _bootstrap_schema(url)
        eng = create_engine(url)

        # ------------------------------------------------------------------ seed
        with Session(eng) as s:
            # Prerequisites
            state = State(id=1, code="TX", name="Texas")
            committee = UnifiedCommittee(filer_id="COMM001", name="Test Committee", state_id=1)
            s.add(state)
            s.add(committee)
            s.commit()

            # unified_entities (needed for campaign_entities)
            entity = UnifiedEntity(
                entity_type="COMMITTEE",
                normalized_name="test committee",
                state_id=1,
            )
            s.add(entity)
            s.commit()
            s.refresh(entity)
            entity_id = entity.id

            # unified_campaigns: two rows with the same natural key (will be deduped)
            camp_survivor = UnifiedCampaign(
                name="Test Campaign",
                normalized_name="test campaign",
                primary_committee_id="COMM001",
                election_year=2024,
                state_id=1,
            )
            s.add(camp_survivor)
            s.commit()
            s.refresh(camp_survivor)
            camp_doomed = UnifiedCampaign(
                name="Test Campaign",
                normalized_name="test campaign",
                primary_committee_id="COMM001",
                election_year=2024,
                state_id=1,
            )
            s.add(camp_doomed)
            s.commit()
            s.refresh(camp_doomed)
            survivor_camp_id, doomed_camp_id = camp_survivor.id, camp_doomed.id

            # unified_campaign_entities: two rows with the same key (will be deduped)
            ce_survivor = UnifiedCampaignEntity(
                campaign_id=survivor_camp_id,
                entity_id=entity_id,
                role="COMMITTEE",
                is_primary=True,
                state_id=1,
            )
            s.add(ce_survivor)
            s.commit()
            s.refresh(ce_survivor)
            ce_doomed = UnifiedCampaignEntity(
                campaign_id=survivor_camp_id,
                entity_id=entity_id,
                role="COMMITTEE",
                is_primary=True,
                state_id=1,
            )
            s.add(ce_doomed)
            s.commit()
            s.refresh(ce_doomed)
            ce_survivor_id, ce_doomed_id = ce_survivor.id, ce_doomed.id

            # A transaction pointing to the doomed campaign (should be repointed).
            txn = UnifiedTransaction(
                state_id=1,
                transaction_type=TransactionType.CONTRIBUTION,
                transaction_id="TXN001",
                campaign_id=doomed_camp_id,
            )
            s.add(txn)
            s.commit()
            s.refresh(txn)
            txn_id = txn.id

            # A campaign_entity child of the doomed campaign (should be purged).
            ce_of_doomed = UnifiedCampaignEntity(
                campaign_id=doomed_camp_id,
                entity_id=entity_id,
                role="COMMITTEE",
                is_primary=True,
                state_id=1,
            )
            s.add(ce_of_doomed)
            s.commit()
            s.refresh(ce_of_doomed)
            ce_of_doomed_id = ce_of_doomed.id

            # unified_committee_persons: two rows with the same key (will be deduped)
            cp_survivor = UnifiedCommitteePerson(
                committee_id="COMM001",
                person_id=None,  # person FK — omit for simplicity
                role=CommitteeRole.TREASURER,
                state_id=1,
            )
            # We need a person to satisfy the FK. Create a minimal one.
            from app.core.models.tables import UnifiedPerson

            person = UnifiedPerson(first_name="Jane", last_name="Doe", state_id=1)
            s.add(person)
            s.commit()
            s.refresh(person)
            person_id = person.id

            cp_survivor = UnifiedCommitteePerson(
                committee_id="COMM001",
                person_id=person_id,
                role=CommitteeRole.TREASURER,
                state_id=1,
            )
            s.add(cp_survivor)
            s.commit()
            s.refresh(cp_survivor)
            cp_doomed = UnifiedCommitteePerson(
                committee_id="COMM001",
                person_id=person_id,
                role=CommitteeRole.TREASURER,
                state_id=1,
            )
            s.add(cp_doomed)
            s.commit()
            s.refresh(cp_doomed)
            cp_survivor_id, cp_doomed_id = cp_survivor.id, cp_doomed.id

            # Version child of the doomed committee_person — should be purged.
            cp_ver = UnifiedCommitteePersonVersion(
                committee_person_id=cp_doomed_id,
                version_number=1,
                data="{}",
            )
            s.add(cp_ver)
            s.commit()

        # Verify starting state: duplicates exist, indexes absent.
        assert survivor_camp_id < doomed_camp_id
        assert cp_survivor_id < cp_doomed_id
        assert ce_survivor_id < ce_doomed_id
        with eng.connect() as c:
            assert _camp_dup_groups(c) == 1
            assert _cp_dup_groups(c) == 1
            assert _ce_dup_groups(c) >= 1
        assert not _all_three_indexes_exist(url)

        # Stamp at the prior head, then upgrade to 0003.
        cfg = alembic_config(url)
        command.stamp(cfg, _PRIOR_HEAD)
        command.upgrade(cfg, _REV_0003)

        with eng.connect() as c:
            assert _camp_dup_groups(c) == 0
            assert _cp_dup_groups(c) == 0
            assert _ce_dup_groups(c) == 0

            # Survivor campaigns must be present; doomed must be gone.
            camp_ids = {r[0] for r in c.execute(text("SELECT id FROM unified_campaigns")).all()}
            assert survivor_camp_id in camp_ids
            assert doomed_camp_id not in camp_ids

            # Transaction must be repointed to survivor campaign.
            repointed = c.execute(
                text("SELECT campaign_id FROM unified_transactions WHERE id = :t"),
                {"t": txn_id},
            ).scalar_one()
            assert repointed == survivor_camp_id

            # Campaign-entity child of doomed campaign must be gone.
            ce_ids = {
                r[0] for r in c.execute(text("SELECT id FROM unified_campaign_entities")).all()
            }
            assert ce_of_doomed_id not in ce_ids

            # Survivor campaign_entity kept; doomed duplicate gone.
            assert ce_survivor_id in ce_ids
            assert ce_doomed_id not in ce_ids

            # Survivor committee_person kept; doomed gone; version child purged.
            cp_ids = {
                r[0] for r in c.execute(text("SELECT id FROM unified_committee_persons")).all()
            }
            assert cp_survivor_id in cp_ids
            assert cp_doomed_id not in cp_ids
            ver_count = c.execute(
                text(
                    "SELECT count(*) FROM unified_committee_person_versions"
                    " WHERE committee_person_id = :p"
                ),
                {"p": cp_doomed_id},
            ).scalar_one()
            assert ver_count == 0

        assert _all_three_indexes_exist(url)

        # Idempotent: re-running 0003 is a no-op (already applied).
        command.upgrade(cfg, _REV_0003)
        with eng.connect() as c:
            assert _camp_dup_groups(c) == 0
            assert _cp_dup_groups(c) == 0
            assert _ce_dup_groups(c) == 0

        # The unique indexes now forbid inserting fresh duplicates.
        with pytest.raises(Exception):  # noqa: B017 — psycopg2 UniqueViolation via SQLAlchemy
            with Session(eng) as s:
                s.add(
                    UnifiedCampaign(
                        name="Test Campaign",
                        normalized_name="test campaign",
                        primary_committee_id="COMM001",
                        election_year=2024,
                        state_id=1,
                    )
                )
                s.commit()

        with pytest.raises(Exception):  # noqa: B017
            with Session(eng) as s:
                s.add(
                    UnifiedCommitteePerson(
                        committee_id="COMM001",
                        person_id=person_id,
                        role=CommitteeRole.TREASURER,
                        state_id=1,
                    )
                )
                s.commit()

        eng.dispose()
    finally:
        _drop(db_name)


# ---------------------------------------------------------------------------
# Fresh-DB no-op test
# ---------------------------------------------------------------------------
def test_0003_is_noop_on_fresh_db():
    """A fresh ``upgrade head`` through 0003 lands clean: all three indexes present,
    nothing to dedup."""
    from alembic import command

    from app.db_migrate import alembic_config, current_revision

    db_name = _fresh_db_name()
    _drop_create(db_name)
    url = f"{_PG_BASE}/{db_name}"
    try:
        _ensure_state_schemas(url)
        cfg = alembic_config(url)
        command.upgrade(cfg, _REV_0003)
        assert current_revision(url) == _REV_0003
        assert _all_three_indexes_exist(url)
        eng = create_engine(url)
        with eng.connect() as c:
            assert _camp_dup_groups(c) == 0
            assert _cp_dup_groups(c) == 0
            assert _ce_dup_groups(c) == 0
        eng.dispose()
    finally:
        _drop(db_name)
