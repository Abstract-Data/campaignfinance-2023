"""Vectorized CAND enrichment family — candidate <-> expenditure linkage.

CAND is NOT a transaction type: a ``cand_*`` row's ``expendInfoId`` is the id of an
EXPENDITURE already loaded from the expend_* files. The ORM
(``production_loader._persist_cand_link``) resolves the named candidate as a deduped
person (find-or-create), find-or-creates that person's entity, and attaches a
``UnifiedTransactionPerson(role=CANDIDATE)`` to the matching expenditure — creating no
new transaction.

This family reproduces that EXACTLY, columnar, AFTER ``flat_txns_detail`` (priority 11)
so the expenditures it enriches already exist. Priority 12.

It writes / enriches:
  * ``unified_persons``               — NEW candidate persons (find-or-create by name).
  * ``unified_entities``              — NEW candidate entities (find-or-create by
                                        (entity_type, normalized_name)).
  * ``unified_transaction_persons``   — one CANDIDATE junction row per CAND row that
                                        matched an expenditure and resolved a candidate
                                        person.

Mirrors the ORM exactly:
  * ``build_person(raw, CANDIDATE, field_prefix="candidate")`` — name comes from
    ``candidateNameFirst/Last/SuffixCd/Organization`` (middle is ``person_middle_name``
    -> None; employer/occupation absent on CAND rows). Candidate rows carry NO address
    columns, so the candidate person/entity get NO address (``address_id`` is NULL).
  * find-or-create person key: organization present -> ``(lower(org))`` only; else
    ``(lower(first), lower(last))`` with org NULL (``_find_person_by_name_state``).
  * find-or-create entity key: ``(entity_type, normalized_name)`` within the state
    (``_find_entity`` / ``_get_or_create_entity``); ORGANIZATION when org present else
    PERSON; entity name = org else full_name.
  * junction row only when the expenditure exists AND the candidate person resolves;
    deduped on (transaction_id, person_id, role=CANDIDATE).

All id-maps are read back from the already-written tables via parameterized SQLAlchemy
core ``select`` and attached with Polars joins. Pure column expressions only — no
``map_elements`` / ``.apply`` (no per-row Python UDF).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from sqlalchemy import select

from app.core.ingest_vectorized import common, id_maps
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.models import (
    UnifiedEntity,
    UnifiedPerson,
    UnifiedTransaction,
    UnifiedTransactionPerson,
)
from app.logger import Logger

_logger = Logger(__name__)


# ---------------------------------------------------------------------------
# Source column whitelist (the candidate name fields the ORM resolves).
# ---------------------------------------------------------------------------

_CAND_COLS = (
    "recordType",
    "formTypeCd",
    "schedFormTypeCd",
    "reportInfoIdent",
    "receivedDt",
    "infoOnlyFlag",
    "filerIdent",
    "filerTypeCd",
    "filerName",
    "expendInfoId",
    "expendDt",
    "expendAmount",
    "expendDescr",
    "expendCatCd",
    "expendCatDescr",
    "itemizeFlag",
    "candidatePersentTypeCd",
    "candidateNameOrganization",
    "candidateNameLast",
    "candidateNameSuffixCd",
    "candidateNameFirst",
    "candidateNamePrefixCd",
    "candidateNameShort",
)

# Placeholder last names that force PersonType.UNKNOWN (mirrors build_person /
# constants.PLACEHOLDER_NAMES, applied case-insensitively on the stripped last name).
_PLACEHOLDER_NAMES_UPPER = frozenset(
    {
        "NON-ITEMIZED CONTRIBUTOR",
        "NON-ITEMIZED",
        "UNKNOWN",
        "ANONYMOUS",
    }
)


# ---------------------------------------------------------------------------
# Frame IO helpers
# ---------------------------------------------------------------------------


def _read(files: list[Path]) -> pl.DataFrame | None:
    frames = [pl.read_parquet(p) for p in files]
    if not frames:
        return None
    return frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")


def _ensure_cols(df: pl.DataFrame, names: tuple[str, ...]) -> pl.DataFrame:
    missing = [pl.lit(None, dtype=pl.Utf8).alias(n) for n in names if n not in df.columns]
    return df.with_columns(missing) if missing else df


def _cs(col: str) -> pl.Expr:
    return common.clean_str(col)


def _norm_name_expr_from(name_expr: pl.Expr) -> pl.Expr:
    """normalize_entity_name applied to an arbitrary string expression (mirrors
    ``value_objects.normalize_entity_name``: strip -> lower -> non-alnum to single
    spaces -> collapse -> strip; null/empty -> "")."""
    s = name_expr.cast(pl.Utf8).str.strip_chars().str.to_lowercase()
    s = s.str.replace_all(r"[^a-z0-9]+", " ").str.replace_all(r"\s+", " ").str.strip_chars()
    return s.fill_null("")


# ---------------------------------------------------------------------------
# Person type / key expressions (mirror builders.build_person).
# ---------------------------------------------------------------------------


def _person_type_expr(last_col: str, first_col: str, org_col: str) -> pl.Expr:
    """Mirror ORM build_person person_type priority order:
    1. last_name stripped.upper() in PLACEHOLDER_NAMES -> UNKNOWN
    2. organization is not null -> ORGANIZATION
    3. first AND last both non-null -> INDIVIDUAL
    4. else -> UNKNOWN
    """
    last_upper = pl.col(last_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase()
    first_clean = _cs(first_col)
    org_clean = _cs(org_col)
    return (
        pl.when(last_upper.is_in(list(_PLACEHOLDER_NAMES_UPPER)))
        .then(pl.lit("UNKNOWN"))
        .when(org_clean.is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .when(first_clean.is_not_null() & _cs(last_col).is_not_null())
        .then(pl.lit("INDIVIDUAL"))
        .otherwise(pl.lit("UNKNOWN"))
    )


def _project_candidates(df: pl.DataFrame) -> pl.DataFrame:
    """One row per CAND record with candidate person/entity keys + expenditure key.

    The ORM person find-or-create key is org-only when an organization is present,
    else (first, last). We carry BOTH the org-only key part and the individual key
    part and select the right one with ``_pk_org`` / ``_pk_fn`` / ``_pk_ln`` matching
    the ORM semantics (org present -> fn/ln are NULLED in the key so they cannot
    collide with an individual of the same name).
    """
    org = _cs("candidateNameOrganization")
    first = _cs("candidateNameFirst")
    last = _cs("candidateNameLast")
    suffix = _cs("candidateNameSuffixCd")
    # middle is person_middle_name in the ORM -> absent on CAND rows -> None.
    full_name = common.full_name_expr(
        "candidateNameFirst",
        "person_middle_name",
        "candidateNameLast",
        "candidateNameSuffixCd",
        "candidateNameOrganization",
    )
    org_low = org.str.to_lowercase()
    # Person find-or-create key (mirrors _find_person_by_name_state):
    #   org present -> key on (lower(org)) only, fn/ln NULL.
    #   else        -> key on (lower(first), lower(last)), org NULL.
    pk_org = org_low
    pk_fn = pl.when(org.is_not_null()).then(None).otherwise(first.str.to_lowercase())
    pk_ln = pl.when(org.is_not_null()).then(None).otherwise(last.str.to_lowercase())

    ent_type = pl.when(org.is_not_null()).then(pl.lit("ORGANIZATION")).otherwise(pl.lit("PERSON"))
    ent_name = pl.when(org.is_not_null()).then(org).otherwise(full_name)

    return (
        df.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("person_middle_name"),
        )
        .with_row_index("_cand_row")
        .with_columns(
            # expenditure natural key
            pl.col("expendInfoId").cast(pl.Utf8).alias("_expend_id"),
            # candidate person identity (stored values, ORM build_person)
            first.alias("first_name"),
            last.alias("last_name"),
            pl.lit(None, dtype=pl.Utf8).alias("middle_name"),
            suffix.alias("suffix"),
            org.alias("organization"),
            _person_type_expr(
                "candidateNameLast", "candidateNameFirst", "candidateNameOrganization"
            ).alias("person_type"),
            full_name.str.len_chars().alias("_full_len"),
            # find-or-create person key. Candidate records carry no address columns, so the
            # address dimension of the key is always NULL (candidate persons get NULL
            # dedup_addr_key, matching the ORM build_person(CANDIDATE) -> no address).
            pk_org.alias("_pk_org"),
            pk_fn.alias("_pk_fn"),
            pk_ln.alias("_pk_ln"),
            pl.lit(None, dtype=pl.Utf8).alias("_pk_addr"),
            # entity key
            ent_type.alias("_ent_type"),
            _norm_name_expr_from(ent_name).alias("_ent_norm"),
            # full-identity key (resolves person id even for non-dedupable names).
            # _fk_addr is NULL (candidates carry no address) so the candidate resolves to
            # the NULL-addr person of that name, not a same-name address-bearing person.
            first.str.to_lowercase().alias("_fk_fn"),
            last.str.to_lowercase().alias("_fk_ln"),
            org_low.alias("_fk_org"),
            suffix.str.to_lowercase().alias("_fk_sfx"),
            _person_type_expr(
                "candidateNameLast", "candidateNameFirst", "candidateNameOrganization"
            ).alias("_fk_type"),
            pl.lit(None, dtype=pl.Utf8).alias("_fk_addr"),
        )
    )


# ---------------------------------------------------------------------------
# Id-maps read back from the already-written tables (parameterized core select).
# ---------------------------------------------------------------------------


def _person_id_map(session, state_id: int) -> pl.DataFrame:
    """Read persons keyed by the ORM find-or-create key (id-map for junction linkage).

    org present -> (lower(org)) with fn/ln/addr NULL; else
    (lower(first), lower(last), dedup_addr_key) with org NULL — exactly how
    ``_find_person_by_name_state`` keys its lookup (now address-inclusive for
    individuals).  Candidate persons carry no address, so their ``_pk_addr`` is NULL and
    they resolve only against same-name persons whose ``dedup_addr_key IS NULL``
    (matching the ORM).  Called both before the candidate insert (to detect which
    candidates already exist) and after (to resolve every candidate's person id).
    """
    rows = session.execute(
        select(
            UnifiedPerson.id,
            UnifiedPerson.first_name,
            UnifiedPerson.last_name,
            UnifiedPerson.organization,
            UnifiedPerson.dedup_addr_key,
        ).where(UnifiedPerson.state_id == state_id)
    ).all()

    def _low(v):
        return v.strip().lower() if isinstance(v, str) and v.strip() else None

    pid, pk_org, pk_fn, pk_ln, pk_addr = [], [], [], [], []
    for r in rows:
        org = _low(r[3])
        pid.append(r[0])
        pk_org.append(org)
        pk_fn.append(None if org is not None else _low(r[1]))
        pk_ln.append(None if org is not None else _low(r[2]))
        # Org-persons key on org alone (addr NULL); individuals carry dedup_addr_key.
        pk_addr.append(None if org is not None else r[4])
    return pl.DataFrame(
        {"_pid": pid, "_pk_org": pk_org, "_pk_fn": pk_fn, "_pk_ln": pk_ln, "_pk_addr": pk_addr},
        schema={
            "_pid": pl.Int64,
            "_pk_org": pl.Utf8,
            "_pk_fn": pl.Utf8,
            "_pk_ln": pl.Utf8,
            "_pk_addr": pl.Utf8,
        },
    )


def _person_full_id_map(session, state_id: int) -> pl.DataFrame:
    """Read persons keyed on their FULL natural identity
    (first/last/org/suffix/type/dedup_addr_key).

    Used to resolve a candidate row's person id for the junction and the entity
    representative. Unlike the find-or-create key (which is None for last-only /
    first-only names), the full identity always resolves a candidate to a person —
    and because structurally-identical persons resolve to the SAME natural snapshot
    under ``resolve_fks=True``, picking ANY matching surrogate id (min) is sufficient.

    ``dedup_addr_key`` is part of the key so a candidate (no address -> NULL key)
    resolves to the NULL-addr person of that name, NOT a same-name address-bearing
    contributor/payee the new dedup key now keeps separate (matching the ORM).
    """
    rows = session.execute(
        select(
            UnifiedPerson.id,
            UnifiedPerson.first_name,
            UnifiedPerson.last_name,
            UnifiedPerson.organization,
            UnifiedPerson.suffix,
            UnifiedPerson.person_type,
            UnifiedPerson.dedup_addr_key,
        ).where(UnifiedPerson.state_id == state_id)
    ).all()

    def _low(v):
        return v.strip().lower() if isinstance(v, str) and v.strip() else None

    df = pl.DataFrame(
        {
            "_pid": [r[0] for r in rows],
            "_fk_fn": [_low(r[1]) for r in rows],
            "_fk_ln": [_low(r[2]) for r in rows],
            "_fk_org": [_low(r[3]) for r in rows],
            "_fk_sfx": [_low(r[4]) for r in rows],
            "_fk_type": [getattr(r[5], "name", r[5]) for r in rows],
            "_fk_addr": [r[6] for r in rows],
        },
        schema={
            "_pid": pl.Int64,
            "_fk_fn": pl.Utf8,
            "_fk_ln": pl.Utf8,
            "_fk_org": pl.Utf8,
            "_fk_sfx": pl.Utf8,
            "_fk_type": pl.Utf8,
            "_fk_addr": pl.Utf8,
        },
    )
    # One representative id per full-identity key (structurally identical persons
    # resolve identically under resolve_fks, so the smallest id is a stable pick).
    return df.group_by(["_fk_fn", "_fk_ln", "_fk_org", "_fk_sfx", "_fk_type", "_fk_addr"]).agg(
        pl.col("_pid").min().alias("_pid")
    )


def _entity_id_map(session, state_id: int) -> pl.DataFrame:
    """Read entities keyed by (entity_type, normalized_name) for the state.

    entity_type is stored as the enum NAME; a "" normalized_name is stored NULL by
    the ORM (coalesced to "" here for join parity).
    """
    rows = session.execute(
        select(
            UnifiedEntity.id,
            UnifiedEntity.entity_type,
            UnifiedEntity.normalized_name,
        ).where(UnifiedEntity.state_id == state_id)
    ).all()
    return pl.DataFrame(
        {
            "_ent_id": [r[0] for r in rows],
            "_ent_type": [getattr(r[1], "name", r[1]) for r in rows],
            "_ent_norm": [(r[2] if r[2] is not None else "") for r in rows],
        },
        schema={"_ent_id": pl.Int64, "_ent_type": pl.Utf8, "_ent_norm": pl.Utf8},
    )


def _expenditure_id_map(session, state_id: int) -> pl.DataFrame:
    """Read expenditure natural id (transaction_id) -> surrogate id for the state."""
    rows = session.execute(
        select(
            UnifiedTransaction.id,
            UnifiedTransaction.transaction_id,
        ).where(
            UnifiedTransaction.state_id == state_id,
            UnifiedTransaction.transaction_type == "EXPENDITURE",
        )
    ).all()
    return pl.DataFrame(
        {
            "_txn_id": [r[0] for r in rows],
            "_expend_id": [(None if r[1] is None else str(r[1])) for r in rows],
        },
        schema={"_txn_id": pl.Int64, "_expend_id": pl.Utf8},
    )


# ---------------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------------


class CandWorker:
    """CAND enrichment: candidate <-> expenditure linkage (no new transactions)."""

    record_types = frozenset({"CAND"})
    priority = 12  # after flat_txns_detail (11): expenditures must already exist.

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        cand = _read(files_by_type.get("CAND", []))
        if cand is None or cand.height == 0:
            return {"loaded": 0}
        cand = _ensure_cols(cand, _CAND_COLS)

        proj = _project_candidates(cand)
        # A candidate person exists only when full_name is non-empty (build_person -> None).
        proj = proj.filter(pl.col("_full_len") > 0)

        # CRITICAL: the ORM (``_persist_cand_link``) builds the candidate person but
        # only ``session.add``s it (persisting the person + its entity) AFTER it has
        # confirmed the expenditure exists. When the named ``expendInfoId`` has no
        # matching expenditure it returns ``unlinked_no_expenditure`` BEFORE the add,
        # so NO person/entity is created for an unmatched CAND row. We mirror that by
        # restricting person/entity/junction creation to CAND rows whose expenditure
        # is present.
        expend_map = _expenditure_id_map(ctx.session, ctx.state_id)
        cand_rows = proj.join(expend_map, on="_expend_id", how="left").filter(
            pl.col("_txn_id").is_not_null()
        )

        counts: dict[str, int] = {}

        # 1. Insert NEW candidate persons (find-or-create by ORM person key).
        new_persons = self._insert_new_persons(cand_rows, ctx)
        counts["persons"] = new_persons

        # 2. Insert NEW candidate entities (find-or-create by (type, normalized_name)),
        #    linking each new entity's representative person.
        new_entities = self._insert_new_entities(cand_rows, ctx)
        counts["entities"] = new_entities

        # 3. Build the CANDIDATE junction rows (expenditure exists + person resolves).
        counts["txn_persons"] = self._write_junction(cand_rows, ctx)

        loaded = sum(counts.values())
        _logger.info(f"[vectorized.cand] loaded {loaded} rows: {counts}")
        return {"loaded": loaded, **counts}

    # -- persons ------------------------------------------------------------

    def _insert_new_persons(self, cand_rows: pl.DataFrame, ctx: FamilyContext) -> int:
        """Insert candidate persons, replicating the ORM find-or-create EXACTLY.

        The ORM only DEDUPES (find-or-create) a person when its key is resolvable:
        ``_find_person_by_name_state`` / ``BuilderCache.person_key`` return a key only
        when an organization is present OR BOTH first and last are present. For a name
        with only first OR only last (no org), find always returns None and the cache
        key is None, so the ORM creates a BRAND-NEW person for EVERY such occurrence
        (no dedup, no reuse of an existing row). We split on that:

        * dedupable rows -> first occurrence per key wins; skip keys already present.
        * non-dedupable rows -> one person per matched CAND row (no existing check).

        Candidate persons carry no address columns, so ``address_id`` stays NULL.
        """
        existing_keys = id_maps.person_key_frame(ctx.engine, ctx.state_id).select(
            ["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"]
        )

        dedupable = pl.col("_pk_org").is_not_null() | (
            pl.col("_pk_fn").is_not_null() & pl.col("_pk_ln").is_not_null()
        )

        # Dedupable: first occurrence per person key (row order), drop existing keys.
        # _pk_addr is NULL for candidates (no address), so they skip only same-name
        # persons whose dedup_addr_key IS NULL — matching the ORM find-or-create.
        dedup_first = (
            cand_rows.filter(dedupable)
            .sort("_cand_row")
            .unique(
                subset=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
                keep="first",
                maintain_order=True,
            )
        )
        dedup_new = common.filter_new_rows(
            dedup_first,
            existing_keys,
            key_cols=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
            join_nulls=True,
        )

        # Non-dedupable: one person per matched CAND row, no existing-key reuse.
        non_dedup = cand_rows.filter(~dedupable)

        new = pl.concat(
            [dedup_new.select(cand_rows.columns), non_dedup.select(cand_rows.columns)],
            how="vertical_relaxed",
        )

        rows = new.select(
            pl.col("first_name"),
            pl.col("last_name"),
            pl.col("middle_name"),
            pl.col("suffix"),
            pl.col("organization"),
            pl.lit(None, dtype=pl.Utf8).alias("employer"),
            pl.lit(None, dtype=pl.Utf8).alias("occupation"),
            pl.lit(None, dtype=pl.Utf8).alias("job_title"),
            pl.col("person_type"),
            # Candidate persons have no address -> NULL dedup key (matches ORM).
            pl.col("_pk_addr").alias("dedup_addr_key"),
            pl.lit(ctx.state_id).alias("state_id"),
        )
        n_persons = common.write_frame(ctx.session, UnifiedPerson, rows, conflict_cols=None)
        _logger.info("[cand._insert_new_persons] persons written=%d", n_persons)
        return n_persons

    # -- entities -----------------------------------------------------------

    def _insert_new_entities(self, cand_rows: pl.DataFrame, ctx: FamilyContext) -> int:
        """Insert candidate entities whose (entity_type, normalized_name) is not present.

        The entity's representative ``person_id`` / ``address_id`` are NOT set here — they
        are assigned once, deterministically, by ``finalize_entity_representatives`` after
        all families run (a person can map to >1 entity via suffix-variant normalized names,
        so a per-family entity-rep assignment violates the one-to-one
        ``unified_entities.person_id`` unique). Empty normalized names never create an entity
        (mirrors ``_get_or_create_entity`` returning None).
        """
        existing = _entity_id_map(ctx.session, ctx.state_id)
        existing_keys = existing.select(["_ent_type", "_ent_norm"]).unique()

        # First candidate row per entity key (row order) -> the display-name source row.
        first = (
            cand_rows.filter(pl.col("_ent_norm") != "")
            .sort("_cand_row")
            .unique(subset=["_ent_type", "_ent_norm"], keep="first", maintain_order=True)
        )
        new = first.join(
            existing_keys.with_columns(pl.lit(True).alias("_exists")),
            on=["_ent_type", "_ent_norm"],
            how="left",
        ).filter(pl.col("_exists").is_null())

        # Entity name: the stored display name (org else full_name) of the rep row.
        name_expr = (
            pl.when(pl.col("organization").is_not_null())
            .then(pl.col("organization"))
            .otherwise(
                common.full_name_expr(
                    "first_name", "middle_name", "last_name", "suffix", "organization"
                )
            )
        )
        rows = new.with_columns(name_expr.alias("name")).select(
            pl.col("_ent_type").alias("entity_type"),
            pl.col("name"),
            pl.col("_ent_norm").alias("normalized_name"),
            pl.lit(None, dtype=pl.Int64).alias("person_id"),
            pl.lit(None, dtype=pl.Utf8).alias("committee_id"),
            pl.lit(None, dtype=pl.Utf8).alias("notes"),
            pl.lit(ctx.state_id).alias("state_id"),
        )
        return common.write_frame(
            ctx.session,
            UnifiedEntity,
            rows,
            conflict_cols=["entity_type", "normalized_name", "state_id"],
            update_cols=[],
            conflict_where="state_id IS NOT NULL",
        )

    # -- junction -----------------------------------------------------------

    def _write_junction(self, cand_rows: pl.DataFrame, ctx: FamilyContext) -> int:
        """One CANDIDATE junction row per CAND record that matched an expenditure and
        resolved a candidate person. entity_id = the candidate's entity (the entity
        whose (type, normalized_name) the candidate maps to). Deduped on
        (transaction_id, person_id, role).
        """
        person_map = _person_full_id_map(ctx.session, ctx.state_id)
        entity_map = _entity_id_map(ctx.session, ctx.state_id)

        # cand_rows already carries the resolved ``_txn_id`` (expenditure surrogate id).
        rows = (
            cand_rows.join(
                person_map,
                on=["_fk_fn", "_fk_ln", "_fk_org", "_fk_sfx", "_fk_type", "_fk_addr"],
                how="left",
                join_nulls=True,
            )
            .join(
                entity_map.rename({"_ent_id": "_cand_ent_id"}),
                on=["_ent_type", "_ent_norm"],
                how="left",
            )
            .filter(pl.col("_txn_id").is_not_null() & pl.col("_pid").is_not_null())
            .with_columns(pl.lit("CANDIDATE").alias("role"))
            .unique(subset=["_txn_id", "_pid", "role"], keep="first", maintain_order=True)
            .select(
                pl.col("_txn_id").alias("transaction_id"),
                pl.col("_pid").alias("person_id"),
                pl.col("_cand_ent_id").alias("entity_id"),
                pl.lit(ctx.state_id).alias("state_id"),
                pl.lit(None, dtype=pl.Int64).alias("committee_person_id"),
                pl.col("role"),
                pl.lit(None, dtype=pl.Decimal(38, 4)).alias("amount"),
                pl.lit(None, dtype=pl.Utf8).alias("notes"),
            )
        )
        return common.write_frame(
            ctx.session,
            UnifiedTransactionPerson,
            rows,
            conflict_cols=["transaction_id", "person_id", "role"],
            update_cols=[],
        )


register(CandWorker())
