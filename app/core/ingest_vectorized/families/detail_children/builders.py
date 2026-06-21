"""Detail-child row builders for the detail_children family.

Functions here are module-level (not methods); ``worker`` is passed explicitly
where ``worker._addr_lookup`` or ``worker._orig_cols`` is required.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.id_maps import (
    committee_entity_map as _committee_entity_map,
)
from app.core.ingest_vectorized.id_maps import (
    entity_id_map as _entity_id_map,
)
from app.core.ingest_vectorized.id_maps import (
    guarantor_key_frame as _guarantor_key_frame,
)
from app.core.ingest_vectorized.id_maps import (
    loan_pk_map as _loan_pk_map,
)
from app.core.ingest_vectorized.id_maps import (
    person_id_map as _person_id_map,
)
from app.core.ingest_vectorized.id_maps import (
    txn_id_map as _txn_id_map,
)
from app.core.ingest_vectorized.registry import FamilyContext
from app.core.models import (
    LoanGuarantor,
    UnifiedAsset,
    UnifiedCredit,
    UnifiedDebt,
    UnifiedLoan,
    UnifiedTravel,
)
from app.core.source_models.pledges import UnifiedPledge
from app.logger import Logger

from .exprs import (
    _cs,
    _full_name,
    _get_unstripped,
    _guar,
    _norm_name,
    _opt_col,
    _pledge_date_expr,
)
from .specs import _SPECS, TypeSpec

_logger = Logger(__name__)

if TYPE_CHECKING:
    from .worker import DetailChildrenWorker


# ---------------------------------------------------------------------------
# Per-row party key helpers
# ---------------------------------------------------------------------------


def _party_keys(
    worker: DetailChildrenWorker,
    df: pl.DataFrame,
    spec: TypeSpec,
) -> pl.DataFrame:
    """Attach the per-row party dedup keys + parent transaction id to *df*."""
    first = _opt_col(df, spec.name_first)
    last = _opt_col(df, spec.name_last)
    org = _opt_col(df, spec.name_org)
    city = _opt_col(df, spec.addr_city)
    state = (
        _opt_col(df, spec.addr_state).str.to_uppercase()
        if spec.addr_state
        else pl.lit(None, dtype=pl.Utf8)
    )
    zip_code = _opt_col(df, spec.addr_zip)
    txn_id = (
        pl.col(spec.id_col).cast(pl.Utf8)
        if spec.id_col in df.columns
        else pl.lit(None, dtype=pl.Utf8)
    )
    # Resolve the inherited street via the SAME omit-null match the dim layer used
    # (worker._addr_lookup, built once in run()), so this detail->person key matches the
    # enriched person stored by write_dims. Materialize city/state/zip as columns for
    # add_resolved_street, then key on the resolved street.
    keyed = df.with_columns(
        city.alias("_rc_city"),
        state.alias("_rc_state"),
        zip_code.alias("_rc_zip"),
    )
    keyed = common.add_resolved_street(
        keyed,
        worker._addr_lookup,
        city_col="_rc_city",
        state_col="_rc_state",
        zip_col="_rc_zip",
        out_col="_res_street",
    )
    return common.collapse_org_person_key(
        keyed.with_columns(
            org.str.to_lowercase().alias("_pk_org"),
            first.str.to_lowercase().alias("_pk_fn"),
            last.str.to_lowercase().alias("_pk_ln"),
            # Address dimension of the individual key — uses the inherited street so it
            # matches the dim-layer person. collapse_org_person_key nulls it for orgs.
            common.person_addr_key_expr(
                pl.col("_res_street"),
                pl.col("_rc_city"),
                pl.col("_rc_state"),
                pl.col("_rc_zip"),
            ).alias("_pk_addr"),
            _full_name(first, last, _opt_col(df, spec.name_suffix), org).alias("_full_name"),
            txn_id.alias("_txn_id"),
        )
    ).drop("_rc_city", "_rc_state", "_rc_zip", "_res_street")


def _join_party_entity(
    keyed: pl.DataFrame,
    person_map: pl.DataFrame,
    entity_map: pl.DataFrame,
) -> pl.DataFrame:
    """Resolve party -> person id -> entity id (PERSON or ORGANIZATION entity)."""
    joined = keyed.join(
        person_map,
        on=["_pk_org", "_pk_fn", "_pk_ln", "_pk_addr"],
        how="left",
        join_nulls=True,
    )
    # The party's entity normalized_name == normalize(org) for orgs else
    # normalize(full_name); entity_type ORGANIZATION vs PERSON.
    joined = joined.with_columns(
        pl.when(pl.col("_pk_org").is_not_null())
        .then(pl.lit("ORGANIZATION"))
        .otherwise(pl.lit("PERSON"))
        .alias("_party_etype"),
        _norm_name(pl.col("_full_name")).alias("_party_nname"),
    )
    emap = entity_map.rename(
        {
            "entity_id": "_party_entity_id",
            "entity_type": "_party_etype",
            "normalized_name": "_party_nname",
        }
    )
    return joined.join(emap, on=["_party_etype", "_party_nname"], how="left")


def _join_txn(
    df: pl.DataFrame,
    txn_map: pl.DataFrame,
    ttype: str,
) -> pl.DataFrame:
    tmap = txn_map.filter(pl.col("transaction_type") == ttype).select(
        pl.col("transaction_id").alias("_txn_id"), pl.col("txn_pk")
    )
    return df.join(tmap, on="_txn_id", how="left")


def _committee_entity_expr(
    df: pl.DataFrame,
    committee_entity: dict[str, int],
) -> pl.DataFrame:
    filer = _cs("filerIdent")
    mapping = pl.DataFrame(
        {
            "_filer": list(committee_entity.keys()),
            "_committee_entity_id": list(committee_entity.values()),
        },
        schema={"_filer": pl.Utf8, "_committee_entity_id": pl.Int64},
    )
    return df.with_columns(filer.alias("_filer")).join(mapping, on="_filer", how="left")


# ---------------------------------------------------------------------------
# Per-type detail child builders
# ---------------------------------------------------------------------------


def _loan_date_expr(spec: TypeSpec, df: pl.DataFrame) -> pl.Expr:
    if spec.date_col and spec.date_col in df.columns:
        d = common.builder_date(spec.date_col)
    else:
        d = pl.lit(None, dtype=pl.Date)
    if spec.date_fallback_received:
        d = d.fill_null(common.builder_date("receivedDt"))
    return d


def _build_loan(
    worker: DetailChildrenWorker,
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    entity_map: pl.DataFrame,
    txn_map: pl.DataFrame,
    committee_entity: dict[str, int],
) -> int:
    keyed = _party_keys(worker, df, spec)
    keyed = _join_party_entity(keyed, _person_id_map(ctx.engine, ctx.state_id), entity_map)
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    keyed = _committee_entity_expr(keyed, committee_entity)
    # ORM skips loans with no lender entity or no borrower (committee) entity.
    out = (
        keyed.filter(
            pl.col("_party_entity_id").is_not_null()
            & pl.col("_committee_entity_id").is_not_null()
            & pl.col("txn_pk").is_not_null()
        )
        .with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.col("_party_entity_id").alias("lender_entity_id"),
            pl.col("_committee_entity_id").alias("borrower_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            common.builder_amount(spec.amount_col).alias("amount")
            if spec.amount_col in df.columns
            else pl.lit(None, dtype=pl.Decimal(38, 4)).alias("amount"),
            _loan_date_expr(spec, df).alias("loan_date"),
            common.builder_date("maturityDt").alias("due_date"),
            common.builder_amount("interestRate").alias("interest_rate"),
            _get_unstripped(df, "collateralDescr").alias("collateral"),
        )
        .select(
            "transaction_id",
            "lender_entity_id",
            "borrower_entity_id",
            "state_id",
            "amount",
            "loan_date",
            "due_date",
            "interest_rate",
            "collateral",
        )
    )
    return common.write_frame(
        ctx.session,
        UnifiedLoan,
        out,
        conflict_cols=["transaction_id"],
        update_cols=[],
    )


def _build_debt(
    worker: DetailChildrenWorker,
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    entity_map: pl.DataFrame,
    txn_map: pl.DataFrame,
    committee_entity: dict[str, int],
) -> int:
    keyed = _party_keys(worker, df, spec)
    keyed = _join_party_entity(keyed, _person_id_map(ctx.engine, ctx.state_id), entity_map)
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    keyed = _committee_entity_expr(keyed, committee_entity)
    # ORM skips debts with no creditor entity. debtor falls back to committee
    # entity, else to creditor entity.
    debtor = pl.coalesce([pl.col("_committee_entity_id"), pl.col("_party_entity_id")])
    # amount is None for debts (no amount column); original_amount = parse or amount.
    out = (
        keyed.filter(pl.col("_party_entity_id").is_not_null() & pl.col("txn_pk").is_not_null())
        .with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.col("_party_entity_id").alias("creditor_entity_id"),
            debtor.alias("debtor_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("amount"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("original_amount"),
            _loan_date_expr(spec, df).alias("debt_date"),
            pl.lit(None, dtype=pl.Date).alias("due_date"),
            pl.lit(None, dtype=pl.Utf8).alias("description"),
            common.bool_expr("loanGuaranteedFlag").alias("is_guaranteed"),
            pl.lit(None, dtype=pl.Utf8).alias("guarantor_name"),
            common.builder_amount("loanGuaranteeAmount").alias("guarantee_amount")
            if "loanGuaranteeAmount" in df.columns
            else pl.lit(None, dtype=pl.Decimal(38, 4)).alias("guarantee_amount"),
            pl.lit(False).alias("is_paid"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("payment_amount"),
            pl.lit(None, dtype=pl.Date).alias("payment_date"),
        )
        .select(
            "transaction_id",
            "creditor_entity_id",
            "debtor_entity_id",
            "state_id",
            "amount",
            "original_amount",
            "debt_date",
            "due_date",
            "description",
            "is_guaranteed",
            "guarantor_name",
            "guarantee_amount",
            "is_paid",
            "payment_amount",
            "payment_date",
        )
    )
    return common.write_frame(
        ctx.session,
        UnifiedDebt,
        out,
        conflict_cols=["transaction_id"],
        update_cols=[],
    )


def _build_credit(
    worker: DetailChildrenWorker,
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    entity_map: pl.DataFrame,
    txn_map: pl.DataFrame,
    committee_entity: dict[str, int],
) -> int:
    keyed = _party_keys(worker, df, spec)
    keyed = _join_party_entity(keyed, _person_id_map(ctx.engine, ctx.state_id), entity_map)
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    keyed = _committee_entity_expr(keyed, committee_entity)
    recipient = pl.coalesce([pl.col("_committee_entity_id"), pl.col("_party_entity_id")])
    out = (
        keyed.filter(pl.col("_party_entity_id").is_not_null() & pl.col("txn_pk").is_not_null())
        .with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.col("_party_entity_id").alias("payor_entity_id"),
            recipient.alias("recipient_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            common.builder_amount(spec.amount_col).alias("amount"),
            common.builder_date(spec.date_col).alias("credit_date"),
            pl.lit(None, dtype=pl.Utf8).alias("credit_type"),
            _get_unstripped(df, spec.descr_col).alias("description"),
            pl.lit(None, dtype=pl.Utf8).alias("related_transaction_id"),
        )
        .select(
            "transaction_id",
            "payor_entity_id",
            "recipient_entity_id",
            "state_id",
            "amount",
            "credit_date",
            "credit_type",
            "description",
            "related_transaction_id",
        )
    )
    return common.write_frame(
        ctx.session,
        UnifiedCredit,
        out,
        conflict_cols=["transaction_id"],
        update_cols=[],
    )


def _build_travel(
    worker: DetailChildrenWorker,
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    txn_map: pl.DataFrame,
) -> int:
    keyed = _party_keys(worker, df, spec)
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    # traveler_person_id is ALWAYS None: TRVL maps the traveller into the PAYEE
    # role (RECORD_TYPE_ROLE_MAP), but _build_travel_detail reads ctx["contributor"]
    # — which is None for TRVL — so traveler is never linked.
    descr = (
        pl.col(spec.descr_col).cast(pl.Utf8)
        if spec.descr_col and spec.descr_col in df.columns
        else pl.lit(None, dtype=pl.Utf8)
    )
    # traveler_name <- traveler_name (no source col) or parent_full_name.
    traveler_name = _get_unstripped(df, "parentFullName")
    out = (
        keyed.filter(pl.col("txn_pk").is_not_null())
        .with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.lit(None, dtype=pl.Int64).alias("traveler_person_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            _get_unstripped(df, "parentType").alias("parent_transaction_type"),
            _get_unstripped(df, "parentId").alias("parent_transaction_id"),
            common.builder_amount("parentAmount").alias("parent_amount"),
            common.builder_amount("parentAmount").alias("amount"),
            common.builder_date(spec.date_col).alias("travel_date"),
            _get_unstripped(df, "transportationTypeCd").alias("transportation_type"),
            _get_unstripped(df, "transportationTypeDescr").alias("transportation_description"),
            _get_unstripped(df, "departureCity").alias("departure_city"),
            pl.lit(None, dtype=pl.Utf8).alias("departure_state"),
            _get_unstripped(df, "arrivalCity").alias("arrival_city"),
            pl.lit(None, dtype=pl.Utf8).alias("arrival_state"),
            common.builder_date("departureDt").alias("departure_date"),
            common.builder_date("arrivalDt").alias("arrival_date"),
            pl.coalesce([_get_unstripped(df, "travelPurpose"), descr]).alias("travel_purpose"),
            traveler_name.alias("traveler_name"),
        )
        .select(
            "transaction_id",
            "traveler_person_id",
            "state_id",
            "parent_transaction_type",
            "parent_transaction_id",
            "parent_amount",
            "amount",
            "travel_date",
            "transportation_type",
            "transportation_description",
            "departure_city",
            "departure_state",
            "arrival_city",
            "arrival_state",
            "departure_date",
            "arrival_date",
            "travel_purpose",
            "traveler_name",
        )
    )
    return common.write_frame(
        ctx.session,
        UnifiedTravel,
        out,
        conflict_cols=["transaction_id"],
        update_cols=[],
    )


def _build_asset(
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    txn_map: pl.DataFrame,
) -> int:
    keyed = df.with_columns(
        (
            pl.col(spec.id_col).cast(pl.Utf8)
            if spec.id_col in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        ).alias("_txn_id")
    )
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    # description <- assetDescr (0.9) ; transaction.description set; detail
    # description = transaction.description or asset_descr.
    descr = _get_unstripped(df, "assetDescr")
    date_expr = common.builder_date("receivedDt")  # acquisition_date = txn date (fallback)
    out = (
        keyed.filter(pl.col("txn_pk").is_not_null())
        .with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            _cs("filerIdent").alias("committee_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            pl.lit(None, dtype=pl.Utf8).alias("asset_type"),
            descr.alias("description"),
            date_expr.alias("acquisition_date"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("acquisition_cost"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("current_value"),
            pl.lit(None, dtype=pl.Date).alias("valuation_date"),
            pl.lit(None, dtype=pl.Date).alias("disposition_date"),
            pl.lit(None, dtype=pl.Decimal(38, 4)).alias("disposition_amount"),
            pl.lit(False).alias("is_disposed"),
        )
        .select(
            "transaction_id",
            "committee_id",
            "state_id",
            "asset_type",
            "description",
            "acquisition_date",
            "acquisition_cost",
            "current_value",
            "valuation_date",
            "disposition_date",
            "disposition_amount",
            "is_disposed",
        )
    )
    return common.write_frame(
        ctx.session,
        UnifiedAsset,
        out,
        conflict_cols=["transaction_id"],
        update_cols=[],
    )


def _build_pledge(
    worker: DetailChildrenWorker,
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    txn_map: pl.DataFrame,
) -> int:
    keyed = df.with_columns(
        (
            pl.col(spec.id_col).cast(pl.Utf8)
            if spec.id_col in df.columns
            else pl.lit(None, dtype=pl.Utf8)
        ).alias("_txn_id")
    )
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    orig_cols = worker._orig_cols.get(spec.record_type, list(df.columns))
    # build_pledge: pledgor/recipient entity ids = None (loader passes None).
    # amount = pledge_amount(pledgeAmount) or txn.amount (== pledgeAmount here).
    # pledge_date = pledgeDt or txn.transaction_date. description = pledgeDescr.
    amount = common.builder_amount("pledgeAmount")
    pdate = _pledge_date_expr("pledgeDt")
    descr = _get_unstripped(df, "pledgeDescr")
    out = (
        keyed.filter(pl.col("txn_pk").is_not_null())
        .with_columns(
            pl.col("txn_pk").alias("transaction_id"),
            pl.lit(None, dtype=pl.Int64).alias("pledgor_entity_id"),
            pl.lit(None, dtype=pl.Int64).alias("recipient_entity_id"),
            pl.lit(ctx.state_id).alias("state_id"),
            amount.alias("amount"),
            pdate.alias("pledge_date"),
            pl.lit(False).alias("is_fulfilled"),
            descr.alias("description"),
            common.raw_json_expr(orig_cols, alias="metadata_json"),
        )
        .select(
            "transaction_id",
            "pledgor_entity_id",
            "recipient_entity_id",
            "state_id",
            "amount",
            "pledge_date",
            "is_fulfilled",
            "description",
            "metadata_json",
        )
    )
    return common.write_frame(
        ctx.session,
        UnifiedPledge,
        out,
        conflict_cols=["transaction_id"],
        update_cols=[],
    )


# ---------------------------------------------------------------------------
# Guarantors
# ---------------------------------------------------------------------------


def _guarantor_rows(
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
    txn_map: pl.DataFrame,
    pk_map: dict[int, int],
    parent_col: str,
) -> pl.DataFrame | None:
    # Map each row to its parent detail surrogate id via transaction id.
    txn_id = (
        pl.col(spec.id_col).cast(pl.Utf8)
        if spec.id_col in df.columns
        else pl.lit(None, dtype=pl.Utf8)
    )
    keyed = df.with_columns(txn_id.alias("_txn_id"))
    keyed = _join_txn(keyed, txn_map, spec.transaction_type)
    # txn_pk -> detail pk
    pk_frame = pl.DataFrame(
        {"txn_pk": list(pk_map.keys()), "_detail_pk": list(pk_map.values())},
        schema={"txn_pk": pl.Int64, "_detail_pk": pl.Int64},
    )
    keyed = keyed.join(pk_frame, on="txn_pk", how="left")

    # Build one struct list per slot 1..5, then explode.
    slot_structs = []
    for i in range(1, 6):
        slot_structs.append(
            pl.struct(
                [
                    pl.lit(i, dtype=pl.Int64).alias("position"),
                    _guar(df, f"guarantorPersentTypeCd{i}", 30).alias("person_type"),
                    _guar(df, f"guarantorNameOrganization{i}", 200).alias("organization"),
                    _guar(df, f"guarantorNameLast{i}", 100).alias("last_name"),
                    _guar(df, f"guarantorNameFirst{i}", 100).alias("first_name"),
                    _guar(df, f"guarantorNameSuffixCd{i}", 30).alias("suffix"),
                    _guar(df, f"guarantorNamePrefixCd{i}", 30).alias("prefix"),
                    _guar(df, f"guarantorStreetCity{i}", 100).alias("city"),
                    _guar(df, f"guarantorStreetStateCd{i}", 2).alias("state_code"),
                    _guar(df, f"guarantorStreetCountyCd{i}", 10).alias("county"),
                    _guar(df, f"guarantorStreetCountryCd{i}", 3).alias("country"),
                    _guar(df, f"guarantorStreetPostalCode{i}", 20).alias("postal_code"),
                    _guar(df, f"guarantorStreetRegion{i}", 50).alias("region"),
                ]
            )
        )
    keyed = keyed.with_columns(pl.concat_list(slot_structs).alias("_slots")).explode("_slots")
    keyed = keyed.unnest("_slots")
    # Emit a slot only when last/first/org present (mirrors _build_guarantors).
    keyed = keyed.filter(
        pl.col("last_name").is_not_null()
        | pl.col("first_name").is_not_null()
        | pl.col("organization").is_not_null()
    )
    keyed = keyed.filter(pl.col("_detail_pk").is_not_null())
    if keyed.height == 0:
        return None
    out = keyed.with_columns(
        pl.col("_detail_pk").alias(parent_col),
        pl.lit(None, dtype=pl.Int64).alias("debt_id" if parent_col == "loan_id" else "loan_id"),
        pl.lit(None, dtype=pl.Int64).alias("entity_id"),
    ).select(
        "loan_id",
        "debt_id",
        "entity_id",
        "position",
        "person_type",
        "organization",
        "last_name",
        "first_name",
        "suffix",
        "prefix",
        "city",
        "state_code",
        "county",
        "country",
        "postal_code",
        "region",
    )
    return out


_GUARANTOR_NATURAL_KEYS = ["loan_id", "debt_id", "last_name", "first_name", "organization"]


def _build_guarantors(
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
    ctx: FamilyContext,
) -> int:
    total = 0
    loan_pk = _loan_pk_map(ctx.engine, "unified_loans") if "LOAN" in frames else {}
    debt_pk = _loan_pk_map(ctx.engine, "unified_debts") if "DEBT" in frames else {}
    want_types = frozenset(_SPECS[rt].transaction_type for rt in ordered if rt in _SPECS)
    txn_map = _txn_id_map(ctx.engine, ctx.state_id, want_types)

    # Bucket C anti-join via filter_new_rows: loan_id and debt_id are mutually exclusive
    # (one is always NULL), so join_nulls=True ensures NULL-to-NULL matches correctly.
    existing_raw = _guarantor_key_frame(ctx.engine)

    for rt, parent_table, pk_map in (
        ("LOAN", "loan_id", loan_pk),
        ("DEBT", "debt_id", debt_pk),
    ):
        if rt not in frames:
            continue
        spec = _SPECS[rt]
        df = frames[rt]
        rows = _guarantor_rows(df, spec, ctx, txn_map, pk_map, parent_table)
        if rows is None or not rows.height:
            continue
        new_rows = common.filter_new_rows(
            rows,
            existing_raw,
            key_cols=_GUARANTOR_NATURAL_KEYS,
            normalize_lower=["last_name", "first_name", "organization"],
            join_nulls=True,
        )
        if new_rows.height:
            total += common.write_frame(ctx.session, LoanGuarantor, new_rows, conflict_cols=None)
    return total


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------


def write_details(
    worker: DetailChildrenWorker,
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
    ctx: FamilyContext,
) -> dict[str, int]:
    engine = ctx.engine
    entity_map = _entity_id_map(engine, ctx.state_id)
    want_types = frozenset(_SPECS[rt].transaction_type for rt in ordered if rt in _SPECS)
    txn_map = _txn_id_map(engine, ctx.state_id, want_types)
    committee_entity = _committee_entity_map(engine, ctx.state_id)

    counts: dict[str, int] = {}
    for rt in ordered:
        spec = _SPECS[rt]
        df = frames[rt]
        if rt == "LOAN":
            counts["loans"] = _build_loan(
                worker, df, spec, ctx, entity_map, txn_map, committee_entity
            )
        elif rt == "DEBT":
            counts["debts"] = _build_debt(
                worker, df, spec, ctx, entity_map, txn_map, committee_entity
            )
        elif rt == "CRED":
            counts["credits"] = _build_credit(
                worker, df, spec, ctx, entity_map, txn_map, committee_entity
            )
        elif rt == "TRVL":
            counts["travel"] = _build_travel(worker, df, spec, ctx, txn_map)
        elif rt == "ASSET":
            counts["assets"] = _build_asset(df, spec, ctx, txn_map)
        elif rt == "PLDG":
            counts["pledges"] = _build_pledge(worker, df, spec, ctx, txn_map)

    # Guarantors depend on loan/debt surrogate ids (written above).
    counts["loan_guarantors"] = _build_guarantors(frames, ordered, ctx)
    return counts
