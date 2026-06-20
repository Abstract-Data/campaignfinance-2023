"""Transaction frame building and writes for the detail_children family."""

from __future__ import annotations

import polars as pl

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.registry import FamilyContext
from app.core.models import UnifiedTransaction

from .exprs import _cs
from .specs import _SPECS, TypeSpec


def transaction_frame(
    df: pl.DataFrame,
    spec: TypeSpec,
    ctx: FamilyContext,
) -> pl.DataFrame:
    txn_id = (
        pl.col(spec.id_col).cast(pl.Utf8)
        if spec.id_col in df.columns
        else pl.lit(None, dtype=pl.Utf8)
    )

    # amount
    if spec.amount_col and spec.amount_col in df.columns:
        amount = common.builder_amount(spec.amount_col)
    else:
        amount = pl.lit(None, dtype=pl.Decimal(38, 4))
    if spec.record_type == "TRVL":
        amount = amount.fill_null(common.builder_amount("parentAmount"))

    # date
    if spec.date_col and spec.date_col in df.columns:
        date_expr = common.builder_date(spec.date_col)
    else:
        date_expr = pl.lit(None, dtype=pl.Date)
    if spec.date_fallback_received:
        date_expr = date_expr.fill_null(common.builder_date("receivedDt"))

    # description: build_transaction assigns the raw _get_field_value result
    # UNSTRIPPED (no clean_str).
    if spec.descr_col and spec.descr_col in df.columns:
        descr = pl.col(spec.descr_col).cast(pl.Utf8)
    else:
        descr = pl.lit(None, dtype=pl.Utf8)

    return df.with_columns(
        pl.lit(ctx.state_id).alias("state_id"),
        txn_id.alias("transaction_id"),
        amount.alias("amount"),
        date_expr.alias("transaction_date"),
        descr.alias("description"),
        pl.lit(spec.transaction_type).alias("transaction_type"),
        _cs("filerIdent").alias("committee_id"),
        _cs("reportInfoIdent").alias("report_ident"),
        # filed_date: filedDt mapped 1.0, receivedDt 0.9. These files carry only
        # receivedDt (filedDt is CVR1-only), so filed_date = builder_date(receivedDt).
        common.builder_date("receivedDt").alias("filed_date"),
        pl.lit(False).alias("amended"),
        pl.lit(None, dtype=pl.Utf8).alias("file_origin_id"),
        # Campaign source columns — always NULL for LOAN/DEBT/CRED/TRVL/ASSET/PLDG
        # record types (none carry candidateHold*/candidateSeek* fields).
        pl.lit(None, dtype=pl.Utf8).alias("campaign_office_src"),
        pl.lit(None, dtype=pl.Utf8).alias("campaign_district_src"),
        pl.lit(None, dtype=pl.Utf8).alias("campaign_name_src"),
    ).select(
        "state_id",
        "transaction_id",
        "amount",
        "transaction_date",
        "description",
        "transaction_type",
        "committee_id",
        "report_ident",
        "filed_date",
        "amended",
        "file_origin_id",
        "campaign_office_src",
        "campaign_district_src",
        "campaign_name_src",
    )


def write_transactions(
    frames: dict[str, pl.DataFrame],
    ordered: list[str],
    ctx: FamilyContext,
) -> int:
    total = 0
    for rt in ordered:
        out = transaction_frame(frames[rt], _SPECS[rt], ctx)
        total += common.write_frame(ctx.session, UnifiedTransaction, out, conflict_cols=None)
    return total
