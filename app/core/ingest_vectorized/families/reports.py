"""Vectorized reports family: CVR1 -> unified_reports, FINL -> is_final update.

Reproduces `app/core/source_models/reports_ingest.py::build_report` +
`build_final_report` columnar (pure Polars, no map_elements). Gated by
diff_snapshots restricted to ``unified_reports``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from sqlalchemy import update

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.source_models.reports import UnifiedReport

#: Source columns referenced by the report transform. TEC files omit columns that
#: are always blank, so any missing one is added as null (mirrors raw.get() -> None).
_SOURCE_COLS = (
    "filerIdent",
    "reportInfoIdent",
    "formTypeCd",
    "filedDt",
    "periodStartDt",
    "periodEndDt",
    "totalContribAmount",
    "unitemizedContribAmount",
    "totalExpendAmount",
    "unitemizedExpendAmount",
    "loanBalanceAmount",
    "contribsMaintainedAmount",
    "cashOnHandAmount",
    "filerName",
    "treasNameFirst",
    "treasNameLast",
    "treasPersentTypeCd",
    "treasNameOrganization",
)


def _read(files: list[Path]) -> pl.DataFrame | None:
    frames = [pl.read_parquet(p) for p in files]
    if not frames:
        return None
    return frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")


def _ensure_cols(df: pl.DataFrame, names) -> pl.DataFrame:
    """Add any referenced source column missing from *df* as a null Utf8 column."""
    missing = [pl.lit(None, dtype=pl.Utf8).alias(n) for n in names if n not in df.columns]
    return df.with_columns(missing) if missing else df


def _treasurer_expr() -> pl.Expr:
    """treasPersentTypeCd==ENTITY -> org; else first+last joined skipping blanks."""
    first = common.clean_str("treasNameFirst")
    last = common.clean_str("treasNameLast")
    individual = pl.concat_str([first, last], separator=" ", ignore_nulls=True)
    individual = pl.when(individual.str.len_chars() > 0).then(individual).otherwise(None)
    return (
        pl.when(common.clean_str("treasPersentTypeCd") == "ENTITY")
        .then(common.clean_str("treasNameOrganization"))
        .otherwise(individual)
    )


class ReportsWorker:
    record_types = frozenset({"CVR1", "FINL"})
    priority = 1

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        loaded = 0
        cvr1 = _read(files_by_type.get("CVR1", []))
        if cvr1 is not None:
            loaded += self._load_reports(cvr1, ctx)
        finl = _read(files_by_type.get("FINL", []))
        if finl is not None:
            self._apply_finl(finl, ctx)
        return {"loaded": loaded}

    def _load_reports(self, df: pl.DataFrame, ctx: FamilyContext) -> int:
        orig_cols = df.columns  # raw_data provenance = the ORIGINAL parquet columns
        df = _ensure_cols(df, _SOURCE_COLS)
        out = df.with_columns(
            [
                pl.lit(ctx.state_id).alias("state_id"),
                common.clean_str("filerIdent").alias("committee_id"),
                common.clean_str("reportInfoIdent").alias("report_ident"),
                common.clean_str("formTypeCd").alias("form_type"),
                common.tec_date("filedDt").alias("filed_date"),
                common.tec_date("periodStartDt").alias("period_start"),
                common.tec_date("periodEndDt").alias("period_end"),
                pl.lit(False).alias("is_final"),
                common.tec_amount("totalContribAmount").alias("total_contributions"),
                common.tec_amount("unitemizedContribAmount").alias(
                    "total_unitemized_contributions"
                ),
                common.tec_amount("totalExpendAmount").alias("total_expenditures"),
                common.tec_amount("unitemizedExpendAmount").alias("total_unitemized_expenditures"),
                common.tec_amount("loanBalanceAmount").alias("loan_balance"),
                common.tec_amount("contribsMaintainedAmount").alias("contributions_maintained"),
                common.tec_amount("cashOnHandAmount").alias("cash_on_hand"),
                pl.lit(None).alias("file_origin_id"),
                common.raw_json_expr(orig_cols, alias="raw_data"),
                common.clean_str("filerName").alias("committee_name_at_filing"),
                _treasurer_expr().alias("treasurer_name_at_filing"),
            ]
        ).select(
            "state_id",
            "committee_id",
            "report_ident",
            "form_type",
            "filed_date",
            "period_start",
            "period_end",
            "is_final",
            "total_contributions",
            "total_unitemized_contributions",
            "total_expenditures",
            "total_unitemized_expenditures",
            "loan_balance",
            "contributions_maintained",
            "cash_on_hand",
            "file_origin_id",
            "raw_data",
            "committee_name_at_filing",
            "treasurer_name_at_filing",
        )
        # build_report raises (rejects the row) when reportInfoIdent is missing.
        out = out.filter(pl.col("report_ident").is_not_null())
        return common.write_frame(ctx.session, UnifiedReport, out, conflict_cols=["report_ident"])

    def _apply_finl(self, df: pl.DataFrame, ctx: FamilyContext) -> None:
        """FINL sets is_final=True on the matching report (state_id, report_ident)."""
        idents = (
            df.select(common.clean_str("reportInfoIdent").alias("ri"))
            .filter(pl.col("ri").is_not_null())["ri"]
            .unique()
            .to_list()
        )
        if not idents:
            return
        ctx.session.execute(
            update(UnifiedReport)
            .where(UnifiedReport.state_id == ctx.state_id, UnifiedReport.report_ident.in_(idents))
            .values(is_final=True)
        )
        ctx.session.commit()


register(ReportsWorker())
