"""Vectorized flat-transactions family: RCPT -> CONTRIBUTION, EXPN -> EXPENDITURE.

Reproduces ``app/core/builders.UnifiedSQLModelBuilder.build_transaction`` +
``scripts/loaders/production_loader._finalize_transaction_for_persist`` columnar
(pure Polars, no map_elements). Gated by diff_snapshots restricted to
``unified_transactions``.

Field mappings (per the Texas unified_field_library + ORM build_transaction):
  RCPT:
    transaction_id   <- contributionInfoId
    amount           <- contributionAmount  (builder_amount)
    transaction_date <- contributionDt      (builder_date, fallback receivedDt)
    description      <- contributionDescr
    transaction_type  = 'CONTRIBUTION'  (SQLAlchemy stores enum.name, uppercase)

  EXPN:
    transaction_id   <- expendInfoId
    amount           <- expendAmount    (builder_amount)
    transaction_date <- expendDt        (builder_date, fallback receivedDt)
    description      <- expendDescr
    transaction_type  = 'EXPENDITURE'

  Both:
    committee_id  <- filerIdent         (_finalize_transaction_for_persist)
    report_ident  <- reportInfoIdent    (build_transaction + _finalize)
    filed_date    <- receivedDt         (builder_date; filedDt/receivedDt both map
                                         to unified 'filed_date', ORM prefers receivedDt
                                         via the field_library mapping for RCPT/EXPN)
    amended        = False              (no amended field in RCPT/EXPN source)
    raw_data       = JSON of orig cols  (json.dumps(raw.copy()) in the ORM)
    state_id       = ctx.state_id
    file_origin_id = None               (no file_origin seeded in vectorized path)
"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import polars as pl

from app.core.ingest_vectorized import common
from app.core.ingest_vectorized.registry import FamilyContext, register
from app.core.models import UnifiedTransaction
from app.logger import Logger

_logger = Logger(__name__)

# ---------------------------------------------------------------------------
# Source column whitelists — every column that appears in TEC RCPT / EXPN
# parquet files (union of both; missing ones are added as null so that
# raw_json_expr covers the correct provenance columns).
# ---------------------------------------------------------------------------

_RCPT_COLS = (
    "recordType",
    "formTypeCd",
    "schedFormTypeCd",
    "reportInfoIdent",
    "receivedDt",
    "infoOnlyFlag",
    "filerIdent",
    "filerTypeCd",
    "filerName",
    "contributionInfoId",
    "contributionDt",
    "contributionAmount",
    "contributionDescr",
    "itemizeFlag",
    "travelFlag",
    "contributorPersentTypeCd",
    "contributorNameOrganization",
    "contributorNameLast",
    "contributorNameSuffixCd",
    "contributorNameFirst",
    "contributorNamePrefixCd",
    "contributorNameShort",
    "contributorStreetCity",
    "contributorStreetStateCd",
    "contributorStreetCountyCd",
    "contributorStreetCountryCd",
    "contributorStreetPostalCode",
    "contributorStreetRegion",
    "contributorEmployer",
    "contributorOccupation",
    "contributorJobTitle",
    "contributorPacFein",
    "contributorOosPacFlag",
    "contributorLawFirmName",
    "contributorSpouseLawFirmName",
    "contributorParent1LawFirmName",
    "contributorParent2LawFirmName",
)

_EXPN_COLS = (
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
    "travelFlag",
    "politicalExpendCd",
    "reimburseIntendedFlag",
    "srcCorpContribFlag",
    "capitalLivingexpFlag",
    "payeePersentTypeCd",
    "payeeNameOrganization",
    "payeeNameLast",
    "payeeNameSuffixCd",
    "payeeNameFirst",
    "payeeNamePrefixCd",
    "payeeNameShort",
    "payeeStreetAddr1",
    "payeeStreetAddr2",
    "payeeStreetCity",
    "payeeStreetStateCd",
    "payeeStreetCountyCd",
    "payeeStreetCountryCd",
    "payeeStreetPostalCode",
    "payeeStreetRegion",
    "creditCardIssuer",
    "repaymentDt",
)


def _read(files: list[Path]) -> pl.DataFrame | None:
    frames = [pl.read_parquet(p) for p in files]
    if not frames:
        return None
    return frames[0] if len(frames) == 1 else pl.concat(frames, how="diagonal_relaxed")


def _ensure_cols(df: pl.DataFrame, names: Iterable[str]) -> pl.DataFrame:
    """Add any referenced source column missing from *df* as a null Utf8 column."""
    missing = [pl.lit(None, dtype=pl.Utf8).alias(n) for n in names if n not in df.columns]
    return df.with_columns(missing) if missing else df


def _transaction_date_expr(date_col: str) -> pl.Expr:
    """builder_date on *date_col*, falling back to receivedDt when null.

    Mirrors builders.build_transaction lines 97-103:
        transaction.transaction_date = self._parse_date(self._get_field_value(...))
        if transaction.transaction_date is None:
            transaction.transaction_date = self._parse_date(raw_data.get("receivedDt"))
    """
    primary = common.builder_date(date_col)
    fallback = common.builder_date("receivedDt")
    return primary.fill_null(fallback)


def _filed_date_expr() -> pl.Expr:
    """Mirror the ORM's filed_date resolution for RCPT/EXPN.

    The field library maps BOTH ``filedDt`` and ``receivedDt`` to the unified
    field ``filed_date``.  build_transaction calls::

        self._get_field_value(raw_data, "filed_date")

    which iterates state field mappings in definition order and returns the
    FIRST matching key found in raw_data.  For RCPT/EXPN parquet the
    ``filedDt`` column does not exist (it is a CVR1-only column); the only
    source column present is ``receivedDt``.  So in practice the ORM always
    resolves filed_date = builder_date(receivedDt) for these record types.
    """
    return common.builder_date("receivedDt")


def _build_transactions(
    df: pl.DataFrame,
    *,
    id_col: str,
    amount_col: str,
    date_col: str,
    descr_col: str,
    transaction_type: str,
    orig_cols: list[str],
    ctx: FamilyContext,
) -> pl.DataFrame:
    """Shared transform for RCPT and EXPN rows into unified_transactions shape."""
    return df.with_columns(
        [
            pl.lit(ctx.state_id).alias("state_id"),
            # transaction_id / description: build_transaction assigns the raw
            # _get_field_value result UNSTRIPPED (no clean_str) — match exactly so a
            # whitespace/empty value isn't normalized to null on the vectorized side.
            pl.col(id_col).cast(pl.Utf8).alias("transaction_id"),
            common.builder_amount(amount_col).alias("amount"),
            _transaction_date_expr(date_col).alias("transaction_date"),
            pl.col(descr_col).cast(pl.Utf8).alias("description"),
            pl.lit(transaction_type).alias("transaction_type"),
            # committee_id: filerIdent (_finalize_transaction_for_persist)
            common.clean_str("filerIdent").alias("committee_id"),
            # report_ident: reportInfoIdent (build_transaction + _finalize)
            common.clean_str("reportInfoIdent").alias("report_ident"),
            _filed_date_expr().alias("filed_date"),
            pl.lit(False).alias("amended"),
            pl.lit(None, dtype=pl.Utf8).alias("file_origin_id"),
            common.raw_json_expr(orig_cols, alias="raw_data"),
        ]
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
        "raw_data",
    )


class FlatTxnsWorker:
    record_types = frozenset({"RCPT", "EXPN"})
    priority = 10  # mirrors _FILE_PRIORITY["RCPT"]

    def run(self, files_by_type: dict[str, list[Path]], ctx: FamilyContext) -> dict[str, int]:
        loaded = 0

        rcpt = _read(files_by_type.get("RCPT", []))
        if rcpt is not None:
            loaded += self._load_rcpt(rcpt, ctx)

        expn = _read(files_by_type.get("EXPN", []))
        if expn is not None:
            loaded += self._load_expn(expn, ctx)

        _logger.info("[vectorized.flat_txns] loaded " + str(loaded) + " transactions")
        return {"loaded": loaded}

    def _load_rcpt(self, df: pl.DataFrame, ctx: FamilyContext) -> int:
        orig_cols = df.columns
        df = _ensure_cols(df, _RCPT_COLS)
        out = _build_transactions(
            df,
            id_col="contributionInfoId",
            amount_col="contributionAmount",
            date_col="contributionDt",
            descr_col="contributionDescr",
            transaction_type="CONTRIBUTION",
            orig_cols=orig_cols,
            ctx=ctx,
        )
        return common.write_frame(ctx.session, UnifiedTransaction, out, conflict_cols=None)

    def _load_expn(self, df: pl.DataFrame, ctx: FamilyContext) -> int:
        orig_cols = df.columns
        df = _ensure_cols(df, _EXPN_COLS)
        out = _build_transactions(
            df,
            id_col="expendInfoId",
            amount_col="expendAmount",
            date_col="expendDt",
            descr_col="expendDescr",
            transaction_type="EXPENDITURE",
            orig_cols=orig_cols,
            ctx=ctx,
        )
        return common.write_frame(ctx.session, UnifiedTransaction, out, conflict_cols=None)


register(FlatTxnsWorker())
