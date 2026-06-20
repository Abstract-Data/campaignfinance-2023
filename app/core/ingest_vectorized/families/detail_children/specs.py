"""Per-type static configuration for the detail_children family.

TypeSpec describes how one TEC record type (LOAN, DEBT, PLDG, CRED, TRVL, ASSET)
maps onto unified rows.  All instances are module-level singletons; nothing here
performs I/O or computation beyond the dataclass constructors.
"""

from __future__ import annotations

from dataclasses import dataclass, field

_PLACEHOLDER_NAMES_UPPER = frozenset(
    {"NON-ITEMIZED CONTRIBUTOR", "NON-ITEMIZED", "UNKNOWN", "ANONYMOUS"}
)


@dataclass(frozen=True)
class TypeSpec:
    """Static description of how one TEC record type maps onto unified rows."""

    record_type: str
    transaction_type: str
    id_col: str  # source column resolved to transaction_id
    amount_col: str | None  # source column resolved to amount (None => null)
    date_col: str | None  # source column resolved to transaction_date
    date_fallback_received: bool  # fall back to receivedDt when date_col null/absent
    descr_col: str | None
    # Role-prefixed person columns ({prefix}NameFirst/Last/Organization/SuffixCd).
    name_first: str | None = None
    name_last: str | None = None
    name_org: str | None = None
    name_suffix: str | None = None
    # Role-prefixed address columns (None when the role has no mapped address).
    addr_city: str | None = None
    addr_state: str | None = None
    addr_zip: str | None = None
    addr_country: str | None = None
    addr_county: str | None = None
    # Priority for load order (mirrors production_loader._FILE_PRIORITY).
    priority: int = 50
    #: Extra source columns referenced (so they are nulled-in when absent).
    extra_cols: tuple[str, ...] = field(default_factory=tuple)


_GUARANTOR_COLS = (
    "guarantorPersentTypeCd",
    "guarantorNameOrganization",
    "guarantorNameLast",
    "guarantorNameSuffixCd",
    "guarantorNameFirst",
    "guarantorNamePrefixCd",
    "guarantorStreetCity",
    "guarantorStreetStateCd",
    "guarantorStreetCountyCd",
    "guarantorStreetCountryCd",
    "guarantorStreetPostalCode",
    "guarantorStreetRegion",
)


def _guarantor_source_cols() -> tuple[str, ...]:
    return tuple(f"{base}{i}" for i in range(1, 6) for base in _GUARANTOR_COLS)


_LOAN = TypeSpec(
    record_type="LOAN",
    transaction_type="LOAN",
    id_col="loanInfoId",
    amount_col="loanAmount",
    date_col="loanDt",
    date_fallback_received=True,
    descr_col="loanDescr",
    name_first="lenderNameFirst",
    name_last="lenderNameLast",
    name_org="lenderNameOrganization",
    name_suffix="lenderNameSuffixCd",
    addr_city="lenderStreetCity",
    addr_state="lenderStreetStateCd",
    addr_zip="lenderStreetPostalCode",
    addr_country="lenderStreetCountryCd",
    addr_county="lenderStreetCountyCd",
    priority=12,
    extra_cols=("interestRate", "maturityDt", "collateralDescr") + _guarantor_source_cols(),
)

_DEBT = TypeSpec(
    record_type="DEBT",
    transaction_type="DEBT",
    id_col="loanInfoId",
    amount_col=None,  # debts fixture has no loanAmount/debtAmount column
    date_col=None,
    date_fallback_received=True,
    descr_col=None,
    name_first="lenderNameFirst",
    name_last="lenderNameLast",
    name_org="lenderNameOrganization",
    name_suffix="lenderNameSuffixCd",
    addr_city="lenderStreetCity",
    addr_state="lenderStreetStateCd",
    addr_zip="lenderStreetPostalCode",
    addr_country="lenderStreetCountryCd",
    addr_county="lenderStreetCountyCd",
    priority=13,
    extra_cols=("loanGuaranteedFlag", "loanGuaranteeAmount") + _guarantor_source_cols(),
)

_PLDG = TypeSpec(
    record_type="PLDG",
    transaction_type="PLEDGE",
    id_col="pledgeInfoId",
    amount_col="pledgeAmount",
    date_col="pledgeDt",
    date_fallback_received=False,
    descr_col="pledgeDescr",
    name_first="pledgerNameFirst",
    name_last="pledgerNameLast",
    name_org="pledgerNameOrganization",
    name_suffix="pledgerNameSuffixCd",
    addr_city="pledgerStreetCity",
    addr_state="pledgerStreetStateCd",
    addr_zip="pledgerStreetPostalCode",
    addr_country="pledgerStreetCountryCd",
    addr_county="pledgerStreetCountyCd",
    priority=14,
)

_CRED = TypeSpec(
    record_type="CRED",
    transaction_type="CREDIT",
    id_col="creditInfoId",
    amount_col="creditAmount",
    date_col="creditDt",
    date_fallback_received=False,
    descr_col="creditDescr",
    name_first="payorNameFirst",
    name_last="payorNameLast",
    name_org="payorNameOrganization",
    name_suffix="payorNameSuffixCd",
    # payor has NO mapped address columns in the field library -> no address.
    priority=15,
)

_TRVL = TypeSpec(
    record_type="TRVL",
    transaction_type="TRAVEL",
    id_col="travelInfoId",
    amount_col=None,  # travel rows carry the value on parentAmount (amount fallback)
    date_col="parentDt",
    date_fallback_received=False,
    descr_col=None,
    name_first="travellerNameFirst",
    name_last="travellerNameLast",
    name_org="travellerNameOrganization",
    name_suffix="travellerNameSuffixCd",
    priority=16,
    extra_cols=(
        "parentType",
        "parentId",
        "parentAmount",
        "parentFullName",
        "transportationTypeCd",
        "transportationTypeDescr",
        "departureCity",
        "arrivalCity",
        "departureDt",
        "arrivalDt",
        "travelPurpose",
    ),
)

_ASSET = TypeSpec(
    record_type="ASSET",
    transaction_type="ASSET",
    id_col="assetInfoId",
    amount_col=None,
    date_col=None,
    date_fallback_received=True,
    descr_col="assetDescr",  # assetDescr -> description (0.9) and asset_descr (1.0)
    priority=17,
    extra_cols=("assetDescr",),
)

_SPECS: dict[str, TypeSpec] = {
    s.record_type: s for s in (_LOAN, _DEBT, _PLDG, _CRED, _TRVL, _ASSET)
}

# Common columns every TEC transaction file carries.
_BASE_COLS = (
    "recordType",
    "formTypeCd",
    "schedFormTypeCd",
    "reportInfoIdent",
    "receivedDt",
    "infoOnlyFlag",
    "filerIdent",
    "filerTypeCd",
    "filerName",
)
