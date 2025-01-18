from __future__ import annotations
from typing import Optional, List, Dict, Any, Type, NamedTuple, Self
import pandas as pd
import polars as pl
from polars import DataFrame
from dataclasses import dataclass, field
from icecream import ic


class TexasFieldsBase(NamedTuple):
    PFX: str
    DATE: str
    AMOUNT: str
    TYPE: str
    FILER_ID: str = "filerIdent"
    FILER_NAME: str = "filerName"


TexasExpenseFields = TexasFieldsBase(
    PFX="exp",
    DATE="expendDt",
    AMOUNT="expendAmount",
    TYPE="payee",
)

TexasContributionFields = TexasFieldsBase(
    PFX="con",
    DATE="contributionDt",
    AMOUNT="contributionAmount",
    TYPE="contrib",
)


@dataclass
class TexasSearchSetup:
    DATA: pl.LazyFrame
    COLS: Optional[Dict[str, List[str]]] = None
    NAME_ORG: Any = None
    amount_: Optional[str] = None
    type_: Optional[TexasFieldsBase] = None
    filer_id_: Optional[str] = "filerIdent"
    filer_name_: Optional[str] = "filerName"
    search_field_: Optional[str] = None

    def __post_init__(self):
        self._sort_fields()
        self._recast_sorted_field_types()
        self._coalesce_names()

    def _sort_fields(self):
        _cols = self.DATA.collect_schema().names()
        self.COLS = {
            'date': [x for x in _cols if x.endswith('Dt')],
            'identity': [x for x in _cols if x.endswith('Ident')],
            'amount': [x for x in _cols if x.endswith('Amount')],
            'all': _cols,
        }
        return self.COLS

    def _recast_sorted_field_types(self):
        self.DATA = self.DATA.with_columns(
            [pl.col(col).str.strptime(pl.Date, '%Y%m%d', strict=False).alias(col) for col in self.COLS['date']]
            + [pl.col(col).cast(pl.Int32).alias(col) for col in self.COLS['identity']]
            + [pl.col(col).cast(pl.Float64).alias(col) for col in self.COLS['amount']],
            )
        _pfx = self.DATA.first().collect().get_column('file_origin')[0]
        if TexasExpenseFields.PFX in _pfx:
            self.type_ = TexasExpenseFields
        elif TexasContributionFields.PFX in _pfx:
            self.type_ = TexasContributionFields
        else:
            raise ValueError("Could not determine type of data")

        _dt_field = next((x for x in self.COLS['date'] if x.endswith('Dt') and self.type_.PFX in x), None)
        if _dt_field:
            self.DATA = self.DATA.with_columns(
                pl.col(_dt_field).dt.year().alias('year')
            )
        return self

    def _coalesce_names(self):
        _fields = [x for x in self.COLS['all'] if self.type_.TYPE in x]
        ic(_fields)
        _first = next((x for x in _fields if 'First' in x), None)
        _last = next((x for x in _fields if 'Last' in x), None)
        _org = next((x for x in _fields if 'Organization' in x), None)
        self.search_field_ = _first.replace('First', "")

        FIRST_AND_LAST = pl.format(
            "{} {}", pl.col(_first), pl.col(_last))
        self.NAME_ORG = pl.coalesce(
            FIRST_AND_LAST, pl.col(_org)).alias(self.search_field_)
        self.DATA = self.DATA.with_columns(self.NAME_ORG)
        return self


@dataclass
class TexasSearch:
    data: Optional[pl.LazyFrame] = None
    config: TexasSearchSetup = None
    results: dict = field(default_factory=dict)

    def __post_init__(self):
        self.config = TexasSearchSetup(self.data)
        self.data = self.config.DATA


    def search(self, search_term: str) -> Self:
        _result = (
            self.data
            .filter(
                pl.col(self.config.search_field_)
                .str
                .contains(search_term))
           .collect())
        _unique_filers = (
            _result
            .group_by(
                pl.col(self.config.filer_id_))
            .agg(
                pl.col(self.config.type_.FILER_NAME)
                .first()
                .alias(self.config.type_.FILER_NAME)
            ))

        self.results['origin'] = _result
        self.results['unique_filers'] = _unique_filers

        return self

    def group_by_year(self) -> DataFrame:
        _df = self.results['origin']
        _result = (
            _df
            .group_by(
                pl.col('filerIdent'),
                self.config.NAME_ORG,
                pl.col(self.config.type_.DATE).dt.year().cast(pl.String).alias('year'))
            .agg(
                pl.col(self.config.type_.AMOUNT).cast(pl.Float64).alias('total').sum().round())
            .to_pandas())

        _merge_uniques = (
            self.results['unique_filers']
            .to_pandas()
            .merge(_result, on='filerIdent', how='left'))

        _ct = pd.crosstab(
            index=[
                _merge_uniques['filerIdent'],
                _merge_uniques['filerName'],
                _merge_uniques[self.config.search_field_]],
            columns=_merge_uniques['year'],
            values=_merge_uniques['total'],
            aggfunc='sum',
            margins=True,
            margins_name='total',
            dropna=True
        )
        self.results['grouped'] = _ct
        return _ct
