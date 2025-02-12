from __future__ import annotations
from typing import Optional, List, Dict, Any, Type, NamedTuple, Self
import pandas as pd
import polars as pl
from polars import DataFrame
from dataclasses import dataclass, field
from icecream import ic
import time


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
        self.DATA = (
            self.DATA
            .with_columns(
                [pl.col(col)
                .str
                .strptime(pl.Date, '%Y%m%d', strict=False)
                .alias(col) for col in self.COLS['date']] +
                [pl.col(col)
                .cast(pl.Int32)
                .alias(col) for col in self.COLS['identity']] +
                [pl.col(col)
                .cast(pl.Float64)
                .alias(col) for col in self.COLS['amount']],
            )
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
class TexasSearchResult:
    input_terms: tuple
    config: TexasSearchSetup
    data: pd.DataFrame | DataFrame = None
    unique_filers: pd.DataFrame = None
    unique_filters: dict = field(default_factory=dict)
    selected_choices: set = field(default_factory=set)
    filtered: Optional[pd.DataFrame] = None
    grouped: Optional[pd.DataFrame] = None

    def __repr__(self):
        return f"TexasSearchResult({', '.join(self.input_terms)})"

    def group_by_year(self) -> pd.DataFrame:
        _df = self.data if self.filtered is None else self.filtered
        _result = (
            _df
            .group_by(
                pl.col('filerIdent'),
                self.config.NAME_ORG,
                pl.col(self.config.type_.DATE)
                .dt.year()
                .cast(pl.String)
                .alias('year'))
            .agg(
                pl.col(self.config.type_.AMOUNT)
                .cast(pl.Float64)
                .alias('total')
                .sum()
                .round())
            .to_pandas()
        )

        _merge_uniques = (
            self.unique_filers
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
        self.grouped = _ct
        return _ct


@dataclass
class TexasSearch:
    data: Optional[pl.LazyFrame] = None
    config: TexasSearchSetup = None

    def __repr__(self):
        return f"TexasSearch({self.config.DATA})"

    def __post_init__(self):
        self.config = TexasSearchSetup(self.data)
        self.data = self.config.DATA


    def search(self, *args, by_filer=False) -> TexasSearchResult:
        result = TexasSearchResult(input_terms=args, config=self.config)
        # self.params.extend(args)
        _result = []
        _unique_filers = []
        _unique_filters = dict()
        _still_running, _message_printed = True, False
        _start_time = time.time()
        while _still_running:
            for p in result.input_terms:
                _param_df = (
                    self.data
                    .filter(
                        pl.col(self.config.search_field_ if not by_filer else self.config.filer_name_)
                        .str
                        .contains(p)
                    )
                   .collect()
                )
                _result.append(_param_df)

                _param_unique_filers = (
                    _param_df
                    .group_by(
                        pl.col(self.config.filer_id_))
                    .agg(
                        pl.col(self.config.type_.FILER_NAME)
                        .first()
                        .alias(self.config.type_.FILER_NAME)
                    ))
                _unique_filers.append(_param_unique_filers)

                _unique_filters[p] = {
                    x[0]: x[1] for x in enumerate(
                        set(
                            _param_df.to_pandas()[self.config.search_field_].to_list()
                        ), 1
                    )
                }
                if time.time() - _start_time > 5 and not _message_printed:
                    print("Still searching...")
                    _message_printed = True
            _result = pl.concat(_result)
            _unique_filers = pl.concat(_unique_filers)
            _still_running = False

        result.data = _result
        result.unique_filers = _unique_filers
        result.unique_filters = _unique_filters
        if by_filer:
            return result
        result = self.choose_options(result)
        if not result.selected_choices:
            return result
        result.selected_choices = result.selected_choices
        _filtered_result = (
            _result
            .filter(
                pl.col(
                    self.config.search_field_
                )
                .str
                .contains(
                    '|'.join(
                        result.selected_choices
                    )
                )
            )
        )
        result.filtered = _filtered_result
        return result

    @staticmethod
    def choose_options(_result: TexasSearchResult) -> TexasSearchResult:
        _selections = set()
        for term, options in _result.unique_filters.items():
            if len(options) == 1 and options[1] == term:
                print(f"{term} found. Added to selection list")
                _selections.add(term)
                continue
            for k, v in options.items():
                if v:
                    print(f"{k}: {v}")
            if options:
                _select = input(f"Select which options you'd like for {term.upper()} by number(s) or hit 'enter' to select all: ")
                if not _select:
                    _result.selected_choices.update(options.values())
                else:
                    _choices = list(map(int, _select.split(',')))
                    _result.selected_choices.update(_choices)
        return _result