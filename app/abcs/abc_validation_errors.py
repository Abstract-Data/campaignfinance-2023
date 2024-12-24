from __future__ import annotations
from pydantic import BaseModel, ValidationError, model_validator, Field
from typing import List, Dict, Optional
import pandas as pd
from icecream import ic

ic.configureOutput(prefix='abc_validation_errors|')


class RecordValidationError(BaseModel):
    id: int
    type: str
    column: Optional[str] = None
    msg: str
    err_num: int
    validator: str
    input: Optional[Dict | str]

    @model_validator(mode='before')
    def set_column(cls, values: Dict) -> Dict:
        if 'ctx' in values:
            col = values['ctx'].get('column', None)
            values['column'] = col
        else:
            values['column'] = values.get('loc')[0]
        return values


class ValidationErrorList(BaseModel):
    errors: List[RecordValidationError] = Field(default_factory=list)
    error_count: int = 0
    summary: object = None

    def add_record_errors(self, record: Dict) -> ValidationErrorList:
        record_errors = 0
        self.error_count += 1
        try:
            record_error_list = []
            for each_error in record['error']:
                record_errors += 1
                _error_validator = RecordValidationError(
                    **each_error,
                    id=self.error_count,
                    err_num=record_errors,
                )
                record_error_list.append(
                    _error_validator
                )
            self.errors.extend(record_error_list)
            # record_error_list.clear()
        except ValidationError as e:
            ic(e)

        return self

    def create_error_list(self, records: List[Dict]) -> ValidationErrorList:
        for record in records:
            self.add_record_errors(record)
        return self

    def _error_dataframe(self) -> pd.DataFrame | ic:
        _errors = [dict(error) for error in self.errors]
        if not _errors:
            return
        df = pd.DataFrame(
            [
                dict(error) for error in self.errors
            ]
        )
        return df

    def _error_summary(self, error_dataframe: pd.DataFrame = None) -> pd.crosstab:
        df = self._error_dataframe() if not error_dataframe else error_dataframe
        df_counts = pd.crosstab(
            index=[df['validator'], df['column']],
            columns=df['type'],
            values=df['id'],
            aggfunc='count',
            margins=True,
            margins_name='Total'
        ).fillna(0).astype(int) if df is not None else None
        self.summary = df_counts
        return self.summary

    def show_errors(self):
        summary = self._error_summary()
        if summary is None:
            ic('No errors found')
            return
        validator_list = summary.index.get_level_values(0).unique()
        ic(f'Error Summary for {validator_list[0]}')
        ic(summary)

    def to_df(self) -> pd.DataFrame | object:
        return self.summary if self.summary is not None else ic('No errors found')
