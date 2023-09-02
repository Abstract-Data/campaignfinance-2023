from abcs.state_configs import StateCampaignFinanceConfigs
from typing import List, Tuple
from dataclasses import field, dataclass
from tqdm import tqdm
from pydantic import ValidationError
from collections import Counter
import pandas as pd
from abc import ABC, abstractmethod

@dataclass
class StateFileValidation(ABC):
    passed: List = field(init=False)
    failed: List = field(init=False)
    errors: pd.DataFrame = field(init=False)

    @abstractmethod
    def validate(
            self,
            records,
            validator: StateCampaignFinanceConfigs.VALIDATOR,
    ) -> Tuple[List, List]:
        return self.passed, self.failed

    @abstractmethod
    def error_report(self) -> pd.DataFrame:

        _errors = [
            {
                'type': f['error'].errors()[0]['type'], 'msg': f['error'].errors()[0]['msg']
            } for f in self.failed
        ]
        error_df = pd.DataFrame.from_dict(
            Counter(
                [
                    str(e) for e in _errors
                ]),
            orient='index',
            columns=['count']
        ).rename_axis('error').reset_index()
        error_df.loc[len(error_df)-1, 'Total'] = error_df['count'].sum()
        self.errors = error_df
        return self.errors
