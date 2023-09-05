from abcs.state_configs import StateCampaignFinanceConfigs
from typing import List, Tuple, Protocol, Iterator
from dataclasses import field, dataclass
from tqdm import tqdm
from pydantic import ValidationError
from collections import Counter
import pandas as pd

@dataclass
class StateFileValidation(Protocol):
    passed: List = field(init=False)
    failed: List = field(init=False)
    errors: pd.DataFrame = field(init=False)

    def validate(
            self,
            records,
            validator: StateCampaignFinanceConfigs.VALIDATOR,
    ) -> Tuple[List, List]:
        return self.passed, self.failed

    def error_report(self) -> pd.DataFrame:
        return self.errors
