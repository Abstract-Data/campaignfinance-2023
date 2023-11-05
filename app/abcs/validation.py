from abcs.state_configs import StateCampaignFinanceConfigs
from pydantic import ValidationError
from typing import List, Tuple, Protocol, Iterator, Dict, Any
from dataclasses import field, dataclass
from tqdm import tqdm
from pydantic import ValidationError, BaseModel
from collections import Counter
import pandas as pd

@dataclass
class StateFileValidation(Protocol):
    passed: Iterator[BaseModel] = field(init=False)
    failed: Iterator[Dict[str, ValidationError]] = field(init=False)
    errors: pd.DataFrame = field(init=False)

    def validate(
            self,
            records,
            validator: StateCampaignFinanceConfigs.VALIDATOR,
            to_db: bool = False,
            update: bool = False,
    ) -> Tuple[Iterator[BaseModel], Iterator[Dict[str, ValidationError]]]:
        return self.passed, self.failed

    def error_report(self) -> pd.DataFrame:
        return self.errors
