from pathlib import Path
from typing import ClassVar, Dict, List
from dataclasses import dataclass
import csv
from tqdm import tqdm
from typing import Generator, Protocol, Iterator, Tuple
import datetime
from abcs import StateCampaignFinanceConfigs, FileDownloader
from logger import Logger
from abcs import StateCampaignFinanceConfigs

logger = Logger(__name__)


@dataclass
class StateCategories(Protocol):
    expenses: ClassVar[List] = None
    contributions: ClassVar[List] = None
    filers: ClassVar[List] = None
    _config: ClassVar[StateCampaignFinanceConfigs]

    def __repr__(self):
        return f"TECFileCategories()"

    def __post_init__(self):
        ...

    @classmethod
    def create_tec_files(cls, prefix: str, folder: StateCampaignFinanceConfigs.FOLDER) -> List[Path]:
        ...

    @classmethod
    def read(cls, category: Generator[object, None, None]) -> Generator[List, None, None]:
        ...

    @classmethod
    def load(cls, read) -> List[Dict]:
        ...

    @classmethod
    def validate_category(cls, load, to_db: bool = False, update: bool = False):
        ...

    def update_database(self, category: List[object]) -> None:
        ...

