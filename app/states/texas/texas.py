from __future__ import annotations
from pathlib import Path
from typing import (
    Dict,
    Type,
    Generator,
    List
)
import funcs
from logger import Logger
# from states.texas.texas_database import local_postgres_engine as engine
import states.texas.validators as validators
from states.texas.texas_downloader import TECDownloader
from abcs import StateCategoryClass, StateConfig, CategoryConfig, CSVReaderConfig
from functools import partial

logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

# SQLModels = Generator[SQLModel, None, None]
CategoryFileList = List[Path]
TexasValidatorType = Type[validators.TECSettings]
FileRecords = Generator[Dict, None, None]

fields = funcs.read_toml(Path(__file__).parent / "texas_fields.toml")

TEXAS_CONFIGURATION = StateConfig(
    STATE_NAME="Texas",
    STATE_ABBREVIATION="TX",
    CATEGORY_TYPES={
        'expenses': CategoryConfig(
            PREFIX=fields['file-prefixes']['expenses'],
            VALIDATOR=validators.TECExpense),
        'contributions': CategoryConfig(
            PREFIX=fields['file-prefixes']['contributions'],
            VALIDATOR=validators.TECContribution),
        'filers': CategoryConfig(
            PREFIX=fields['file-prefixes']['filers'],
            VALIDATOR=validators.TECFiler),
        'reports': CategoryConfig(
            PREFIX=fields['file-prefixes']['reports'],
            VALIDATOR=validators.TECFinalReport),
        'travel': CategoryConfig(
            PREFIX=fields['file-prefixes']['travel'],
            VALIDATOR=validators.TECTravelData),
        'candidates': CategoryConfig(
            PREFIX=fields['file-prefixes']['candidates'],
            VALIDATOR=validators.CandidateData),
        'debts': CategoryConfig(
            PREFIX=fields['file-prefixes']['debts'],
            VALIDATOR=validators.DebtData)
    },
    # DATABASE_ENGINE=engine,
    CSV_CONFIG=CSVReaderConfig(),
)

TexasDownloader = TECDownloader(config=TEXAS_CONFIGURATION)
TexasCategory = partial(StateCategoryClass, config=TEXAS_CONFIGURATION)
