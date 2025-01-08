from __future__ import annotations
from logger import Logger
# from states.texas.texas_database import local_postgres_engine as engine
from . import validators
from .texas_downloader import TECDownloader
from abcs import StateCategoryClass, StateConfig, CategoryConfig, CSVReaderConfig, CategoryTypes
from functools import partial

logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")


TEXAS_CONFIGURATION = StateConfig(
    STATE_NAME="Texas",
    STATE_ABBREVIATION="TX",
    # DATABASE_ENGINE=engine,
    CSV_CONFIG=CSVReaderConfig(),
)

FIELDS = TEXAS_CONFIGURATION.FIELD_DATA['file-prefixes']
TEXAS_CONFIGURATION.CATEGORY_TYPES = CategoryTypes(
    **{
        'expenses': CategoryConfig(
            PREFIX=FIELDS['expenses'],
            VALIDATOR=validators.TECExpense),
        'contributions': CategoryConfig(
            PREFIX=FIELDS['contributions'],
            VALIDATOR=validators.TECContribution),
        'filers': CategoryConfig(
            PREFIX=FIELDS['filers'],
            VALIDATOR=validators.TECFiler),
        'reports': CategoryConfig(
            PREFIX=FIELDS['reports'],
            VALIDATOR=validators.TECFinalReport),
        'travel': CategoryConfig(
            PREFIX=FIELDS['travel'],
            VALIDATOR=validators.TECTravelData),
        'candidates': CategoryConfig(
            PREFIX=FIELDS['candidates'],
            VALIDATOR=validators.CandidateData),
        'debts': CategoryConfig(
            PREFIX=FIELDS['debts'],
            VALIDATOR=validators.DebtData),
    }
)

TexasDownloader = partial(TECDownloader, config=TEXAS_CONFIGURATION)
TexasCategory = partial(StateCategoryClass, config=TEXAS_CONFIGURATION)
