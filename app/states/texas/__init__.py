from __future__ import annotations
from logger import Logger
# from states.texas.texas_database import local_postgres_engine as engine
import states.texas.validators as validators
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

TX_FIELDS = TEXAS_CONFIGURATION.FIELD_DATA
TexasCategoryConfig = partial(CategoryConfig, FIELDS=TX_FIELDS)
TEXAS_CONFIGURATION.CATEGORY_TYPES = CategoryTypes(
    **{
        'expenses': TexasCategoryConfig(
            DESC="expenses",
            VALIDATOR=validators.TECExpense),
        'contributions': TexasCategoryConfig(
            DESC="contributions",
            VALIDATOR=validators.TECContribution),
        'filers': TexasCategoryConfig(
            DESC="filers",
            VALIDATOR=validators.TECFilerName),
        'reports': TexasCategoryConfig(
            DESC='reports',
            VALIDATOR=validators.TECFinalReport),
        'travel': TexasCategoryConfig(
            DESC="travel",
            VALIDATOR=validators.TECTravelData),
        'candidates': TexasCategoryConfig(
            DESC="candidates",
            VALIDATOR=validators.CandidateData),
        'debts': TexasCategoryConfig(
            DESC="debts",
            VALIDATOR=validators.DebtData),
    }
)

TexasDownloader = partial(TECDownloader, config=TEXAS_CONFIGURATION)
TexasCategory = partial(StateCategoryClass, config=TEXAS_CONFIGURATION)
