from __future__ import annotations
from pathlib import Path
from typing import Dict, List
from typing import Generator, Type
import funcs
from logger import Logger
from abcs import StateCategoryClass, StateConfig, CategoryConfig, CSVReaderConfig
from states.oklahoma.oklahoma_database import oklahoma_snowpark_session
import states.oklahoma.validators as validators
from functools import partial

# TODO: Change File Prefix Configurations to Oklahoma
# TODO: Make sure file folder reads only CSVs in Oklahoma so it doesn't try to read Zip files
ENGINE = oklahoma_snowpark_session
logger = Logger(__name__)
logger.info(f"Logger initialized in {__name__}")

# SQLModels = Generator[SQLModel, None, None]

CategoryFileList = List[Path]
OklahomaValidatorType = Type[validators.OklahomaSettings]
FileRecords = Generator[Dict, None, None]

fields = funcs.read_toml(Path(__file__).parent / "oklahoma_fields.toml")


OKLAHOMA_CONFIGURATION = StateConfig(
    STATE_NAME="Oklahoma",
    STATE_ABBREVIATION="OK",
    CATEGORY_TYPES={
            'expenses': CategoryConfig(
                SUFFIX=fields['file-suffixes']['expenses'],
                VALIDATOR=validators.OklahomaExpenditure),
            'contributions': CategoryConfig(
                SUFFIX=fields['file-suffixes']['contributions'],
                VALIDATOR=validators.OklahomaContribution),
            'lobby': CategoryConfig(
                SUFFIX=fields['file-suffixes']['lobby'],
                VALIDATOR=validators.OklahomaLobbyistExpenditure)
    },
    DATABASE_ENGINE=ENGINE,
    CSV_CONFIG=CSVReaderConfig(
        lowercase_headers=True,
        replace_space_in_headers=True
    )
)


OklahomaCategory = partial(StateCategoryClass, config=OKLAHOMA_CONFIGURATION)
