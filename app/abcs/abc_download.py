from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
import abc
from abcs.abc_state_config import StateConfig
from logger import Logger
import sys


@dataclass
class FileDownloaderABC(abc.ABC):
    config: StateConfig
    folder: Path = field(init=False)
    __logger: Logger = field(init=False)

    def check_if_folder_exists(self) -> Path:
        _temp_folder_name = self.config.TEMP_FOLDER.stem.title()
        self.__logger.info(f"Checking if {_temp_folder_name} temp folder exists...")
        if self.config.TEMP_FOLDER.exists():
            self.folder = self.config.TEMP_FOLDER
            return self.folder

        self.__logger.debug(f"{_temp_folder_name} temp folder does not exist...")
        self.__logger.debug(f"Throwing input prompt...")
        _create_folder = input("Temp folder does not exist. Create? (y/n): ")
        self.__logger.debug(f"User input: {_create_folder}")
        if _create_folder.lower() == "y":
            self.config.TEMP_FOLDER.mkdir()
            self.folder = self.config.TEMP_FOLDER
            return self.folder
        else:
            print("Exiting...")
            self.__logger.info("User selected 'n'. Exiting...")
            sys.exit()

    @abc.abstractmethod
    def download(self, overwrite: bool, read_from_temp: bool) -> FileDownloaderABC:
        ...

    def read(self):
        return self.folder

    def __post_init__(self):
        self.__logger = Logger(self.__class__.__name__)
        self.check_if_folder_exists()
