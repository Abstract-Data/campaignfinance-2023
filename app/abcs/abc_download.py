from __future__ import annotations
from abcs.abc_config import StateCampaignFinanceConfigClass
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Protocol


@dataclass
class FileDownloader(Protocol):
    _configs: ClassVar[StateCampaignFinanceConfigClass]
    _folder: StateCampaignFinanceConfigClass.TEMP_FOLDER

    @property
    def folder(self) -> StateCampaignFinanceConfigClass.TEMP_FOLDER:
        return ...

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return ...

    def check_if_folder_exists(self) -> Path:
        ...

    def download(self, config: StateCampaignFinanceConfigClass, read_from_temp: bool = True) -> FileDownloader:
        ...

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        self.check_if_folder_exists()
