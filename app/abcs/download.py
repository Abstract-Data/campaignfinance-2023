from abcs.state_configs import StateCampaignFinanceConfigs
from dataclasses import dataclass
from pathlib import Path
from typing import Self, ClassVar, Protocol


@dataclass
class FileDownloader(Protocol):
    _configs: ClassVar[StateCampaignFinanceConfigs]
    _folder: Path

    @property
    def folder(self) -> Path:
        return ...

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return ...

    def check_if_folder_exists(self) -> Path:
        ...

    def download(self, read_from_temp: bool = True) -> Self:
        ...

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        self.check_if_folder_exists()
