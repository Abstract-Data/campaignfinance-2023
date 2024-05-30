from __future__ import annotations
from pathlib import Path
from typing import ClassVar
from dataclasses import field, dataclass
import inject
from tqdm import tqdm
from zipfile import ZipFile
import requests
import os
import sys
import ssl
import urllib.request
from logger import Logger
from abcs import (
    StateCampaignFinanceConfigClass,
    FileDownloader,
)
from .texas_configs import TexasConfigs


def download_base_config(binder):
    binder.bind(StateCampaignFinanceConfigClass, TexasConfigs)


@dataclass
class TECFileDownloader(FileDownloader):
    _configs: ClassVar[StateCampaignFinanceConfigClass]
    _folder: Path = TexasConfigs.TEMP_FOLDER
    __logger: Logger = field(init=False)

    def init(self):
        inject.configure(download_base_config)

    @property
    def folder(self) -> StateCampaignFinanceConfigClass.TEMP_FOLDER:
        return self._folder

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return self.folder if self.folder else StateCampaignFinanceConfigClass.TEMP_FOLDER

    @classmethod
    def download_file_with_requests(cls, config: StateCampaignFinanceConfigClass) -> None:
        # download files
        with requests.get(config.ZIPFILE_URL, stream=True) as resp:
            # check header to get content length, in bytes
            total_length = int(resp.headers.get("Content-Length"))

            # Chunk download of zip file and write to temp folder
            with open(config.TEMP_FILENAME, "wb") as f:
                for chunk in tqdm(
                        resp.iter_content(chunk_size=1024),
                        total=round(total_length / 1024, 2),
                        unit="KB",
                        desc="Downloading",
                ):
                    if chunk:
                        f.write(chunk)
                print("Download Complete")
            return None

    def check_if_folder_exists(self) -> Path:
        self.__logger.info(f"Checking if {self.folder} exists...")
        if self.folder.exists():
            return self.folder

        self.__logger.debug(f"{self.folder} does not exist...")
        self.__logger.debug(f"Throwing input prompt...")
        _create_folder = input("Temp folder does not exist. Create? (y/n): ")
        self.__logger.debug(f"User input: {_create_folder}")
        if _create_folder.lower() == "y":
            self.folder.mkdir()
            return self.folder
        else:
            print("Exiting...")
            self.__logger.info("User selected 'n'. Exiting...")
            sys.exit()

    @inject.params(read_from_temp=False, overwrite=True, config=StateCampaignFinanceConfigClass)
    def download(
            self,
            config: StateCampaignFinanceConfigClass,
            read_from_temp: bool,
            overwrite: bool
    ) -> None:
        tmp = self._tmp
        temp_filename = tmp / "TEC_CF_CSV.zip"

        self.__logger.info(f"Setting temp filename to {temp_filename} in download func")

        def download_file_with_requests() -> None:
            # download files
            with requests.get(config.ZIPFILE_URL, stream=True) as resp:
                # check header to get content length, in bytes
                total_length = int(resp.headers.get("Content-Length"))

                # Chunk download of zip file and write to temp folder
                with open(temp_filename, "wb") as f:
                    for chunk in tqdm(
                            resp.iter_content(chunk_size=1024),
                            total=round(total_length / 1024, 2),
                            unit="KB",
                            desc="Downloading",
                    ):
                        if chunk:
                            f.write(chunk)
                    self.__logger.info("Download Complete")
                return None

        def download_file_with_urllib3() -> None:
            self.__logger.info(
                f"Downloading {config.STATE_CAMPAIGN_FINANCE_AGENCY} Files..."
            )
            ssl_context = ssl.create_default_context()
            ssl_context.options |= ssl.OP_CIPHER_SERVER_PREFERENCE
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=2")
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            self.__logger.info(f"SSL Context: {ssl_context}")
            self.__logger.debug(
                f"Downloading {config.STATE_CAMPAIGN_FINANCE_AGENCY} Files..."
            )

            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ssl_context)
            )
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(config.ZIPFILE_URL, temp_filename)

        def extract_zipfile() -> None:
            # extract zip file to temp folder
            self.__logger.debug(f"Extracting {temp_filename} to {tmp}...")
            with ZipFile(temp_filename, "r") as myzip:
                print("Extracting Files...")
                for _ in tqdm(myzip.namelist()):
                    myzip.extractall(tmp)
                os.unlink(temp_filename)
                self.folder = tmp  # set folder to temp folder
                self.__logger.debug(
                    f"Extracted {temp_filename} to {tmp}, set folder to {tmp}"
                )

        try:
            if read_from_temp is False:
                # check if tmp folder exists
                if tmp.is_dir():
                    if overwrite is False:
                        ask_to_make_folder = input(
                            "Temp folder already exists. Overwrite? (y/n): "
                        )
                        if ask_to_make_folder.lower() == "y":
                            print("Overwriting Temp Folder...")
                            download_file_with_urllib3()
                            extract_zipfile()
                        else:
                            as_to_change_folder = input(
                                "Use temp folder as source? (y/n): "
                            )
                            if as_to_change_folder.lower() == "y":
                                if tmp.glob("*.csv") == 0 and tmp.glob("*.zip") == 1:
                                    print("No CSV files in temp folder. Found .zip file...")
                                    print("Extracting .zip file...")
                                    extract_zipfile()
                                else:
                                    self.folder = tmp  # set folder to temp folder
                            else:
                                print("Exiting...")
                                sys.exit()
                    else:
                        self.__logger.info("Overwriting Temp Folder...")
                        download_file_with_urllib3()
                        self.__logger.info("Temp Folder Overwritten, extracting zip file...")
                        extract_zipfile()
                        self.__logger.info("Zip file extracted")

                # else:
                #     self.folder = tmp  # set folder to temp folder

            # remove tmp folder if user cancels download
        except KeyboardInterrupt:
            print("Download Cancelled")
            print("Removing Temp Folder...")
            for file in tmp.iterdir():
                file.unlink()
            tmp.rmdir()
            print("Temp Folder Removed")
        return None

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        TECFileDownloader.__logger = Logger(self.__class__.__name__)
        self.init()
        self.check_if_folder_exists()
