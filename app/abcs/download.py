from abcs.state_configs import StateCampaignFinanceConfigs
import requests
import ssl
import sys
from tqdm import tqdm
from dataclasses import dataclass, field
import urllib.request
from zipfile import ZipFile
from pathlib import Path
from typing import Self, ClassVar, Protocol, Type
import os


@dataclass
class FileDownloader(Protocol):
    _configs: ClassVar[StateCampaignFinanceConfigs]
    _folder: Path

    @property
    def folder(self) -> Path:
        return self._folder

    @folder.setter
    def folder(self, value: Path) -> None:
        self._folder = value

    @property
    def _tmp(self) -> Path:
        return self.folder if self.folder else self._configs.FOLDER

    def check_if_folder_exists(self) -> Path:
        ...

    def download(self, read_from_temp: bool = True) -> Self:
        tmp = ...
        temp_filename = tmp / ...

        def download_file_with_requests() -> None:
            # download files
            with requests.get(self._configs.ZIPFILE_URL, stream=True) as resp:
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
                    print("Download Complete")
                return None

        def download_file_with_urllib3() -> None:
            ssl_context = ssl.create_default_context()
            ssl_context.options |= ssl.OP_CIPHER_SERVER_PREFERENCE
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=2")
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            print(f"Downloading {self._configs.STATE_CAMPAIGN_FINANCE_AGENCY} Files...")

            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ssl_context)
            )
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(self._configs.ZIPFILE_URL, temp_filename)

        def extract_zipfile() -> None:
            # extract zip file to temp folder
            with ZipFile(temp_filename, "r") as myzip:
                print("Extracting Files...")
                for _ in tqdm(myzip.namelist()):
                    myzip.extractall(tmp)
                os.unlink(temp_filename)
                self.folder = tmp  # set folder to temp folder

        try:
            if read_from_temp is False:
                # check if tmp folder exists
                if tmp.is_dir():
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
                            return self
                        else:
                            print("Exiting...")
                            sys.exit()

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

            return self

    def read(self):
        self.folder = self._tmp

    def __post_init__(self):
        self.check_if_folder_exists()
