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
    FileDownloaderABC, StateConfig
)


@dataclass
class TECDownloader(FileDownloaderABC):
    config: StateConfig

    @classmethod
    def download_file_with_requests(cls) -> None:
        # download files
        with requests.get(cls.config.DOWNLOAD_CONFIG.ZIPFILE_URL, stream=True) as resp:
            # check header to get content length, in bytes
            total_length = int(resp.headers.get("Content-Length"))

            # Chunk download of zip file and write to temp folder
            with open(cls.config.DOWNLOAD_CONFIG.TEMP_FILENAME, "wb") as f:
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

    def download(
            self,
            overwrite: bool,
            read_from_temp: bool
    ) -> TECDownloader:
        tmp = self.config.TEMP_FOLDER
        temp_filename = self.config.DOWNLOAD_CONFIG.TEMP_FILENAME

        self.__logger.info(f"Setting temp filename to {temp_filename} in download func")

        def download_file_with_requests() -> None:
            # download files
            with requests.get(self.config.DOWNLOAD_CONFIG.ZIPFILE_URL, stream=True) as resp:
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
                f"Downloading {self.config.STATE_NAME.title()} Campaign Finance Files..."
            )
            ssl_context = ssl.create_default_context()
            ssl_context.options |= ssl.OP_CIPHER_SERVER_PREFERENCE
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=2")
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            self.__logger.info(f"SSL Context: {ssl_context}")
            self.__logger.debug(
                f"Downloading {self.config.STATE_NAME.title()} Campaign Finance Files..."
            )

            opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=ssl_context)
            )
            urllib.request.install_opener(opener)
            urllib.request.urlretrieve(self.config.DOWNLOAD_CONFIG.ZIPFILE_URL, temp_filename)

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
        return self

    def read(self):
        self.folder = self.config.TEMP_FOLDER

    # def __post_init__(self):
    #     self.check_if_folder_exists()
