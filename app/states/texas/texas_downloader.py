from __future__ import annotations
from dataclasses import field, dataclass
from pathlib import Path
from datetime import datetime
import zipfile
from click import File
from numpy import record
from tqdm import tqdm
import time
from icecream import ic
from funcs.csv_reader import FileReader
import functools

import selenium.common.exceptions
from selenium import webdriver
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.wait import WebDriverWait

from abcs import (
    FileDownloaderABC, StateConfig, CategoryConfig, CSVReaderConfig, CategoryTypes, RecordGen)
import states.texas.validators as validators


# class TECFileTypeList(FileTypeListABC):
        
#     def filter(self, fields: CategoryTypes) -> FileTypeListABC:
#         _filter = self._filter_by
#         self.contributions = _filter(fields.contributions)
#         self.expenses = _filter(fields.expenses)
#         self.filers = _filter(fields.filers)
#         self.reports = _filter(fields.reports)
#         # self.loans = _filter(fields.loans)
#         self.debts = _filter(fields.debts)
#         # self.notices = _filter(fields.notices)
#         # self.credits = _filter(fields.credits)
#         # self.spacs = _filter(fields.spacs)
#         self.candidates = _filter(fields.candidates)
#         self.travel = _filter(fields.travel)
#         return self
    
@dataclass
class TECDownloader(FileDownloaderABC):
    config: StateConfig
    
    def __post_init__(self):
        self.folder = self.config.TEMP_FOLDER


    def download(
            self,
            overwrite: bool = False,
            read_from_temp: bool = True
    ) -> TECDownloader:
        tmp = self.config.TEMP_FOLDER
        options = Options()
        _prefs = {'download.default_directory': str(tmp)}
        options.add_experimental_option('prefs', _prefs)
        options.add_argument("--window-size=1920,1080")  # set window size to native GUI size
        options.add_argument("start-maximized")  # ensure window is full-screen
        options.page_load_strategy = "none"  # Load the page as soon as possible
        # options.add_argument("--headless=True")  # hide GUI

        if not tmp.is_dir():
            tmp.mkdir()

        if overwrite:
            read_from_temp = False
            for file_extension in ('*.csv', '*.txt'):
                files = tmp.glob(file_extension)
                for file in files:
                    ic(f"Removing {file}")
                    file.unlink()

        driver = webdriver.Chrome(options=options)
        wait = WebDriverWait(driver, 10)

        driver.get("https://ethics.state.tx.us/")
        wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Search"))).click()
        wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Campaign Finance Reports"))).click()
        wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Database of Campaign Finance Reports"))).click()
        wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Campaign Finance CSV Database"))).click()
        time.sleep(5)

        in_progress = False
        while True:
            dl_files = list(tmp.glob("*.crdownload"))
            if dl_files:
                if not in_progress:
                    ic(f"File {Path(dl_files[0]).stem} download in progress")
                    in_progress = True
                time.sleep(10)
            else:
                if in_progress:
                    ic("File download complete")
                    in_progress = False
                break

        files = (Path(x) for x in tmp.glob("*.zip"))
        latest_file = max(files, key=lambda x: x.stat().st_ctime)
        with zipfile.ZipFile(latest_file, "r") as zip_ref:
            zip_file_info = zip_ref.infolist()
            for file in tqdm(zip_file_info, desc="Extracting Files"):
                zip_ref.extract(file, tmp)
                file_name = Path(file.filename)
                rename = f"{file_name.stem}_{datetime.now().strftime("%Y%m%d")}{file_name.suffix}"
                Path(tmp / file.filename).rename(tmp / rename)
        ic(f"Removing {latest_file}")
        latest_file.unlink()


        return self

    def read(self) -> CategoryTypes:
        _reader = FileReader()
        self.data = _reader.read_folder(self.folder, file_counts=self.config.FILE_COUNTS)
        return self.data
