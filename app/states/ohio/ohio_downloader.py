
from abcs import (
    FileDownloaderABC, StateConfig, CategoryTypes, RecordGen, CategoryConfig, progress, CSVReaderConfig)
from web_scrape_utils import CreateWebDriver, By


OHIO_ENTRY_URL = "https://www.ohiosos.gov/campaign-finance/search/"

OHIO_CONFIGURATION = StateConfig(
    STATE_NAME="Ohio",
    STATE_ABBREVIATION="OH",
    # DATABASE_ENGINE=engine,
    CSV_CONFIG=CSVReaderConfig(),

)


class OhioDownloader(FileDownloaderABC):
    config: StateConfig = OHIO_CONFIGURATION

    def download(self, overwrite: bool, read_from_temp: bool) -> FileDownloaderABC:
        _driver = self.driver.create_driver()
        _wait = self.driver
        _driver.get(OHIO_ENTRY_URL)
        _wait.wait_until_clickable(By.LINK_TEXT, "FTP Site")
        _ftp_site_url = _driver.find_element(By.LINK_TEXT, "FTP Site").get_attribute("href")
        _driver.get(_ftp_site_url)

        ...

    def consolidate_files(self):
        ...

    def read(self):
        ...
