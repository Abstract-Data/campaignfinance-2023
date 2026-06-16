from __future__ import annotations

import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

from app.abcs import FileDownloaderABC, RecordGen, StateConfig
from app.funcs.csv_reader import FileReader
from app.logger import Logger
from app.scrapers.drift_detector import ScraperMarkupError, verify_markup
from app.scrapers.expectations import TEXAS_TEC_PORTAL

logger = Logger(__name__)

_DOWNLOAD_POLL_SECONDS = 10
_DOWNLOAD_TIMEOUT_SECONDS = 600


class DownloadError(Exception):
    """Raised when the Texas TEC download or extraction fails."""


def _is_safe_zip_member(member_name: str, destination: Path) -> bool:
    normalized = member_name.replace("\\", "/")
    member_path = Path(normalized)
    if member_path.is_absolute() or ".." in member_path.parts:
        return False
    resolved_target = (destination / member_path).resolve()
    resolved_dest = destination.resolve()
    return resolved_target == resolved_dest or resolved_target.is_relative_to(resolved_dest)


@dataclass
class TECDownloader(FileDownloaderABC):
    config: StateConfig

    def __post_init__(self) -> None:
        super().__post_init__()

    def _verify_portal_markup(self, html: str, *, step: str) -> None:
        """Assert TEC portal navigation markup is present before Selenium clicks."""
        try:
            verify_markup(
                html,
                expectation=TEXAS_TEC_PORTAL,
                compare_fingerprint=True,
                logger_instance=logger,
            )
        except ScraperMarkupError as exc:
            msg = f"Texas TEC markup drift at {step}: {exc}"
            logger.error(msg)
            raise DownloadError(msg) from exc

    def download(
        self,
        *,
        overwrite: bool = False,
        headless: bool = False,
        output_dir: Path | None = None,
    ) -> Path:
        tmp = output_dir if output_dir is not None else self.config.TEMP_FOLDER
        options = Options()
        prefs = {"download.default_directory": str(tmp)}
        options.add_experimental_option("prefs", prefs)
        options.add_argument("--window-size=1920,1080")
        options.add_argument("start-maximized")
        options.page_load_strategy = "none"
        if headless:
            options.add_argument("--headless")

        tmp.mkdir(parents=True, exist_ok=True)

        if overwrite:
            for file_extension in ("*.csv", "*.txt", "*.zip", "*.crdownload"):
                for file in tmp.glob(file_extension):
                    logger.info(f"Removing {file}")
                    file.unlink()

        driver = None
        try:
            driver = webdriver.Chrome(options=options)
            wait = WebDriverWait(driver, 10)

            driver.get("https://ethics.state.tx.us/")
            self._verify_portal_markup(driver.page_source, step="landing")
            wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Search"))).click()
            wait.until(
                EC.element_to_be_clickable((By.LINK_TEXT, "Campaign Finance Reports"))
            ).click()
            wait.until(
                EC.element_to_be_clickable((By.LINK_TEXT, "Database of Campaign Finance Reports"))
            ).click()
            wait.until(
                EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Campaign Finance CSV Database"))
            ).click()
            time.sleep(5)

            self._wait_for_download(tmp)

            zip_files = list(tmp.glob("*.zip"))
            if not zip_files:
                msg = f"No zip file found in {tmp} after download"
                raise DownloadError(msg)

            latest_file = max(zip_files, key=lambda path: path.stat().st_ctime)
            with zipfile.ZipFile(latest_file, "r") as zip_ref:
                zip_file_info = zip_ref.infolist()
                for file in tqdm(zip_file_info, desc="Extracting Files"):
                    if not _is_safe_zip_member(file.filename, tmp):
                        logger.warning(f"Skipping unsafe zip entry: {file.filename}")
                        continue
                    zip_ref.extract(file, tmp)
                    file_name = Path(file.filename)
                    rename = (
                        f"{file_name.stem}_{datetime.now().strftime('%Y%m%d')}{file_name.suffix}"
                    )
                    Path(tmp / file.filename).rename(tmp / rename)
            logger.info(f"Removing {latest_file}")
            latest_file.unlink()
        except (DownloadError, ScraperMarkupError):
            raise
        except Exception as exc:
            msg = f"Texas TEC download failed: {exc}"
            raise DownloadError(msg) from exc
        finally:
            if driver is not None:
                driver.quit()

        return tmp

    def _wait_for_download(self, tmp: Path) -> None:
        saw_in_progress = False
        deadline = time.monotonic() + _DOWNLOAD_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            crdownloads = list(tmp.glob("*.crdownload"))
            zip_files = list(tmp.glob("*.zip"))

            if crdownloads:
                if not saw_in_progress:
                    logger.info(f"File {Path(crdownloads[0]).stem} download in progress")
                saw_in_progress = True
                time.sleep(_DOWNLOAD_POLL_SECONDS)
                continue

            if saw_in_progress and zip_files:
                latest = max(zip_files, key=lambda path: path.stat().st_ctime)
                if self._is_file_size_stable(latest):
                    logger.info("File download complete")
                    return
                time.sleep(_DOWNLOAD_POLL_SECONDS)
                continue

            time.sleep(_DOWNLOAD_POLL_SECONDS)

        msg = f"Download timed out after {_DOWNLOAD_TIMEOUT_SECONDS} seconds"
        raise DownloadError(msg)

    def _is_file_size_stable(
        self,
        path: Path,
        *,
        checks: int = 2,
        interval: float = 1.0,
    ) -> bool:
        try:
            previous_size = path.stat().st_size
        except OSError:
            return False

        for _ in range(checks):
            time.sleep(interval)
            if list(path.parent.glob("*.crdownload")):
                return False
            try:
                current_size = path.stat().st_size
            except OSError:
                return False
            if current_size != previous_size:
                previous_size = current_size
                continue
            return True
        return False

    def read(self) -> RecordGen:
        reader = FileReader()
        self.data = reader.read_folder(self.folder, file_counts=self.config.FILE_COUNTS)
        return iter(self.data)
