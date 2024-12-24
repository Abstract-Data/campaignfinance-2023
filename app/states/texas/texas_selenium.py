# from pathlib import Path
# from datetime import datetime
# import zipfile
# from tqdm import tqdm
# import time
# from icecream import ic
#
# import selenium.common.exceptions
# from selenium import webdriver
# from selenium.webdriver.support.ui import Select, WebDriverWait
# from selenium.webdriver.chrome.options import Options
# from selenium.webdriver.common.by import By
# from selenium.common.exceptions import NoSuchElementException
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.action_chains import ActionChains
# from selenium.webdriver.common.keys import Keys
# from selenium.webdriver.support.wait import WebDriverWait
#
# download_path = Path(__file__).parents[3] / "tmp" / "texas"
# options = Options()
# _prefs = {'download.default_directory': str(download_path)}
# options.add_experimental_option('prefs', _prefs)
# options.add_argument("--window-size=1920,1080")  # set window size to native GUI size
# options.add_argument("start-maximized")  # ensure window is full-screen
# options.page_load_strategy = "none"  # Load the page as soon as possible
# options.add_argument("--headless=True")  # hide GUI
#
# def download_file():
#     driver = webdriver.Chrome(options=options)
#     wait = WebDriverWait(driver, 10)
#
#     driver.get("https://ethics.state.tx.us/")
#     wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Search"))).click()
#     wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Campaign Finance Reports"))).click()
#     wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Database of Campaign Finance Reports"))).click()
#     wait.until(EC.element_to_be_clickable((By.PARTIAL_LINK_TEXT, "Campaign Finance CSV Database"))).click()
#     time.sleep(5)
#
#     in_progress = False
#     while True:
#         dl_files = list(download_path.glob("*.crdownload"))
#         if dl_files:
#             if not in_progress:
#                 ic(f"File {Path(dl_files[0]).stem} download in progress")
#                 in_progress = True
#             time.sleep(10)
#         else:
#             if in_progress:
#                 ic("File download complete")
#                 in_progress = False
#             break
#
#
#     files = (Path(x) for x in download_path.glob("*.zip"))
#     latest_file = max(files, key=lambda x: x.stat().st_ctime)
#     with zipfile.ZipFile(latest_file, "r") as zip_ref:
#         zip_file_info = zip_ref.infolist()
#         for file in tqdm(zip_file_info, desc="Extracting Files"):
#             zip_ref.extract(file, download_path)
#             file_name = Path(file.filename)
#             rename = f"{file_name.stem}{datetime.now().strftime("%Y%m%d")}{file_name.suffix}"
#             Path(download_path / file.filename).rename(download_path / rename)
#     latest_file.unlink()
#
# download_file()