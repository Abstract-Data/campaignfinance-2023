from pathlib import Path
from app.states.texas.texas_downloader import TexasDownloader
from app.funcs.csv_reader import FileReader
from icecream import ic

TMP_FOLDER = Path(__file__).parents[1] / "tmp"
FOLDERS = list(x for x in TMP_FOLDER.iterdir() if x.is_dir())

file_reader = FileReader()

download = TexasDownloader()
download.download()

state_fields = dict()
for folder in FOLDERS:
    ic(f"{folder.name}")
    files = list(folder.glob("*.csv")) + list(folder.glob("*.parquet"))
    headers = set()
    for file in files:
        ic(f"-| {file.name}")
        try:
            if file.suffix == ".csv":
                _read = file_reader.read_csv(file)
                _headers = next(_read).keys()
                headers.update(_headers)
            elif file.suffix == ".parquet":
                _read = file_reader.read_parquet(file)
                _headers = next(_read).keys()
                headers.update(_headers)
        except StopIteration:
            ic(f"-| No headers found in {file.name}")
            continue
    state_fields[folder.name] = headers

ic(state_fields)
ic(len(state_fields))