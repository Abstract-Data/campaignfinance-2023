import pandas as pd
import os

data_dir = 'tmp/texas'
files = [
    'filers_20250805w.parquet',
    'cover_20250805w.parquet',
    'contribs_20250805w.parquet',
    'expend_20250805w.parquet'
]

for f in files:
    path = os.path.join(data_dir, f)
    if os.path.exists(path):
        print(f"--- {f} ---")
        try:
            df = pd.read_parquet(path)
            print(df.info())
            print(df.head(2))
        except Exception as e:
            print(f"Error reading {f}: {e}")
        print("\n")
