import sys
sys.path.insert(0, '/Users/johneakin/PyCharmProjects/campaignfinance')

import polars as pl
from pathlib import Path

# Check actual column names in parquet files
contribs_file = Path('/Users/johneakin/PyCharmProjects/campaignfinance/tmp/texas/contribs_05_20260524.parquet')
expend_file = Path('/Users/johneakin/PyCharmProjects/campaignfinance/tmp/texas/expend_03_20260524.parquet')

print("=== CONTRIBUTION FILE COLUMNS ===")
df = pl.scan_parquet(contribs_file).limit(1).collect()
print("Columns:", df.columns)
print("Schema:", df.schema)
print("\nSample row:")
row = df.to_dicts()[0]
for k, v in list(row.items())[:15]:
    print(f"  {k!r}: {v!r} ({type(v).__name__})")

print("\n=== EXPENDITURE FILE COLUMNS ===")
df2 = pl.scan_parquet(expend_file).limit(1).collect()
print("Columns:", df2.columns)
row2 = df2.to_dicts()[0]
for k, v in list(row2.items())[:15]:
    print(f"  {k!r}: {v!r} ({type(v).__name__})")
