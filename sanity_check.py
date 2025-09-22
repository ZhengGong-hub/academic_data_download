import pandas as pd

df = pd.read_parquet("data/factors_combined_updated.parquet", engine="pyarrow")
print(df.head())

df = df.loc[
    (df['date'] >= pd.Timestamp('2010-01-01')) &
    (df['gvkey'].isin(["001690"]))
]

print(df.tail(100).to_string(index=False))

from pathlib import Path

p = Path("data/factors_combined_updated.parquet")
size_bytes = p.stat().st_size

def fmt(n):
    for u in ["B","KB","MB","GB","TB"]:
        if n < 1024:
            return f"{n:.2f} {u}"
        n /= 1024
    return f"{n:.2f} PB"

print(f"{fmt(size_bytes)} ({size_bytes:,} bytes)")



num = df.describe(percentiles=[.25, .5, .75])
cat = df.describe(include=['object', 'category'])
nas = df.isna().sum()

print("\n== Numeric ==\n" + num.to_string())
print("\n== Categorical ==\n" + cat.to_string())
print("\n== Missing values ==\n" + nas.to_string())


na_rows = df[df.isna().any(axis=1)]
print("\n== Rows with any NA ==")
if not na_rows.empty:
    print(na_rows.to_string(index=False))
else:
    print("None")

print(df.tail(100).to_string(index=False))