import pandas as pd

df = pd.read_parquet("data/factors_combined_updated.parquet", engine="pyarrow")
print(df.head())

df = df.loc[
    (df['date'] >= pd.Timestamp('2010-01-01')) &
    (df['gvkey'].isin(["001690"]))
]

print(df.tail(100).to_string(index=False))