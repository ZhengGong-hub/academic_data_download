import pandas as pd
import glob 
import time 
from academic_data_download.utils.col_transform import merge_mktcap_fundq

factor_addrs = glob.glob('data/factors/*.parquet')

mktcap_df = pd.read_parquet('data/crsp/marketcap.parquet')
print(mktcap_df.head())

for addr in factor_addrs:
    # idenitify column name starting with "f_"
    df = pd.read_parquet(addr)
    if 'date' not in df.columns:
        # if date is not in df
        df = df[['gvkey', 'rdq', [col for col in df.columns if col.startswith('f_')][0]]]
        mktcap_df = merge_mktcap_fundq(mktcap_df, df).drop(columns=['rdq'])
    else:
        # if date is in df,
        df = df[['gvkey', 'date', [col for col in df.columns if col.startswith('f_')][0]]]
        mktcap_df = pd.merge_asof(mktcap_df, df, on=['date'], by=['gvkey'], direction='backward')
    
    print(mktcap_df.query('gvkey == "001690"').tail())

mktcap_df.drop_duplicates(subset=['gvkey', 'date'], keep=False, inplace=True) # just gvkey: ['010846' '030331']

mktcap_df.to_parquet('data/factors_combined.parquet')