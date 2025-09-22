import pandas as pd
import glob 
import time 
from utils.col_transform import merge_mktcap_fundq

factor_addrs = glob.glob('data/factors/*.parquet')

mktcap_df = pd.read_parquet('data/crsp/marketcap.parquet')
mktcap_df = mktcap_df.loc[mktcap_df['gvkey'] == '001690']

print(mktcap_df.head())

for addr in factor_addrs:
    # idenitify column name starting with "f_"
    df = pd.read_parquet(addr)
    df = df.loc[df['gvkey'] == '001690']
    if 'date' not in df.columns:
        # if date is not in df
        df = df[['gvkey', 'rdq', [col for col in df.columns if col.startswith('f_')][0]]]
        mktcap_df = merge_mktcap_fundq(mktcap_df, df).drop(columns=['rdq'])
        print("column date is there")
        print(df)
        print(mktcap_df)
    else:
        # if date is in df,
        df = df[['gvkey', 'date', [col for col in df.columns if col.startswith('f_')][0]]]
        mktcap_df = pd.merge_asof(mktcap_df, df, on=['date'], by=['gvkey'], direction='backward')
        print("column date is not there")
        print(df)
        print(mktcap_df)
    
    print(mktcap_df.query('gvkey == "001690"').tail(50))


#mktcap_df.to_parquet('data/factors_combined.parquet')