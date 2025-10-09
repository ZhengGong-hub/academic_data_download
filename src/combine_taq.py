"""
This script combines TAQ (Trade and Quote) data with price target and earnings data for US equities.
It performs the following steps:

1. Loads TAQ data and processes symbol information.
2. Loads and merges the TAQ link table to map symbols to PERMNOs.
3. Loads price/volume data and merges it with TAQ data.
4. Computes shifted TAQ and volume features for various time windows.
5. Loads detailed price target revision data and filters by date.
6. Loads earnings announcement dates and merges with price target data.
7. Merges the enriched price target data with TAQ data on PERMNO and date.
8. Saves the final combined dataset to disk.

Input files:
    - data/taq/taq_retail_markethour_100000_0_2013-01-01_2024-12-31.parquet
    - data/pricevol/pricevol_processed.parquet
    - data/combined/price_target_detail_all_data.parquet
    - data/factors/single_factor/f_ep.parquet

Output files:
    - data/combined/price_target_all_data_with_taq.parquet

Dependencies:
    - academic_data_download.factors_lab.taq_builder
    - academic_data_download.utils.wrds_connect
    - dotenv
    - pandas
    - os, glob
"""

import pandas as pd
import os
import glob

# compute factors
from academic_data_download.factors_lab.taq_builder import TAQBuilder
from academic_data_download.utils.wrds_connect import connect_wrds
import dotenv
dotenv.load_dotenv()

# Connect to WRDS database using credentials from environment variables
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))
taq_builder = TAQBuilder(verbose=True, db=db)

# Hyperparameters and file paths
start_year = 2013
pricevol_path = 'data/pricevol/pricevol_processed.parquet'
ravenpack_equities_path = 'data/ravenpack/f_rp_ess.parquet'
ravenpack_global_macro_path = 'data/ravenpack/f_rp_global_macro.parquet'
factors_path = 'data/factors/combined/factors_combined.parquet'
bbg_macro_var_path = glob.glob('data/Macro variables/*.xlsx')
taq_path = 'data/taq/taq_retail_markethour_100000_0_2013-01-01_2024-12-31.parquet'

price_target_all_data_path = 'data/combined/price_target_detail_all_data.parquet'

os.makedirs('data/combined', exist_ok=True)
combined_path = 'data/combined/taq_combined.parquet'

if __name__ == "__main__":

    # Step 1: Load TAQ data and process symbol information
    print("Step 1: Loading taq data...")
    taq_df = pd.read_parquet(taq_path).sort_values(by=['date'])
    taq_df['date'] = pd.to_datetime(taq_df['date'])
    taq_df.sort_values(by=['date'], inplace=True)
    taq_df['sym_suffix'] = taq_df['sym_suffix'].fillna('')
    taq_df['full_name'] = taq_df['sym_root'] + taq_df['sym_suffix']

    # Step 2: Load and merge the TAQ link table to map symbols to PERMNOs
    print("Step 2: Loading link table... merging with taq data")
    link_table = taq_builder.taq_link_table(
        start_date='2013-01-01',
        permno_list=None,
        symbol_root=taq_df['sym_root'].unique(),
        date=None
    )
    link_table['sym_suffix'] = link_table['sym_suffix'].fillna('')
    link_table['date'] = pd.to_datetime(link_table['date'])
    taq_df = pd.merge(taq_df, link_table, on=['sym_root', 'sym_suffix', 'date'], how='inner')

    # Step 3: Load price/volume data and merge with TAQ data
    print("Step 3: Loading pricevol data... merging with taq data")
    pricevol = pd.read_parquet(pricevol_path)[['permno', 'date', 'vol']]
    pricevol['date'] = pd.to_datetime(pricevol['date'])
    pricevol = pricevol[pricevol['date'] >= f'{start_year}-01-01']
    pricevol['vol'] = round(pricevol['vol']/1000, 0)  # Convert volume to thousands

    taq_df = pd.merge(taq_df, pricevol, left_on=['permno', 'date'], right_on=['permno', 'date'], how='left')
    
    # Step 4: Compute shifted TAQ and volume features for various time windows
    for _day in list(range(-5, 23)) + [-66, -22, 66, 132, 198, 252]:
        taq_df[f'no_in_{_day}d'] = taq_df.groupby('full_name')['no'].transform(lambda x: x.shift(-_day))
        taq_df[f'nob_in_{_day}d'] = taq_df.groupby('full_name')['nob'].transform(lambda x: x.shift(-_day))
        taq_df[f'nos_in_{_day}d'] = taq_df.groupby('full_name')['nos'].transform(lambda x: x.shift(-_day))

        taq_df[f's_in_{_day}d'] = taq_df.groupby('full_name')['s'].transform(lambda x: x.shift(-_day))
        taq_df[f'sb_in_{_day}d'] = taq_df.groupby('full_name')['sb'].transform(lambda x: x.shift(-_day))
        taq_df[f'ss_in_{_day}d'] = taq_df.groupby('full_name')['ss'].transform(lambda x: x.shift(-_day))
        
        taq_df[f'vol_in_{_day}d'] = taq_df.groupby('full_name')['vol'].transform(lambda x: x.shift(-_day))

    # Step 5: Load detailed price target revision data and filter by date
    print("Step 4: Loading price_target_all_data... ")
    price_target_all_data = pd.read_parquet(price_target_all_data_path)
    price_target_all_data = price_target_all_data.query(f"trading_day_et >= '{start_year}-01-01'")
    price_target_all_data['ann_deemed_date'] = pd.to_datetime(price_target_all_data['ann_deemed_date'])
    print(f"  Price/volume data loaded from {price_target_all_data_path}.")
    print("price_target_all_data: ", price_target_all_data)

    # Step 6: Load earnings announcement dates and merge with price target data
    print("Step 5: Loading earnings date... merging with price_target_all_data")
    earnings_date = pd.read_parquet('data/factors/single_factor/f_ep.parquet')
    earnings_date['date'] = pd.to_datetime(earnings_date['date'])
    price_target_all_data = pd.merge(
        price_target_all_data,
        earnings_date[['permco', 'date', 'rdq']],
        left_on=['permco', 'ann_deemed_date'],
        right_on=['permco', 'date'],
        how='inner'
    )
    print("price_target_all_data with earnings date: ", price_target_all_data)

    # Step 7: Merge the enriched price target data with TAQ data on PERMNO and date
    print("Step 6: Taq data merging with price_target_all_data")
    price_target_all_data = pd.merge(
        price_target_all_data,
        taq_df,
        left_on=['permno', 'ann_deemed_date'],
        right_on=['permno', 'date'],
        how='inner'
    )
    print("price_target_all_data with taq data: ", price_target_all_data)

    # Step 8: Save the final combined dataset to disk
    price_target_all_data.to_parquet('data/combined/price_target_all_data_with_taq.parquet')
    print("saved price_target_all_data with taq data to data/combined/price_target_all_data_with_taq.parquet")