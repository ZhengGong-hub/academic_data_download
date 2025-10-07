# combine all the data into one file

# we require 1-1 relation between permco and gvkey at a given date. 
# we use permco as identifer variable
from datetime import datetime
import pandas as pd
import os
import glob

# compute factors
from academic_data_download.factors_lab.taq_builder import TAQBuilder
from academic_data_download.utils.wrds_connect import connect_wrds
import os
import dotenv
dotenv.load_dotenv()

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

permno_list = None
# permno_list = [
#     # 14542, # Google class C
#     # 14593, # apple
#     17778, # berkshire class A
#     ]
taq_builder = TAQBuilder(verbose=True, db=db)

# hyperparameters
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

if False:
    print("Step 1: Loading taq data...")
    taq_df = pd.read_parquet(taq_path).sort_values(by=['date'])
    taq_df['date'] = pd.to_datetime(taq_df['date'])
    taq_df['sym_suffix'] = taq_df['sym_suffix'].fillna('')

    link_table = taq_builder.taq_link_table(start_date='2013-01-01', permno_list=permno_list, symbol_root=taq_df['sym_root'].unique(), date=None)
    link_table['sym_suffix'] = link_table['sym_suffix'].fillna('')
    link_table['date'] = pd.to_datetime(link_table['date'])
    print(link_table)
    taq_df = pd.merge(taq_df, link_table, on=['sym_root', 'sym_suffix', 'date'], how='inner')

    # load price 
    pricevol = pd.read_parquet(pricevol_path)[['permno', 'date', 'vol']]
    pricevol['date'] = pd.to_datetime(pricevol['date'])
    pricevol = pricevol[pricevol['date'] >= f'{start_year}-01-01']
    
    taq_df = pd.merge(taq_df, pricevol, left_on=['permno', 'date'], right_on=['permno', 'date'], how='left')

    taq_df['n_net'] = round((taq_df['nob'] - taq_df['nos']) / (taq_df['nob'] + taq_df['nos']), 3)
    taq_df['s_net'] = round((taq_df['sb'] - taq_df['ss']) / (taq_df['sb'] + taq_df['ss']), 3)
    taq_df['v_net'] = round((taq_df['vb'] - taq_df['vs']) / (taq_df['vb'] + taq_df['vs']), 3)
    taq_df['retail_magnitude'] = 1e6 * (taq_df['sb'] - taq_df['ss']) / taq_df['vol']


    taq_df['full_name'] = taq_df['sym_root'] + taq_df['sym_suffix']
    for _day in range(1, 23):
        # taq_df[f'nob_in_{_day}d'] = taq_df.groupby('full_name')['nob'].transform(lambda x: x.shift(-_day))
        # taq_df[f'nos_in_{_day}d'] = taq_df.groupby('full_name')['nos'].transform(lambda x: x.shift(-_day))

        # taq_df[f'sb_in_{_day}d'] = taq_df.groupby('full_name')['sb'].transform(lambda x: x.shift(-_day))
        # taq_df[f'ss_in_{_day}d'] = taq_df.groupby('full_name')['ss'].transform(lambda x: x.shift(-_day))
        
        # taq_df[f'vb_in_{_day}d'] = taq_df.groupby('full_name')['vb'].transform(lambda x: x.shift(-_day))
        # taq_df[f'vs_in_{_day}d'] = taq_df.groupby('full_name')['vs'].transform(lambda x: x.shift(-_day))
        
        # taq_df[f'vol_in_{_day}d'] = taq_df.groupby('full_name')['vol'].transform(lambda x: x.shift(-_day))

        taq_df[f'n_net_in_{_day}d'] = taq_df.groupby('full_name')['n_net'].transform(lambda x: x.shift(-_day))
        taq_df[f's_net_in_{_day}d'] = taq_df.groupby('full_name')['s_net'].transform(lambda x: x.shift(-_day))
        taq_df[f'v_net_in_{_day}d'] = taq_df.groupby('full_name')['v_net'].transform(lambda x: x.shift(-_day))
        taq_df[f'retail_magnitude_in_{_day}d'] = taq_df.groupby('full_name')['retail_magnitude'].transform(lambda x: x.shift(-_day))

    print(taq_df.query('sym_root == "AAPL"'))
    taq_df.to_parquet("data/combined/taq_temp.parquet")
    assert False



print("Step 3: Loading price/volume data...")
# load pricevol
price_target_all_data = pd.read_parquet(price_target_all_data_path)
price_target_all_data = price_target_all_data.query(f"trading_day_et >= '{start_year}-01-01'")
price_target_all_data['ann_deemed_date'] = pd.to_datetime(price_target_all_data['ann_deemed_date'])
print(f"  Price/volume data loaded from {price_target_all_data_path}.")
print(price_target_all_data.columns)

earnings_date = pd.read_parquet('/Users/zhenggong/academic_data_download/data/factors/single_factor/f_ep.parquet')
earnings_date['date'] = pd.to_datetime(earnings_date['date'])
price_target_all_data = pd.merge(price_target_all_data, earnings_date[['permco', 'date', 'rdq']], left_on=['permco', 'ann_deemed_date'], right_on=['permco', 'date'], how='inner')
print(price_target_all_data)

# import taq data
taq_df = pd.read_parquet('data/combined/taq_temp.parquet')
taq_df['date'] = pd.to_datetime(taq_df['date'])
print(taq_df)
print(taq_df.columns)

price_target_all_data = pd.merge(price_target_all_data, taq_df, left_on=['permno', 'ann_deemed_date'], right_on=['permno', 'date'], how='inner')
print(price_target_all_data)
price_target_all_data.to_parquet('data/combined/price_target_all_data_with_taq.parquet')
assert False

print("Step 6: Merging all data sources...")
# combine all the data into one file
print("  Merging pricevol and taq...")
print(pricevol_df)
assert False

print(f"Step 7: Saving combined data to {combined_path} ...")
df.to_parquet(combined_path)
print(f'Saved to {combined_path}')