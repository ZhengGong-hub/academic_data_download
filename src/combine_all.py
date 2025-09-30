# combine all the data into one file

# we require 1-1 relation between permco and gvkey at a given date. 
# we use permco as identifer variable
from datetime import datetime
import pandas as pd
import os
import glob

# hyperparameters
start_year = 2010
pricevol_path = 'data/pricevol/pricevol_processed.parquet'
ravenpack_equities_path = 'data/ravenpack/f_rp_ess.parquet'
ravenpack_global_macro_path = 'data/ravenpack/f_rp_global_macro.parquet'
factors_path = 'data/factors/combined/factors_combined.parquet'
bbg_macro_var_path = glob.glob('data/Macro variables/*.xlsx')

os.makedirs('data/combined', exist_ok=True)
combined_path = 'data/combined/all_data.parquet'

print("Step 1: Loading macro variables...")
# load macro variables where the column trading_day_et is every day since start_year
macro_var_df = pd.DataFrame(columns=['trading_day_et'], data=pd.date_range(start=f'{start_year-1}-01-01', end=datetime.now().strftime('%Y-%m-%d'), freq='D'))

for path in bbg_macro_var_path:
    print(f"  Reading macro variable file: {path}")
    macro_var_name = path.split('/')[-1].split('.')[0]
    df = pd.read_excel(path, sheet_name="Tabelle1")
    df = df.rename(columns={'Last Price': f'f_{macro_var_name}', 'Date': 'trading_day_et'})
    df.sort_values(by=['trading_day_et'], inplace=True)
    df = df[df['trading_day_et'] >= f'{start_year-1}-01-01']
    df['trading_day_et'] = pd.to_datetime(df['trading_day_et'])

    macro_var_df = pd.merge(macro_var_df, df[['trading_day_et', f'f_{macro_var_name}']], on=['trading_day_et'], how='left')
macro_var_df.fillna(method='ffill', inplace=True)

to_divide_by_100_cols = ['f_3mTreasury', 'f_10yTreasury', 'f_CreditSpread', 'f_ConsumerPriceIndex']
macro_var_df[to_divide_by_100_cols] = macro_var_df[to_divide_by_100_cols] / 100

print("Macro variables loaded. Last few rows:")
print(macro_var_df.tail())


print("Step 2: Loading factors data...")
# load factors 
factors_df = pd.read_parquet(factors_path)
print(f"  Factors data loaded from {factors_path}.")
factors_df = factors_df.rename(columns={'date': 'trading_day_et'})
factors_df = factors_df[factors_df['trading_day_et'] >= f'{start_year}-01-01']
factors_df['trading_day_et'] = pd.to_datetime(factors_df['trading_day_et'])
# check whether there are duplicates on ['permco', 'date'] by assert False
dup_permco_list = factors_df[factors_df.duplicated(subset=['permco', 'trading_day_et'])]['permco'].unique()
print("  Checking for duplicate permco-date pairs in factors...")
print("  We drop the following permco: ", dup_permco_list)
factors_df = factors_df[~factors_df['permco'].isin(dup_permco_list)]
assert factors_df.duplicated(subset=['permco', 'trading_day_et']).sum() == 0
print("  Factors data after cleaning:")
print(factors_df.head())


print("Step 3: Loading price/volume data...")
# load pricevol
pricevol_df = pd.read_parquet(pricevol_path)
print(f"  Price/volume data loaded from {pricevol_path}.")
pricevol_df = pricevol_df.rename(columns={'date': 'trading_day_et'})
pricevol_df = pricevol_df[pricevol_df['trading_day_et'] >= f'{start_year}-01-01']
pricevol_df['trading_day_et'] = pd.to_datetime(pricevol_df['trading_day_et'])
print("  Price/volume data after cleaning:")
print(pricevol_df.head())

print("Step 4: Loading RavenPack equities data...")
# load ravenpack equities
ravenpack_df = pd.read_parquet(ravenpack_equities_path)
# fillna with 0 for the columns starts with 'f_rp_'
ravenpack_df[ravenpack_df.columns[ravenpack_df.columns.str.startswith('f_rp_')]] = ravenpack_df[ravenpack_df.columns[ravenpack_df.columns.str.startswith('f_rp_')]].fillna(0)
print(f"  RavenPack equities data loaded from {ravenpack_equities_path}.")
ravenpack_df = ravenpack_df[ravenpack_df['trading_day_et'] >= f'{start_year}-01-01']
# drop certain columns that are not needed
_to_drop_cols = ['bmq', 'bee', 'bam', 'bca', 'css', 'ber']
ravenpack_df = ravenpack_df.drop(columns=[f'f_rp_{col}_agg_7d' for col in _to_drop_cols])
ravenpack_df = ravenpack_df.drop(columns=[f'f_rp_{col}_agg_30d' for col in _to_drop_cols])
ravenpack_df = ravenpack_df.drop(columns=[f'f_rp_{col}_times_event_count' for col in _to_drop_cols])
ravenpack_df = ravenpack_df.drop(columns=['f_rp_ess_times_event_count'])
# check whether there are duplicates on ['permco', 'date'] by assert False
assert ravenpack_df.duplicated(subset=['permco', 'trading_day_et']).sum() == 0
print("  RavenPack equities data after cleaning:")
print(ravenpack_df.head())

print("Step 5: Loading RavenPack global macro data...")
# load ravenpack global macro
ravenpack_global_macro_df = pd.read_parquet(ravenpack_global_macro_path)
print(f"  RavenPack global macro data loaded from {ravenpack_global_macro_path}.")
ravenpack_global_macro_df = ravenpack_global_macro_df[ravenpack_global_macro_df['trading_day_et'] >= f'{start_year}-01-01']

ravenpack_global_macro_df_us = ravenpack_global_macro_df[ravenpack_global_macro_df['us_bucket'] == 'US'].drop(columns=['us_bucket'])
ravenpack_global_macro_df_us.columns = [col.replace('f_rp_', 'f_rp_us_') for col in ravenpack_global_macro_df_us.columns]
ravenpack_global_macro_df_row = ravenpack_global_macro_df[ravenpack_global_macro_df['us_bucket'] == 'RoW'].drop(columns=['us_bucket'])
ravenpack_global_macro_df_row.columns = [col.replace('f_rp_', 'f_rp_row_') for col in ravenpack_global_macro_df_row.columns]
# drop certain columns that are not needed
ravenpack_global_macro_df_us = ravenpack_global_macro_df_us.drop(columns=['f_rp_us_ess_times_event_count'])
ravenpack_global_macro_df_row = ravenpack_global_macro_df_row.drop(columns=['f_rp_row_ess_times_event_count'])
print("  RavenPack global macro US data after cleaning:")
print(ravenpack_global_macro_df_us.head())

print("Step 6: Merging all data sources...")
# combine all the data into one file
print("  Merging pricevol and factors...")
df = pd.merge(pricevol_df, factors_df, on=['permco', 'trading_day_et'], how='left')
print("  Merging with macro variables...")
df = pd.merge(df, macro_var_df, on=['trading_day_et'], how='left')
print("  Merging with RavenPack equities...")
df = pd.merge(df, ravenpack_df, on=['permco', 'trading_day_et'], how='left')
print("  Merging with RavenPack global macro US...")
df = pd.merge(df, ravenpack_global_macro_df_us, on=['trading_day_et'], how='left')
print("  Merging with RavenPack global macro RoW...")
df = pd.merge(df, ravenpack_global_macro_df_row, on=['trading_day_et'], how='left')
print("  Final merged dataframe preview:")
print("the shape of the dataframe is: ", df.shape)
print(df.head())

print(f"Step 7: Saving combined data to {combined_path} ...")
df.to_parquet(combined_path)
print(f'Saved to {combined_path}')