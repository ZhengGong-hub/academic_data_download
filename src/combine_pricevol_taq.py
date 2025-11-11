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
cutoff_upper = 100000
cutoff_lower = 0
pricevol_path = 'data/pricevol/pricevol_processed_past_prc.parquet'
# ravenpack_equities_path = 'data/ravenpack/f_rp_ess.parquet'
# ravenpack_global_macro_path = 'data/ravenpack/f_rp_global_macro.parquet'
factors_path = 'data/factors/combined/factors_combined.parquet'
bbg_macro_var_path = glob.glob('data/Macro variables/*.xlsx')
taq_path = f'data/taq/processed/taq_retail_markethour_processed_{cutoff_upper}_{cutoff_lower}.parquet'

# price_target_all_data_path = 'data/combined/price_target_detail_all_data.parquet'

os.makedirs('data/combined', exist_ok=True)
combined_path = f'data/combined/pricevol_processed_past_with_taq.parquet'

if __name__ == "__main__":

    # Step 1: Load TAQ data and process symbol information
    print("Step 1: Loading taq data...")
    taq_df = pd.read_parquet(taq_path).sort_values(by=['date'])
    taq_df['date'] = pd.to_datetime(taq_df['date'])
    taq_df.sort_values(by=['date'], inplace=True)
    taq_df['sym_suffix'] = taq_df['sym_suffix'].fillna('')
    taq_df['full_name'] = taq_df['sym_root'] + taq_df['sym_suffix']

    # Step 3: Load price/volume data and merge with TAQ data
    print("Step 3: Loading pricevol data... merging with taq data")
    pricevol = pd.read_parquet(pricevol_path)
    pricevol['date'] = pd.to_datetime(pricevol['date'])
    pricevol = pricevol[pricevol['date'] >= f'{start_year}-01-01']
    pricevol['vol'] = round(pricevol['vol']/1000, 0)  # Convert volume to thousands

    # Step 4: Compute shifted TAQ and volume features for various time windows
    # for _day in list(range(-5, 23)) + [-66, -22, 66, 132, 198, 252]:
    for _day in list(range(-5, 10)) + [-66, -22, 66, 132, 252]:
        pricevol[f'vol_in_{_day}d'] = pricevol.groupby('permno')['vol'].transform(lambda x: x.shift(-_day))

    taq_df = pd.merge(taq_df, pricevol, left_on=['permno', 'date'], right_on=['permno', 'date'], how='left')

    # Step 6: Load earnings announcement dates and merge with price target data
    print("Step 5: Loading earnings date... merging with price_target_all_data")
    earnings_date = pd.read_parquet('data/factors/single_factor/f_ep.parquet')
    earnings_date['date'] = pd.to_datetime(earnings_date['date'])
    all_data = pd.merge(
        taq_df,
        earnings_date[['permco', 'date', 'rdq']],
        on=['permco', 'date'],
        how='inner'
    )
    print("taq_df with earnings date: ", all_data)

    # Step 8: Save the final combined dataset to disk
    all_data.to_parquet(combined_path)
    print(f"saved taq_df with pricevol data to {combined_path}")