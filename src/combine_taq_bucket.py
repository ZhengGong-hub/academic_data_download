import pandas as pd
import gc
import glob
import os
# compute factors
from academic_data_download.factors_lab.taq_builder import TAQBuilder
from academic_data_download.utils.wrds_connect import connect_wrds
import dotenv
dotenv.load_dotenv()

# Connect to WRDS database using credentials from environment variables
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))
taq_builder = TAQBuilder(verbose=True, db=db)

bucket_combos = [
    (100000, 0),
    (100000, 30000),
    (30000, 10000),
    (10000, 2000),
    (2000, 500),
    (500, 0),
]

res_df = None

for bucket_combo in bucket_combos:
    print(f"Combining TAQ data for bucket {bucket_combo}")

    taq_bucket = pd.read_parquet(f'data/taq/processed/taq_retail_markethour_processed_{bucket_combo[0]}_{bucket_combo[1]}.parquet')
    taq_bucket['full_name'] = taq_bucket['sym_root'] + taq_bucket['sym_suffix'].fillna('')


    # drop useless columns, that are not starting with 'ss' or 'sb' or 'sym_root' or 'sym_suffix' or 'date'
    taq_bucket = taq_bucket.loc[:, [col for col in taq_bucket.columns if col.startswith('ss') or col.startswith('sb') or col.startswith('full_name') or col.startswith('sym_root') or col.startswith('sym_suffix') or col.startswith('date')]]
    # change the name by adding buecket name for all columns except date sym_root sym_suffix
    taq_bucket.columns = [f'{col}_{bucket_combo[0]}_{bucket_combo[1]}' if col not in ['date', 'full_name', 'sym_root', 'sym_suffix'] else col for col in taq_bucket.columns]

    print(taq_bucket.head())

    if res_df is None:
        res_df = taq_bucket
    else:
        res_df = pd.merge(res_df, taq_bucket, on=['date', 'full_name', 'sym_root', 'sym_suffix'], how='left')
    print(res_df.head())
    # Free memory from the per-bucket dataframe before loading the next one.
    del taq_bucket
    gc.collect()
    print("--------------------------------")
    print("The bucket combo that is done: ", bucket_combo)
    print("--------------------------------")

print(res_df['sym_root'].unique())

res_df['date'] = pd.to_datetime(res_df['date'])

# Load and merge the TAQ link table to map symbols to PERMNOs
link_table = taq_builder.taq_link_table(
    start_date='2013-01-01',
    permno_list=None,
    symbol_root=None,
    date=None
)
link_table['sym_suffix'] = link_table['sym_suffix'].fillna('')
link_table['date'] = pd.to_datetime(link_table['date'])
res_df = pd.merge(res_df, link_table, on=['sym_root', 'sym_suffix', 'date'], how='inner')

res_df.to_parquet('data/taq/processed/taq_retail_markethour_processed_combined.parquet')
print("saved the combined taq data to data/taq/processed/taq_retail_markethour_processed_combined.parquet")