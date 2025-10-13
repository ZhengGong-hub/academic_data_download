"""
This script combines price target data from analyst estimates with a master dataset of stock data.
It performs the following steps:

1. Loads the main stock data (`all_data`) and ensures key columns are of the correct type.
2. Loads the price target summary data, aligns it by date and security, and merges it with the main data using a backward as-of join (within 90 days).
   The merged result is saved to disk, containing only rows with available median price target (`medptg`).
3. Loads the detailed price target revision data, ensures correct types, and merges it with the main data using a forward as-of join (within 10 days).
   The merged result is saved to disk, containing only rows with available price (`prc`).

Input files:
    - ../data/combined/all_data.parquet
    - ../data/analysts_estimate/price_target_summary.parquet
    - ../data/analysts_estimate/price_target_detail_revision.parquet

Output files:
    - ../data/combined/all_data_with_pt_summary.parquet
    - ../data/combined/price_target_detail_all_data.parquet
"""

import pandas as pd 

if __name__ == "__main__":

    # Load and preprocess the main stock data
    print("Reading all_data from parquet...")
    all_data = pd.read_parquet('data/combined/all_data.parquet')
    print("Converting trading_day_et to datetime...")
    all_data['trading_day_et'] = pd.to_datetime(all_data['trading_day_et'])
    all_data['permno'] = all_data['permno'].astype(int)
    all_data['permco'] = all_data['permco'].astype(int)
    print("Sorting by trading_day_et...")
    all_data = all_data.sort_values(by='trading_day_et', kind="quicksort")
    print("Preview of all_data after preprocessing:")
    print(all_data.head())

    # # Load and preprocess the price target summary data
    # price_target_summary = pd.read_parquet('../data/analysts_estimate/price_target_summary.parquet')
    # price_target_summary['statpers'] = pd.to_datetime(price_target_summary['statpers'])
    # price_target_summary.sort_values(by='statpers', inplace=True)
    # print(price_target_summary.head())

    # # Merge all_data with price target summary using a backward as-of join (within 90 days)
    # all_data_with_pt = pd.merge_asof(
    #     all_data,
    #     price_target_summary[['permno', 'permco', 'statpers', 'medptg']],
    #     left_on='trading_day_et',
    #     right_on='statpers',
    #     by=['permno', 'permco'],
    #     tolerance=pd.Timedelta("90d"),
    #     direction='backward'
    # )
    # all_data_with_pt.dropna(subset=['medptg']).to_parquet('../data/combined/all_data_with_pt_summary.parquet')
    if True:
        # Load and preprocess the detailed price target revision data
        price_target_detail = pd.read_parquet('data/analysts_estimate/pt_detail_with_eps_estimate.parquet')
        price_target_detail.sort_values(by='ann_deemed_date', inplace=True)
        price_target_detail['permno'] = price_target_detail['permno'].astype(int)
        price_target_detail['permco'] = price_target_detail['permco'].astype(int)
        price_target_detail['ann_deemed_date'] = pd.to_datetime(price_target_detail['ann_deemed_date'])
        price_target_detail['last_ann_deemed_date'] = pd.to_datetime(price_target_detail['last_ann_deemed_date'])
        price_target_detail = price_target_detail.dropna(subset=['permco', 'permno', 'ann_deemed_date'], how='any')

        print("Merging price target detail with all_data using a forward as-of join (within 10 days)...")
        # Merge price target detail with all_data using a forward as-of join (within 10 days)
        price_target_detail_all_data = pd.merge_asof(
            price_target_detail,
            all_data,
            left_on='ann_deemed_date',
            right_on='trading_day_et',
            by=['permno', 'permco'],
            tolerance=pd.Timedelta("5d"),
            direction='forward'
        ).drop(columns=['trading_day_et'])

        print("Merging price target detail with all_data using a backward as-of join (within 10 days)...")
        # Merge price target detail with all_data using a forward as-of join (within 10 days)
        price_target_detail_all_data["last_ann_deemed_date"] = price_target_detail_all_data[
            "last_ann_deemed_date"
        ].fillna(pd.Timestamp("2099-12-31"))
        price_target_detail_all_data.sort_values(by='last_ann_deemed_date', inplace=True)
        price_target_detail_all_data = pd.merge_asof(
            price_target_detail_all_data,
            all_data[['permno', 'permco', 'trading_day_et', 'prc', 'ret']].rename(columns={'prc': 'last_prc', 'ret': 'last_ret'}),
            left_on='last_ann_deemed_date',
            right_on='trading_day_et',
            by=['permno', 'permco'],
            tolerance=pd.Timedelta("5d"),
            direction='backward'
        ).drop(columns=['trading_day_et'])

        price_target_detail_all_data.dropna(subset=['prc']).to_parquet('data/combined/price_target_detail_all_data.parquet')