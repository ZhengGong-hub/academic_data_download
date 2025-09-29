import pandas as pd
import numpy as np
from typing import Callable

from academic_data_download.db_manager.wrds_sql import WRDSManager
from academic_data_download.utils.save_file import save_file
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.utils.col_transform import rolling_sum, fill_forward, merge_mktcap_fundq, fillna_with_0, merge_funda_rdq, shift_n_rows, merge_funda_fundq


class RavenpackBuilder():
    def __init__(self, verbose, db, permno_list, save_path='data/ravenpack'):
        self.verbose = verbose
        self.permno_list = permno_list
        self.wrds_manager = WRDSManager(db, verbose=verbose)
        self.save_path = save_path

    # -------------------------- ravenpack --------------------------
    def ravenpack_equities(self, name='f_ravenpack_equities', path='data/ravenpack', start_year=2009, end_year=2025):
        """
        Ravenpack ESS:
        Ravenpack Event Sentiment Score
        """
        total_df = []

        for year in range(start_year, end_year+1):
            raven_df = self.wrds_manager.get_raven_full_equities(year=year, relevance_threshold=75, event_similarity_days_threshold=90, permno_list=self.permno_list)

            if self.verbose:
                print(f"peeks at the data after calculation of the year {year}!")
                sneak_peek(raven_df)
            total_df.append(raven_df)
        
        total_df = pd.concat(total_df)
        print("finished with data retrieval!")

        # rename 
        total_df.rename(columns={
            'mean_ess': 'f_rp_ess',
            'mean_bmq': 'f_rp_bmq',
            'mean_bee': 'f_rp_bee',
            'mean_bam': 'f_rp_bam',
            'mean_bca': 'f_rp_bca',
            'mean_css': 'f_rp_css',
            'mean_ber': 'f_rp_ber'
        }, inplace=True)
        total_df.rename(columns={'event_count': 'f_rp_event_count'}, inplace=True)

        # to datetime
        total_df['trading_day_et'] = pd.to_datetime(total_df['trading_day_et'])

        # special case handle: in the for loop by the year, each df will contain also the jan.1 of the next year, therefore, for 
        #   2 dfs next to each other, there is a one-day overlap, i.e. Jan.1. 
        #   the next line of code is to handle this case
        total_df = total_df.groupby(['permco', 'trading_day_et']).agg({
            'f_rp_ess': 'mean',
            'f_rp_bmq': 'mean',
            'f_rp_bee': 'mean',
            'f_rp_bam': 'mean',
            'f_rp_bca': 'mean',
            'f_rp_css': 'mean',
            'f_rp_ber': 'mean',
            'f_rp_event_count': 'sum'
        }).reset_index()

        # create a empty df where the date are all the dates in the year and the permco are all the permco in the total_df
        # Create cartesian product of all trading days and all unique permco
        # we create a df, that looks like:
        # ind  date permco
        # 1    2009-01-01 1001 
        # 2    2009-01-02 1001 
        all_days = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        all_permco = total_df['permco'].unique()
        # first loops over all days, for each day, loops over all permco, and creates a df, that looks like:
        daily_df = pd.MultiIndex.from_product([all_days, all_permco], names=['trading_day_et', 'permco']).to_frame(index=False)
        daily_df = pd.merge(daily_df, total_df, on=['permco', 'trading_day_et'], how='left').fillna(0)


        # Event count rolling sums
        daily_df['f_rp_event_count_agg_7d'] = daily_df.groupby('permco')['f_rp_event_count'].transform(lambda x: x.rolling(window=7, min_periods=1).sum())
        daily_df['f_rp_event_count_agg_30d'] = daily_df.groupby('permco')['f_rp_event_count'].transform(lambda x: x.rolling(window=30, min_periods=1).sum())

        # Weighted rolling averages for each column
        for col in ['f_rp_ess', 'f_rp_bmq', 'f_rp_bee', 'f_rp_bam', 'f_rp_bca', 'f_rp_css', 'f_rp_ber']:
            for window in [7, 30]:
                agg_col_name = f'{col}_agg_{window}d'
                # Calculate weighted rolling average for each permco
                daily_df[f'{col}_times_event_count'] = daily_df[col] * daily_df['f_rp_event_count']
                daily_df[agg_col_name] = daily_df.groupby('permco')[f'{col}_times_event_count'].transform(lambda x: x.rolling(window=window, min_periods=1).sum()) / daily_df[f'f_rp_event_count_agg_{window}d']

        if self.verbose:
            print("peeks at the data after calculation!")
            sneak_peek(daily_df)
        if self.permco_list is None:
            save_file(daily_df, name, path=path)
        return daily_df

    def ravenpack_global_macro(self, name='f_ravenpack_global_macro', path='data/ravenpack', start_year=2009, end_year=2025):
        """
        Ravenpack ESS:
        Ravenpack Event Sentiment Score
        """
        total_df = []

        for year in range(start_year, end_year+1):
            raven_df = self.wrds_manager.get_raven_global_macro(year=year, relevance_threshold=75, event_similarity_days_threshold=90)

            if self.verbose:
                print(f"peeks at the data after calculation of the year {year}!")
                sneak_peek(raven_df)
            total_df.append(raven_df)
        
        total_df = pd.concat(total_df)
        print("finished with data retrieval!")
        
        # rename 
        total_df = total_df.rename(columns={
            'mean_ess': 'f_rp_ess', 'event_count': 'f_rp_event_count'})

        # to datetime
        total_df['trading_day_et'] = pd.to_datetime(total_df['trading_day_et'])

        # groupby us_bucket and trading_day_et and do some weighted average weighted by col: event_count
        total_df = total_df.groupby(['us_bucket', 'trading_day_et']).agg({
            'f_rp_ess': 'mean',
            'f_rp_event_count': 'sum'
        }).reset_index()

        # create a empty df where the date are all the dates in the year and the us_bucket are all the us_bucket in the total_df
        all_days = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        us_buckets = ['US', 'RoW']
        daily_df = pd.MultiIndex.from_product([all_days, us_buckets], names=['trading_day_et', 'us_bucket']).to_frame(index=False)
        daily_df = pd.merge(daily_df, total_df, on=['us_bucket', 'trading_day_et'], how='left').fillna(0)

        # weighted score weighted by event_count for all relevant columns
        agg_cols = [
            'f_rp_ess',
        ]
        # Event count rolling sums
        daily_df['f_rp_event_count_agg_7d'] = daily_df.groupby('us_bucket')['f_rp_event_count'].transform(lambda x: x.rolling(window=7, min_periods=1).sum())
        daily_df['f_rp_event_count_agg_30d'] = daily_df.groupby('us_bucket')['f_rp_event_count'].transform(lambda x: x.rolling(window=30, min_periods=1).sum())

        # Weighted rolling averages for each column
        for col in agg_cols:
            for window in [7, 30]:
                agg_col_name = f'{col}_agg_{window}d'
                # Calculate weighted rolling average for each us_bucket
                daily_df[f'{col}_times_event_count'] = daily_df[col] * daily_df['f_rp_event_count']
                daily_df[agg_col_name] = daily_df.groupby('us_bucket')[f'{col}_times_event_count'].transform(lambda x: x.rolling(window=window, min_periods=1).sum()) / daily_df[f'f_rp_event_count_agg_{window}d']

        if self.verbose:
            print("peeks at the data after calculation!")
            sneak_peek(daily_df)
        save_file(daily_df, name, path=path)
        return daily_df
        