from locale import D_FMT
import pandas as pd
import numpy as np
from typing import Callable
from functools import wraps
import inspect

from academic_data_download.utils.save_file import save_file
from academic_data_download.utils.necessary_cond_calculation import check_if_calculation_needed
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.db_manager.wrds_sql import WRDSManager
from academic_data_download.utils.merger import merge_permco_gvkey_link

def pricevol(fn: Callable) -> Callable:
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        name = kwargs.get('name', 'crsp_daily')
        if not check_if_calculation_needed(name, self.permno_list, save_path=self.save_path):
            print(f'Done with {name} loading from cache!')
            df = pd.read_parquet(f'{self.save_path}/{name}.parquet')
            return df

        df = fn(self, *args, **kwargs)
        if self.verbose:
            print("peeks at the data after calculation!")
            sneak_peek(df)
        if self.permno_list is None:
            save_file(df, name, path=self.save_path)
        print(f'Done with {name}!')
        return df
    return wrapper

class PriceVolComputer():
    def __init__(self, verbose, db, permno_list):
        self.verbose = verbose
        self.wrds_manager = WRDSManager(db, verbose=verbose)
        self.permno_list = permno_list
        self.save_path = 'data/pricevol'

    @pricevol
    def pricevol_raw(self, name='pricevol_raw'):
        """
        Retrieve raw price and volume data.
        """
        df = self.wrds_manager.get_crsp_daily(cache_path=f'{self.save_path}/pricevol_raw.parquet', permno_list=self.permno_list)
        return df

    @pricevol
    def pricevol_processed(self, name='pricevol_processed'):
        """
        Retrieve raw price and volume data, then calculate adjusted close, cumulative and forward returns.
        """
        df = self.pricevol_raw()
        print("Converting date column to datetime...")
        df['date'] = pd.to_datetime(df['date'])
        print("Calculating adjusted close price...")
        df['adjclose'] = df['prc'] / df['cfacpr']

        print("Calculating 1-year cumulative return (including dividends)...")
        df['cum_ret_1y'] = df.groupby(['permno'])['ret'].transform(
            lambda x: x.rolling(window=252).apply(lambda y: np.prod(1 + y) - 1)
        )
        print("Calculating 1-year forward return (including dividends)...")
        df['fwd_ret_1y'] = df.groupby(['permno'])['cum_ret_1y'].transform(
            lambda x: x.shift(-252)
        )

        print("Calculating 1-year cumulative return (excluding dividends)...")
        df['cum_ret_1y_excl_div'] = df.groupby(['permno'])['retx'].transform(
            lambda x: x.rolling(window=252).apply(lambda y: np.prod(1 + y) - 1)
        )
        print("Calculating 1-year forward return (excluding dividends)...")
        df['fwd_ret_1y_excl_div'] = df.groupby(['permno'])['cum_ret_1y_excl_div'].transform(
            lambda x: x.shift(-252)
        )
        return df

    @pricevol
    def marketcap(self, name='marketcap'):
        # get link table
        permco_gvkey_link_df = self.wrds_manager.permco_gvkey_link()

        # get pricevol data
        pricevol_df = self.pricevol_raw()

        # calculate marketcap
        pricevol_df['marketcap_permno'] = pricevol_df['prc'] * pricevol_df['shrout']

        # sum across different permno for each permco (account for different share class)
        mktcap_df = pricevol_df.groupby(['date','permco']).agg({'marketcap_permno': 'sum'}).reset_index()
        mktcap_df.rename(columns={'marketcap_permno': 'marketcap'}, inplace=True)
        
        # round to integer
        mktcap_df['marketcap'] = mktcap_df['marketcap'].astype(int)

        # merge with link table
        mktcap_df = merge_permco_gvkey_link(mktcap_df, permco_gvkey_link_df)
        return mktcap_df