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
from academic_data_download.utils.merger import merge_permco_gvkey_link, merge_link_table_crsp

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
        # get link table
        permco_gvkey_link_df = self.wrds_manager.permco_gvkey_link()

        # get pricevol data
        df = self.pricevol_raw()
        print("Converting date column to datetime...")
        df['date'] = pd.to_datetime(df['date'])
        print("Calculating adjusted close price...")
        df['adjclose'] = round(df['prc'] / df['cfacpr'], 3)
        df['ret'] = round(df['ret'], 4)
        df['retx'] = round(df['retx'], 4)

        for _day in [252, 5, 126, 22, 1]:
            print(f"Calculating {_day}-day cumulative return (including dividends)...")
            df[f'cum_ret_{_day}d'] = round(df.groupby(['permno'])['ret'].transform(
                lambda x: x.rolling(window=_day).apply(lambda y: np.prod(1 + y) - 1)
            ), 4)
            print(f"Calculating {_day}-day forward return (including dividends)...")
            df[f'fwd_ret_{_day}d'] = round(df.groupby(['permno'])[f'cum_ret_{_day}d'].transform(
                lambda x: x.shift(-_day)
            ), 4)

            print(f"Calculating {_day}-day cumulative return (excluding dividends)...")
            df[f'cum_ret_{_day}d_excl_div'] = round(df.groupby(['permno'])['retx'].transform(
                lambda x: x.rolling(window=_day).apply(lambda y: np.prod(1 + y) - 1)
            ), 4)
            print(f"Calculating {_day}-day forward return (excluding dividends)...")
            df[f'fwd_ret_{_day}d_excl_div'] = round(df.groupby(['permno'])[f'cum_ret_{_day}d_excl_div'].transform(
                lambda x: x.shift(-_day)
            ), 4)

        # merge with link table
        df = merge_link_table_crsp(crsp_df=df, link_df=permco_gvkey_link_df)
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
        mktcap_df['marketcap'] = mktcap_df['marketcap'].astype(int) / 1000 # in millions (default record in thousands)

        # merge with link table
        mktcap_df = merge_permco_gvkey_link(mktcap_df, permco_gvkey_link_df)
        return mktcap_df
    
    @pricevol
    def live_pricevol(self, name='live_pricevol', start_date=None, end_date=None):
        # get link table
        df = self.wrds_manager.get_secd_daily(start_date=start_date, end_date=end_date)
        df['turnover'] = (df['cshtrd'] / df['cshoc'] * 100).round(2) # in percentage
        df['dvol'] = (df['prccd'] * df['cshtrd'] / 1e9).round(2) # in billions
        df['mktcap'] = (df['prccd'] * df['cshoc'] / 1e9).round(2) # in billions

        # moving averages 
        # TODO

        print(df.query('mktcap > 5 and turnover > 10 and tpci != "%" and prccd > 10 and prccd < 200').sort_values('turnover', ascending=False).head(50))
        print(df.columns.to_list())
        # print(df.sort_values("mktcap", ascending=False).head(50))
        assert False