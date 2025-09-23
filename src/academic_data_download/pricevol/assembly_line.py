import pandas as pd
import numpy as np

from academic_data_download.db_manager.wrds_sql import get_crsp_daily
from academic_data_download.utils.save_file import save_file
from academic_data_download.utils.necessary_cond_calculation import check_if_calculation_needed
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.utils.col_transform import rolling_sum, fill_forward, merge_mktcap_fundq, fillna_with_0, merge_funda_rdq, shift_n_rows, merge_funda_fundq
from academic_data_download.db_manager.wrds_sql import get_raven_full_equities, get_raven_global_macro

class PriceVolComputer():
    def __init__(self, verbose, db, permno_list):
        self.verbose = verbose
        self.db = db
        self.permno_list = permno_list

    def pricevol_raw(self, name='crsp_daily', path='data/crsp'):
        """
        price and volume
        """
        if not check_if_calculation_needed(name, self.permno_list, save_path=path):
            print(f'Done with {name} loading from cache!')
            self.pricevol_raw = pd.read_parquet(f'{path}/{name}.parquet')
            return self.pricevol_raw
        
        df = get_crsp_daily(self.db, permno_list=self.permno_list)
        df['date'] = pd.to_datetime(df['date'])

        self.pricevol_raw = df

        if self.verbose:
            print("peeks at the data after calculation!")
            sneak_peek(df)
        if self.permno_list is None:
            save_file(df, name, path=path)
        print(f'Done with {name}!')
        return self.pricevol_raw

    def pricevol_calc(self, name='crsp_daily_processed', path='data/crsp'):
        """
        price and volume
        """
        if not check_if_calculation_needed(name, self.permno_list, save_path=path):
            print(f'Done with {name} loading from cache!')
            self.pricevol_processed = pd.read_parquet(f'{path}/{name}.parquet')
            return self.pricevol_processed
        
        pricevol_raw_copy = self.pricevol_raw.copy()
        
        pricevol_raw_copy['adjclose'] = pricevol_raw_copy['prc'] / pricevol_raw_copy['cfacpr']

        # get the ret by aggregation of col: ret 
        pricevol_raw_copy['cum_ret_1y'] = pricevol_raw_copy.groupby(['permno'])['ret'].transform(lambda x: x.rolling(window=252).apply(lambda y: np.prod(1 + y) - 1))
        pricevol_raw_copy['fwrd_ret_1y'] = pricevol_raw_copy.groupby(['permno'])['ret'].transform(lambda x: x.shift(-252))
        
        self.pricevol_processed = pricevol_raw_copy

        if self.verbose:
            print("peeks at the data after calculation!")
            sneak_peek(self.pricevol_processed)
        if self.permno_list is None:
            save_file(self.pricevol_processed, name, path=path)
        print(f'Done with {name}!')
        return self.pricevol_processed
        