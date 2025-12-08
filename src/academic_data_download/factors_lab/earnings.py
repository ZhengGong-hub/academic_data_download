import pandas as pd
import numpy as np
from typing import Callable
import inspect
from functools import wraps

from academic_data_download.db_manager.wrds_sql import WRDSManager
from academic_data_download.utils.save_file import save_file
from academic_data_download.utils.necessary_cond_calculation import check_if_calculation_needed
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.factors_lab.pricevol_builder import PriceVolComputer
from academic_data_download.factors_lab.analyst_estimation_builder import AnalystEstimationBuilder
from academic_data_download.factors_lab.pricevol_builder import PriceVolComputer

class EarningsBuilder():
    def __init__(self, verbose, db, gvkey_list, save_path='data/earnings'):
        self.verbose = verbose
        self.gvkey_list = gvkey_list
        self.wrds_manager = WRDSManager(db, verbose=verbose)
        self.save_path = save_path
        fb = AnalystEstimationBuilder(permno_list=None, verbose=False, db=db)
        self.earnings_date = fb.eps_act_qtr()[['permno', 'permco', 'ann_ts', 'ann_deemed_date']]
        self.earnings_date['ann_deemed_date'] = pd.to_datetime(self.earnings_date['ann_deemed_date'])

        self.trading_date = PriceVolComputer(permno_list=None, verbose=False, db=db).marketcap(name='marketcap')[['gvkey', 'permco', 'date']]
        self.trading_date['date'] = pd.to_datetime(self.trading_date['date'])


    def trading_date_with_adjacent_earnings_date(self, name='trading_date_with_adjacent_earnings_date'):
        """
        """
        trading_date = self.trading_date.copy()

        trading_date = pd.merge_asof(trading_date, self.earnings_date[['permno', 'permco', 'ann_deemed_date']].rename(columns={'ann_deemed_date': 'last_earnings_deemed_date'}), left_on='date', right_on='last_earnings_deemed_date', by=['permco'], direction='backward')
        trading_date = pd.merge_asof(trading_date, self.earnings_date[['permco', 'ann_deemed_date']].rename(columns={'ann_deemed_date': 'next_earnings_deemed_date'}), left_on='date', right_on='next_earnings_deemed_date', by=['permco'], direction='forward')
        
        trading_date.to_parquet(f'{self.save_path}/{name}.parquet')
        return trading_date