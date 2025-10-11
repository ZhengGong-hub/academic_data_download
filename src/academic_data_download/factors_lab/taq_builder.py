import pandas as pd
import numpy as np
from typing import Callable
import os
import glob

from academic_data_download.db_manager.wrds_sql import WRDSManager
from academic_data_download.utils.save_file import save_file
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.utils.col_transform import rolling_sum, fill_forward, merge_mktcap_fundq, fillna_with_0, merge_funda_rdq, shift_n_rows, merge_funda_fundq

from academic_data_download.factors_lab.analyst_estimation_builder import AnalystEstimationBuilder
from academic_data_download.factors_lab.pricevol_builder import PriceVolComputer


class TAQBuilder():
    def __init__(self, verbose, db, save_path='data/taq'):
        self.verbose = verbose
        self.wrds_manager = WRDSManager(db, verbose=verbose)
        self.save_path = save_path
        self.db = db
        self.analyst_estimation_builder = AnalystEstimationBuilder(verbose=verbose, db=db, permno_list=None)
        self.pricevol_builder = PriceVolComputer(verbose=verbose, db=db, permno_list=[14593])

    # -------------------------- TAQ --------------------------
    def taq_peek(self, name='f_taq_peek', path='data/taq', sym_root_list=None, year=None, date=None):
        """
        TAQ:
        """
        df = self.wrds_manager.get_taq_peek(sym_root_list=sym_root_list, year=year, date=date)
        # # keep the column that incldues the word "taq"
        # df = df[df['library'].str.contains('taq', case=False, na=False)]
        # df = df.query('library == "taqm_2025"')
        print(df)
        return df

    def taq_tables(self, name='f_taq_tables', path='data/taq'):
        """
        TAQ:
        """
        df = self.wrds_manager.get_taq_tables()
        print(df)
        return df

    def taq_retail_markethour(self, retail_cutoff_upper = 100000, retail_cutoff_lower = 0, name='taq_retail_markethour', start_date='2013-01-01', end_date='2024-12-31', combine=False):
        """
        TAQ:
        """
        permno_list = self.analyst_estimation_builder.price_target_detail()['permno'].unique()
        trading_dates = self.pricevol_builder.pricevol_processed().query(f"date>'{start_date}' and date<'{end_date}'")['date'].unique()

        # the data is quite big, so the database organizes it by date
        for date in trading_dates:
            # covert date to a string 
            date = date.strftime('%Y-%m-%d')
            ind_save_path = f'{self.save_path}/parts/{retail_cutoff_upper}_{retail_cutoff_lower}/{name}_{date}.parquet'
            # check if the file exists
            if os.path.exists(ind_save_path):
                print(f'{ind_save_path} already exists')
                continue
            else:
                os.makedirs(f'{self.save_path}/parts/{retail_cutoff_upper}_{retail_cutoff_lower}', exist_ok=True)
            link_df = self.taq_link_table(date=date, permno_list=permno_list)
            sym_root_list = link_df['sym_root'].unique()

            df = self.wrds_manager.get_taq_retail_markethour(date=date, sym_root_list=sym_root_list, retail_cutoff_upper=retail_cutoff_upper, retail_cutoff_lower=retail_cutoff_lower)
            print(df)
            df.to_parquet(ind_save_path)
        
        if combine:
            print(f"Combining all the parts into one file: {self.save_path}/{name}_{retail_cutoff_upper}_{retail_cutoff_lower}_{start_date}_{end_date}.parquet")
            # combine all the parts into one file
            part_files = glob.glob(f'{self.save_path}/parts/{retail_cutoff_upper}_{retail_cutoff_lower}/{name}*.parquet')
            df = pd.concat([pd.read_parquet(file) for file in part_files])
            df.to_parquet(f'{self.save_path}/{name}_{retail_cutoff_upper}_{retail_cutoff_lower}_{start_date}_{end_date}.parquet')
        return 'Done with TAQ retail markethour!'

    def taq_link_table(self, date='2021-12-31', permno_list=None, symbol_root=None, name='_taq_link_table', start_date='2013-01-01'):
        """
        TAQ:
        """
        df = self.wrds_manager.get_taq_link_table(date=date, permno_list=permno_list, symbol_root=symbol_root, start_date=start_date)
        print("the shape of the link table is: ", df.shape)
        print(df.head())
        return df
        