import os
import pandas as pd
import tqdm
import numpy as np
import glob
from jinja2 import Environment, FileSystemLoader

from academic_data_download.utils.merger import merge_link_table_crsp, merge_link_table_msp500list
from academic_data_download.utils.clean import crsp_clean
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.utils.sql_execution import sql_execution_in_chunks

# hyperparameter
# set up environment, assuming your template lives in sql/
env = Environment(loader=FileSystemLoader("src/academic_data_download/sql_inventory"))

class WRDSManager():
    def __init__(self, db, verbose=True):
        self.db = db
        self.verbose = verbose

    def get_fundq(self, fund_list, gvkey_list=None, start_year=2000):
        """
        Get quarterly fundamental data from Compustat FUNDQ.

        Parameters
        ----------
        fund_list : list of str
            Columns to retrieve (e.g., ['saleq', 'cogsq', 'atq']).
        verbose : bool, optional
            Whether to print the SQL query.
        start_year : int, optional
            Earliest fiscal year (default 2000).
        gvkey_list : list of str, optional
        Returns
        -------
        pandas.DataFrame
            Quarterly data with requested columns.
        """
        if self.verbose: 
            print("fund_list: ", fund_list)
            print("gvkey_list: ", gvkey_list)

        sql = env.get_template("fundamentals/fundq.sql.j2").render(
            fund_list=fund_list, 
            start_year=start_year, 
            gvkey_list=gvkey_list)

        df = self.db.raw_sql(sql)
        df['datadate'] = pd.to_datetime(df['datadate'])
        df['rdq'] = pd.to_datetime(df['rdq'])

        # the fund list value should have precision of 3
        for col in fund_list:
            df[col] = df[col].astype(float).round(3)

        if self.verbose:
            print("peeks at the data right after getting from wrds")
            sneak_peek(df)
        return df

    def get_funda(self, fund_list, start_year=2000, gvkey_list=None):
        """
        Get annual fundamental data from Compustat FUNDA.

        Parameters
        ----------
        fund_list : list of str
            Columns to retrieve (e.g., ['sale', 'cogs', 'at']).
        start_year : int, optional
            Earliest fiscal year (default 2000).
        gvkey_list : list of str, optional
        verbose : bool, optional
            Whether to print the SQL query.
        Returns
        -------
        pandas.DataFrame
            Annual data with requested columns.
        """
        if self.verbose: 
            print("fund_list: ", fund_list)
            print("gvkey_list: ", gvkey_list)

        sql = env.get_template("fundamentals/funda.sql.j2").render(
            fund_list=fund_list, 
            start_year=start_year, 
            gvkey_list=gvkey_list)

        df = self.db.raw_sql(sql)
        df['datadate'] = pd.to_datetime(df['datadate'])
        
        for col in fund_list:
            df[col] = df[col].astype(float).round(3) # the fund list value should have precision of 3
        return df
    
    def get_secd_daily(self, start_date=None, end_date=None):
        """
        Get daily SEC data from Compustat SECD.
        """
        sql = env.get_template("pricevol/comp_secd.sql.j2").render(start_date=start_date, end_date=end_date)
        df = self.db.raw_sql(sql)
        return df

    def get_crsp_daily(
            self, 
            cache_path='data/pricevol/pricevol_raw.parquet', 
            start_date='2000-01-01', 
            crop_by_year = False,
            permno_list=None,
        ):
        """
        Retrieve daily CRSP stock data (price, return, volume, shares, adjustment factors).

        This method supports efficient data retrieval and caching:
        - If a local Parquet cache exists at `cache_path`, the data is loaded from disk.
        - If no cache is found and `permno_list` is None, the method retrieves all available PERMNOs,
          queries the database in manageable chunks, saves each chunk, merges them, and caches the result.
        - If a specific `permno_list` is provided, the method queries the database for just those PERMNOs.

        Parameters
        ----------
        cache_path : str, optional
            Path to the local Parquet file for caching the CRSP daily data. Default is 'data/pricevol'.
        start_date : str, optional
            Earliest date to retrieve (format: 'YYYY-MM-DD'). Default is '2000-01-01'.
        crop_by_year : bool, optional
            Whether to crop the data by year. Default is False. Otherwise give a year as an integer, like 2020. This option does not work if you pass permno_list as None.
        permno_list : list or None, optional
            List of PERMNOs to retrieve. If None, retrieves all available PERMNOs.

        Returns
        -------
        pandas.DataFrame
            DataFrame containing daily CRSP data with columns such as:
            ['permco', 'permno', 'cusip', 'date', 'prc', 'ret', 'retx', 'vol', 'shrout', 'cfacpr', 'cfacshr', 'openprc']
        """
        # Ensure required directories exist
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        template = env.get_template("pricevol/crsp_dsf.sql.j2") # template for the sql query

        # If permno_list is None, retrieve all permnos in chunks and cache the result
        if permno_list is None:
            link_df = self.permco_gvkey_link()
            permno_list = link_df['permno'].dropna().unique()            

            if os.path.exists(cache_path):
                print("Cache file for CRSP daily data found. Loading from disk...")
                return pd.read_parquet(cache_path)
            else:
                print("Cache file for CRSP daily data not found. Starting SQL queries. This may take a while...")
                sql_renderer = lambda x: template.render(permno_list=x, start_date=start_date, crop_by_year=False) # there is no crop_by_year when we do not specify permno, because we want to save all results into a parquet file
                pricevol_df = sql_execution_in_chunks(self.db, sql_renderer, permno_list, cache_path, chunk_size=10)
                
        # If a permno_list is provided, retrieve data for those permnos only
        else:
            print("Retrieving CRSP daily data for a specific permno list...")
            sql = template.render(permno_list=permno_list, start_date=start_date, crop_by_year=crop_by_year)
            pricevol_df = self.db.raw_sql(sql)

        return pricevol_df

    def get_price_target_summary(self, permno_list=None):
        """
        Get target price from CRSP daily data.
        """
        template = env.get_template("analyst_estimation/price_target_summary.sql.j2")
        sql = template.render(permno_list=permno_list)
        return self.db.raw_sql(sql)

    def get_price_target_detail(self, permno_list=None):
        """
        Get target price from CRSP daily data.
        """
        template = env.get_template("analyst_estimation/price_target.sql.j2")
        sql = template.render(permno_list=permno_list)
        return self.db.raw_sql(sql)

    def permco_gvkey_link(self):
        """
        Get table mapping Compustat GVKEYs to CRSP PERMCOs and PERMNOs.

        Returns
        -------
        pandas.DataFrame
            Link table with GVKEY, iid, PERMNO, PERMCO, etc.
        """
        sql = env.get_template("link_table/ccmxpf_linktable.sql.j2").render()
        link_df = self.db.raw_sql(sql)
        return link_df

    def get_sp500_constituents_snapshot(self, year):
        """
        Get SP500 list from CRSP. with link table and gic sector. at a given year.
        """
        template = env.get_template("sp500/sp500_constituents.sql.j2")
        sql = template.render(year=year)
        return self.db.raw_sql(sql)

    def get_raven_full_equities(self, year=2024, relevance_threshold=75, event_similarity_days_threshold=90, permno_list=None):
        sql = env.get_template("ravenpack/rp_equities.sql.j2").render(
            year=year, 
            relevance_threshold=relevance_threshold, 
            event_similarity_days_threshold=event_similarity_days_threshold,
            permno_list=permno_list)
        return self.db.raw_sql(sql).dropna(subset=['permco', 'permno'], how='any').drop_duplicates(subset=['trading_day_et', 'permco', 'permno'])


    def get_raven_global_macro(self, year=2024, relevance_threshold=75, event_similarity_days_threshold=90):
        sql = env.get_template("ravenpack/rp_macro.sql.j2").render(
            year=year, 
            relevance_threshold=relevance_threshold, 
            event_similarity_days_threshold=event_similarity_days_threshold)
        return self.db.raw_sql(sql)

    def get_taq_peek(self, sym_root_list=None, year=None, date=None):
        sql = env.get_template("taq/taq_lib_peek.sql.j2").render(sym_root_list=sym_root_list, year=year, date=date)
        return self.db.raw_sql(sql)

    def get_taq_tables(self):
        sql = env.get_template("taq/taq_table_peek.sql.j2").render()
        return self.db.raw_sql(sql)
    
    def get_taq_retail_markethour(self, date='2021-12-31', sym_root_list=None, retail_cutoff_upper=100000, retail_cutoff_lower=0):
        sql = env.get_template("taq/taq_retail_markethour.sql.j2").render(
            year=date.split('-')[0], 
            date=date.replace('-', ''), 
            sym_root_list=sym_root_list, 
            retail_cutoff_upper=retail_cutoff_upper, 
            retail_cutoff_lower=retail_cutoff_lower
        )
        return self.db.raw_sql(sql)
    
    def get_taq_link_table(self, date='2021-12-31', permno_list=None, symbol_root=None, start_date='2013-01-01'):
        sql = env.get_template("taq/taq_link_table.sql.j2").render(date=date, permno_list=permno_list, symbol_root=symbol_root, start_date=start_date)
        return self.db.raw_sql(sql)

    def get_eps_detail(self, permno_list=None, qtr=True, ann=True):
        sql = env.get_template("analyst_estimation/eps_detail.sql.j2").render(permno_list=permno_list, qtr=qtr, ann=ann)
        return self.db.raw_sql(sql)