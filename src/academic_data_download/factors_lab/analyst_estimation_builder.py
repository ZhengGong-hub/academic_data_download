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

def analyst_estimator(fn: Callable) -> Callable:
    """
    Decorator for factor calculation methods.
    Ensures the decorated function has a 'name' keyword argument with a default value.
    Handles post-processing and saving of results.
    """
    sig = inspect.signature(fn)
    if "name" not in sig.parameters:
        raise ValueError(f"{fn.__name__} must have a 'name' parameter with a default value")
    default_name = sig.parameters["name"].default

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        nm = kwargs.get("name", default_name)
        print("dealing with: ", nm)
        if not check_if_calculation_needed(nm, self.permno_list, self.save_path):
            print("Already computed. Done with: ", nm)
            return pd.read_parquet(f'{self.save_path}/{nm}.parquet')
        df = fn(self, *args, **kwargs)
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"{fn.__name__} must return a DataFrame, got {type(df)}")
        if self.verbose:
            print("peeks at the data after calculation!\n")
            sneak_peek(df)
        if self.permno_list is None:
            save_file(df, nm, path=self.save_path)
        return df
    return wrapper


class AnalystEstimationBuilder():
    def __init__(self, verbose, db, permno_list):
        self.verbose = verbose
        self.wrds_manager = WRDSManager(db, verbose=verbose)
        self.permno_list = permno_list
        self.save_path = 'data/analysts_estimate'

    @analyst_estimator
    def price_target_summary(self, name='price_target_summary'):
        """
        """
        df = self.wrds_manager.get_price_target_summary(permno_list=self.permno_list)
        return df

    @analyst_estimator
    def price_target_detail(self, name='price_target_detail'):
        """
        """
        df = self.wrds_manager.get_price_target_detail(permno_list=self.permno_list)
        return df

    @analyst_estimator
    def price_target_detail_revision(self, name='price_target_detail_revision'):
        """
        """
        df = self.price_target_detail().dropna(subset=['value'])
        df['value'] = df['value'].astype(float)
        # pair id of company covered + analyst id
        df['analyst_coverage_id'] = df['permno'].astype(str) + '_' + df['amaskcd'].astype(str)

        # have a column to record initial price target to differentiate from price target revision 
        # if the pair [permno, amaskcd] show up the first time, the column value of revision to 0, otherwise to 1
        df = df.sort_values(['analyst_coverage_id', 'ann_deemed_date'])
        df['revision'] = (
            df.groupby('analyst_coverage_id').cumcount()
        )
        df['last_pt'] = df.groupby('analyst_coverage_id')['value'].transform(lambda x: x.shift(1))
        return df