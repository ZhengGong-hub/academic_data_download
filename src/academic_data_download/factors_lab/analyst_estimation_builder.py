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
            return
        df = fn(self, *args, **kwargs)
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"{fn.__name__} must return a DataFrame, got {type(df)}")
        if self.verbose:
            print("peeks at the data after calculation!\n")
            sneak_peek(df)
        if self.permno_list is None:
            save_file(df, nm, path=self.save_path)
        return
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