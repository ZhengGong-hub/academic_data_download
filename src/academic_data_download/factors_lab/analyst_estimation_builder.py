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
        df.rename(columns={'value': 'pt'}, inplace=True)
        # pair id of company covered + analyst id
        df['analyst_coverage_id'] = df['permno'].astype(str) + '_' + df['amaskcd'].astype(str)

        # have a column to record initial price target to differentiate from price target revision 
        # if the pair [permno, amaskcd] show up the first time, the column value of revision to 0, otherwise to 1
        df = df.sort_values(['analyst_coverage_id', 'ann_deemed_date'])
        df['revision'] = (
            df.groupby('analyst_coverage_id').cumcount()
        )
        df['last_pt'] = df.groupby('analyst_coverage_id')['pt'].transform(lambda x: x.shift(1))
        df['last_ann_deemed_date'] = df.groupby('analyst_coverage_id')['ann_deemed_date'].transform(lambda x: x.shift(1))
        return df

    @analyst_estimator
    def eps_detail_qtr(self, name='eps_detail_qtr'):
        """
        """
        df = self.wrds_manager.get_eps_detail(permno_list=self.permno_list, qtr=True, ann=False)
        return df

    @analyst_estimator
    def eps_detail_ann(self, name='eps_detail_ann'):
        """
        """
        df = self.wrds_manager.get_eps_detail(permno_list=self.permno_list, qtr=False, ann=True)
        return df

    @analyst_estimator
    def pt_detail_with_eps_estimate(self, name='pt_detail_with_eps_estimate'):
        """
        """
        pt_detail = self.price_target_detail_revision().drop(columns=['act', 'namedt', 'nameendt']).sort_values(by=['ann_deemed_date'])
        pt_detail['ann_deemed_date'] = pd.to_datetime(pt_detail['ann_deemed_date'])

        eps_detail_qtr = self.eps_detail_qtr()
        eps_detail_qtr['ann_deemed_date'] = pd.to_datetime(eps_detail_qtr['ann_deemed_date'])
        # q1
        q1_eps = eps_detail_qtr[eps_detail_qtr['fpi'] == '6'][['permno', 'ann_deemed_date', 'analys', 'value']].rename(columns={'value': 'q1_eps', 'analys': 'amaskcd'})
        q1_eps['q1_eps_date'] = q1_eps['ann_deemed_date']
        # q2
        q2_eps = eps_detail_qtr[eps_detail_qtr['fpi'] == '7'][['permno', 'ann_deemed_date', 'analys', 'value']].rename(columns={'value': 'q2_eps', 'analys': 'amaskcd'})
        q2_eps['q2_eps_date'] = q2_eps['ann_deemed_date']

        eps_detail_ann = self.eps_detail_ann()
        eps_detail_ann['ann_deemed_date'] = pd.to_datetime(eps_detail_ann['ann_deemed_date'])
        # y1
        y1_eps = eps_detail_ann[eps_detail_ann['fpi'] == '1'][['permno', 'ann_deemed_date', 'analys', 'value']].rename(columns={'value': 'y1_eps', 'analys': 'amaskcd'})
        y1_eps['y1_eps_date'] = y1_eps['ann_deemed_date']
        # y2
        y2_eps = eps_detail_ann[eps_detail_ann['fpi'] == '2'][['permno', 'ann_deemed_date', 'analys', 'value']].rename(columns={'value': 'y2_eps', 'analys': 'amaskcd'})
        y2_eps['y2_eps_date'] = y2_eps['ann_deemed_date']
        
        pt_detail = pd.merge_asof(pt_detail, q1_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        pt_detail = pd.merge_asof(pt_detail, q2_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        pt_detail = pd.merge_asof(pt_detail, y1_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        pt_detail = pd.merge_asof(pt_detail, y2_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')

        return pt_detail