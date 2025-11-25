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
from academic_data_download.factors_lab.pricevol_builder import PriceVolComputer

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
        self.pricevol_builder = PriceVolComputer(verbose=verbose, db=db, permno_list=permno_list)

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
    def eps_summary_qtr(self, name='eps_summary_qtr'):
        """
        """
        df = self.wrds_manager.get_eps_summary(permno_list=self.permno_list, qtr=True, ann=False)
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
    def eps_act_ann(self, name='eps_act_ann'):
        """
        """
        df = self.wrds_manager.get_eps_act(permno_list=self.permno_list, qtr=False, ann=True)
        return df
    
    @analyst_estimator
    def eps_act_qtr(self, name='eps_act_qtr'):
        """
        """
        df = self.wrds_manager.get_eps_act(permno_list=self.permno_list, qtr=True, ann=False)
        return df

    @analyst_estimator
    def earnings_date_analyst_detail_research(self, name='earnings_date_analyst_detail_research'):
        """
        Build a detailed DataFrame aligning analyst estimates and price targets with quarterly earnings dates.

        For each company and analyst:
        - Annotates each quarterly earnings announcement date with: next earnings date,
          nearest EPS estimates & price targets (before/after), and the same for the next period.
        - Useful for event studies/research needing precise temporal linking of forecasts, 
          realized earnings, and price targets.

        Returns
        -------
        DataFrame
            A DataFrame indexed by earnings event, company, and analyst, containing:
            - event dates, earnings dates, next earnings period
            - EPS estimates and price targets immediately before/after the event
            - the same for the subsequent period
        """
        # --- Load core quarterly earnings events ("actuals") ---
        #   - Rename columns for clarity & sort chronologically.
        qtr_earn_df = (
            self.eps_act_qtr()
            .rename(columns={'ann_ts': 'et', 'ann_deemed_date': 'earnings_deemed_date'})
            .sort_values(['pends', 'permno'])
        )
        # Ensure date columns are datetime objects.
        qtr_earn_df['earnings_deemed_date'] = pd.to_datetime(qtr_earn_df['earnings_deemed_date'])
        qtr_earn_df['et'] = pd.to_datetime(qtr_earn_df['et'])
        qtr_earn_df['pends'] = pd.to_datetime(qtr_earn_df['pends'])

        # Annotate with the next period end date (for linking expectations for next quarter).
        qtr_earn_df['n1q_pends'] = qtr_earn_df.groupby('permno')['pends'].transform(lambda x: x.shift(-1))

        # Drop columns not needed for analysis, if present.
        try:
            qtr_earn_df = qtr_earn_df.drop(columns=['act_ts', 'measure', 'pdicity'])
        except Exception:
            pass

        # --- Load core annual earnings events ("actuals") ---
        ann_earn_df = (
            self.eps_act_ann()
            .rename(columns={'ann_ts': 'et', 'ann_deemed_date': 'earnings_deemed_date'})
            .sort_values(['pends', 'permno'])
        )
        # Ensure date columns are datetime objects.
        ann_earn_df['pends'] = pd.to_datetime(ann_earn_df['pends'])

        qtr_earn_df = pd.merge_asof(qtr_earn_df, ann_earn_df[['pends', 'permno']].rename(columns={'pends': 'n1y_pends'}), left_on=['pends'], right_on=['n1y_pends'], by=['permno'], direction='forward', allow_exact_matches=False)
        qtr_earn_df.sort_values(by=['et', 'permno'], inplace=True)

        # --- Load analyst EPS estimates for quarters ("detail qtr") ---
        #   - Rename for clarity, sort for merge_asof.
        eps_detail_qtr = (
            self.eps_detail_qtr()
            .rename(columns={'fpedats': 'pends', 'analys': 'amaskcd'})
            .sort_values(['ann', 'permno'])
        )
        eps_detail_qtr['ann'] = pd.to_datetime(eps_detail_qtr['ann'])
        eps_detail_qtr['pends'] = pd.to_datetime(eps_detail_qtr['pends'])
        # Uncomment only for debugging!
        # print(eps_detail_qtr)

        # --- Load analyst EPS estimates for annual ("detail ann") ---
        #   - Rename for clarity, sort for merge_asof.
        eps_detail_ann = (
            self.eps_detail_ann()
            .rename(columns={'fpedats': 'pends', 'analys': 'amaskcd'})
            .sort_values(['ann', 'permno'])
        )
        eps_detail_ann['ann'] = pd.to_datetime(eps_detail_ann['ann'])
        eps_detail_ann['pends'] = pd.to_datetime(eps_detail_ann['pends'])

        # --- Load analyst price target details, cleaned ---
        pt_detail = (
            self.price_target_detail_revision()
            .drop(columns=['act', 'namedt', 'nameendt', 'revision', 'last_pt', 'last_ann_deemed_date'])
            .sort_values(['ann'])
        )
        pt_detail['ann_deemed_date'] = pd.to_datetime(pt_detail['ann_deemed_date'])
        pt_detail['ann'] = pd.to_datetime(pt_detail['ann'])

        # --- Load market cap ---
        market_cap_df = self.pricevol_builder.marketcap(name='marketcap')
        market_cap_df['date'] = pd.to_datetime(market_cap_df['date'])

        # --- Load price return ---
        price_return_df = self.pricevol_builder.pricevol_processed(name='pricevol_processed')
        # Select columns permno, date, prc plus any that start with 'fwd' or 'cum'
        selected_cols = ['permno', 'date', 'prc', 'vol'] + [
            col for col in price_return_df.columns
            if col.startswith('fwd') or col.startswith('cum')
        ]
        price_return_df = price_return_df[selected_cols]
        price_return_df['date'] = pd.to_datetime(price_return_df['date'])

        # =====================================================
        # === ALIGN ANALYSTS/COMPANIES ACROSS DATA SOURCES ====
        # =====================================================

        # merge market cap with earnings date
        qtr_earn_df = pd.merge(qtr_earn_df, market_cap_df[['permco', 'date', 'marketcap']], left_on=['permco', 'earnings_deemed_date'], right_on=['permco', 'date'], how='left')
        

        # merge factors with earnings date
        factor_df = pd.read_parquet('data/factors/combined/factors_combined.parquet')
        factor_df['date'] = pd.to_datetime(factor_df['date'])
        factor_df['permco'] = factor_df['permco'].astype('Int64')
        qtr_earn_df = pd.merge_asof(qtr_earn_df, factor_df.drop(columns=['gvkey']), left_on=['earnings_deemed_date'], right_on=['date'], by=['permco'], direction='backward')
        print("factors merged with earnings date")

        # merge price return with earnings date
        qtr_earn_df = pd.merge(qtr_earn_df, price_return_df, left_on=['permno', 'earnings_deemed_date'], right_on=['permno', 'date'], how='left')

        # Ensure we have every permno-analyst combination for the earnings events.
        # This will DUPLICATE each earnings event row for every analyst with coverage.
        analyst_combos = pt_detail[['permno', 'amaskcd']].drop_duplicates()
        qtr_earn_df = qtr_earn_df.merge(analyst_combos, on='permno', how='inner')

        # --- 1. Merge each earnings event with analyst EPS estimate just before event ("asof") ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            eps_detail_qtr[['permno', 'pends', 'value', 'ann', 'amaskcd']].rename(columns={'value': 'eps_est', 'ann': 'eps_est_ann'}),
            left_on=['et'],
            right_on=['eps_est_ann'],
            by=['permno', 'pends', 'amaskcd'],
            direction='backward'
        )

        # --- 2. Merge price target issued just before event, by company-analyst ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            pt_detail[['permno', 'amaskcd', 'ann', 'pt']]
                .rename(columns={'ann': 'ex_pt_ann', 'pt': 'ex_pt'}),
            left_on=['et'],
            right_on=['ex_pt_ann'],
            by=['permno', 'amaskcd'],
            direction='backward',
            allow_exact_matches=False
        )

        # --- 3. Merge price target issued just AFTER event, by company-analyst ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            pt_detail[['permno', 'amaskcd', 'ann', 'pt']]
                .rename(columns={'ann': 'post_pt_ann', 'pt': 'post_pt'}),
            left_on=['et'],
            right_on=['post_pt_ann'],
            by=['permno', 'amaskcd'],
            direction='forward',
            allow_exact_matches=False
        )

        # --- 4. EPS estimate for the *next* period (nearest announced before the event) ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            eps_detail_qtr[['permno', 'pends', 'value', 'ann', 'amaskcd']]
                .rename(columns={
                    'value': 'ex_n1q_eps_est',
                    'ann': 'ex_n1q_eps_est_ann',
                    'pends': 'n1q_pends'
                }),
            left_on=['et'],
            right_on=['ex_n1q_eps_est_ann'],
            by=['permno', 'n1q_pends', 'amaskcd'],
            direction='backward',
            allow_exact_matches=False
        )

        # --- 5. EPS estimate for the *next* period (nearest announced AFTER the event) ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            eps_detail_qtr[['permno', 'pends', 'value', 'ann', 'amaskcd']]
                .rename(columns={
                    'value': 'post_n1q_eps_est',
                    'ann': 'post_n1q_eps_est_ann',
                    'pends': 'n1q_pends'
                }),
            left_on=['et'],
            right_on=['post_n1q_eps_est_ann'],
            by=['permno', 'n1q_pends', 'amaskcd'],
            direction='forward',
            allow_exact_matches=False
        )

        # --- 6. EPS estimate for the *next* period (nearest announced BEFORE the event) ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            eps_detail_ann[['permno', 'pends', 'value', 'ann', 'amaskcd']]
                .rename(columns={
                    'value': 'ex_n1y_eps_est',
                    'ann': 'ex_n1y_eps_est_ann',
                    'pends': 'n1y_pends'
                }),
            left_on=['et'],
            right_on=['ex_n1y_eps_est_ann'],
            by=['permno', 'n1y_pends', 'amaskcd'],
            direction='backward',
            allow_exact_matches=False
        )

        # --- 7. EPS estimate for the *next* period (nearest announced AFTER the event) ---
        qtr_earn_df = pd.merge_asof(
            qtr_earn_df,
            eps_detail_ann[['permno', 'pends', 'value', 'ann', 'amaskcd']]
                .rename(columns={
                    'value': 'post_n1y_eps_est',
                    'ann': 'post_n1y_eps_est_ann',
                    'pends': 'n1y_pends'
                }),
            left_on=['et'],
            right_on=['post_n1y_eps_est_ann'],
            by=['permno', 'n1y_pends', 'amaskcd'],
            direction='forward',
            allow_exact_matches=False
        )

        return qtr_earn_df

    @analyst_estimator
    def pt_detail_with_eps_estimate(self, name='pt_detail_with_eps_estimate'):
        """
        """
        def _extract_eps_estimate_subdf(df, fpi_code, eps_label):
            """
            Helper to extract EPS estimates by forecast period identifier (fpi).
            Renames columns for clarity and adds a timestamp column for the estimate.
            """
            subdf = (
                df[df['fpi'] == fpi_code][['permno', 'ann_deemed_date', 'analys', 'value', 'eps_act', 'eps_act_date']]
                .rename(columns={'value': eps_label, 'analys': 'amaskcd'})
            )
            subdf[f"{eps_label}_date"] = subdf['ann_deemed_date']
            subdf.rename(columns={'eps_act': f"{eps_label}_act", 'eps_act_date': f"{eps_label}_act_date"}, inplace=True)
            return subdf

        pt_detail = self.price_target_detail_revision().drop(columns=['act', 'namedt', 'nameendt']).sort_values(by=['ann_deemed_date'])
        pt_detail['ann_deemed_date'] = pd.to_datetime(pt_detail['ann_deemed_date'])

        eps_detail_qtr = self.eps_detail_qtr().rename(columns={'fpedats': 'pends'})
        eps_detail_qtr['ann_deemed_date'] = pd.to_datetime(eps_detail_qtr['ann_deemed_date'])

        eps_act_qtr = self.eps_act_qtr()
        eps_act_qtr['eps_act_date'] = pd.to_datetime(eps_act_qtr['ann_deemed_date'])

        eps_detail_qtr = pd.merge(eps_detail_qtr, eps_act_qtr[['permno', 'pends', 'eps_act', 'eps_act_date']], on=['permno', 'pends'], how='left')

        q1_eps = _extract_eps_estimate_subdf(eps_detail_qtr, '6', 'q1_eps')
        q2_eps = _extract_eps_estimate_subdf(eps_detail_qtr, '7', 'q2_eps')

        eps_detail_ann = self.eps_detail_ann()
        eps_detail_ann['ann_deemed_date'] = pd.to_datetime(eps_detail_ann['ann_deemed_date'])

        eps_act_ann = self.eps_act_ann()
        eps_act_ann['eps_act_date'] = pd.to_datetime(eps_act_ann['ann_deemed_date'])

        eps_detail_ann = pd.merge(eps_detail_ann, eps_act_ann[['permno', 'eps_act', 'eps_act_date']], on=['permno'], how='left')

        y1_eps = _extract_eps_estimate_subdf(eps_detail_ann, '1', 'y1_eps')
        y2_eps = _extract_eps_estimate_subdf(eps_detail_ann, '2', 'y2_eps')
        
        pt_detail = pd.merge_asof(pt_detail, q1_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        pt_detail = pd.merge_asof(pt_detail, q2_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        pt_detail = pd.merge_asof(pt_detail, y1_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        pt_detail = pd.merge_asof(pt_detail, y2_eps, on=['ann_deemed_date'], by=['permno', 'amaskcd'], direction='backward')
        return pt_detail


    @analyst_estimator
    def pt_detail_with_earnings_date(self, name='pt_detail_with_earnings_date'):
        """
        Build a DataFrame aligning analyst price targets with earnings dates.

        For each company and analyst:
        - Annotates each price target date with: next earnings date,
          nearest EPS estimates & price targets (before/after), and the same for the next period.
        - Useful for event studies/research needing precise temporal linking of forecasts, 
          realized earnings, and price targets.
        """
        def _extract_eps_estimate_subdf(df, fpi_code, eps_label):
            """
            Helper to extract EPS estimates by forecast period identifier (fpi).
            Renames columns for clarity and adds a timestamp column for the estimate.
            """
        pt_detail = self.price_target_detail_revision().drop(columns=['act', 'namedt', 'nameendt']).sort_values(by=['ann_deemed_date'])
        pt_detail['ann_deemed_date'] = pd.to_datetime(pt_detail['ann_deemed_date'])

        eps_act_qtr = self.eps_act_qtr()
        eps_act_qtr['ann_deemed_date'] = pd.to_datetime(eps_act_qtr['ann_deemed_date'])

        # merge the next earnings date with the pt_detail with merge_asof
        pt_detail = pd.merge_asof(pt_detail, eps_act_qtr[['permno', 'ann_deemed_date']].rename(columns={'ann_deemed_date': 'next_earnings_date'}), left_on=['ann_deemed_date'], right_on=['next_earnings_date'], by=['permno'], direction='forward')

        # merge the last earnings date with the pt_detail with merge_asof
        pt_detail = pd.merge_asof(pt_detail, eps_act_qtr[['permno', 'ann_deemed_date']].rename(columns={'ann_deemed_date': 'last_earnings_date'}), left_on=['ann_deemed_date'], right_on=['last_earnings_date'], by=['permno'], direction='backward')
        return pt_detail