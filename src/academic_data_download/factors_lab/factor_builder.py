import pandas as pd
import numpy as np
from typing import Callable
import inspect
from functools import wraps

from academic_data_download.db_manager.wrds_sql import WRDSManager
from academic_data_download.utils.save_file import save_file
from academic_data_download.utils.necessary_cond_calculation import check_if_calculation_needed
from academic_data_download.utils.sneak_peek import sneak_peek
from academic_data_download.utils.col_transform import rolling_sum, fill_forward, merge_mktcap_fundq, fillna_with_0, merge_funda_rdq, shift_n_rows, merge_funda_fundq
from academic_data_download.factors_lab.pricevol_builder import PriceVolComputer

def factor(fn: Callable) -> Callable:
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
        if not check_if_calculation_needed(nm, self.gvkey_list, self.save_path):
            print("Already computed. Done with: ", nm)
            return
        df = fn(self, *args, **kwargs)
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"{fn.__name__} must return a DataFrame, got {type(df)}")
        if self.verbose:
            print("peeks at the data after calculation!\n")
            sneak_peek(df)
        if self.gvkey_list is None:
            save_file(df, nm, path=self.save_path)
        return
    return wrapper

class FactorBuilder():
    def __init__(self, verbose, db, gvkey_list, save_path='data/factors/single_factor'):
        self.verbose = verbose
        self.gvkey_list = gvkey_list
        self.wrds_manager = WRDSManager(db, verbose=verbose)
        self.save_path = save_path
        pvc = PriceVolComputer(permno_list=None, verbose=False, db=db)
        self.spy_pricevol = PriceVolComputer(permno_list=[84398], verbose=False, db=db).pricevol_raw()

        self.mktcap_df = pvc.marketcap(name='marketcap')
        # sum up for every date, the biggest 500 mktcap companies
        # to a new df
        self.sp500_mktcap_df = self.mktcap_df.groupby('date').agg({'marketcap': 'sum'}).reset_index()

        self.pricevol_df = pvc.pricevol_processed(name='pricevol_processed')

        if self.gvkey_list is not None:
            self.mktcap_df = self.mktcap_df.query('gvkey in @gvkey_list')
        else:
            self.mktcap_df = self.mktcap_df

    @factor
    def gross_profit_to_assets(self, qtr=True, name='f_gpta'):
        """
        Gross profit to assets:
        (Revenue - Cost of Goods Sold) / Total Assets
        """
        if qtr:
            # revtq: revenue, cogsq: cost of goods sold, atq: total assets
            fund_df = self.wrds_manager.get_fundq(fund_list=["revtq", "cogsq", "atq"], gvkey_list=self.gvkey_list) 
            
            # group by gvkey and rolling sum of saleq for the last 4 quarters
            fund_df['revtq_ltm'] = rolling_sum(fund_df, 'revtq')
            fund_df['cogsq_ltm'] = rolling_sum(fund_df, 'cogsq')
            # atq: total assets, fill na with last value, group by gvkey
            fund_df['atq'] = fill_forward(fund_df, 'atq')
            
            # calculate gross profit to assets
            fund_df[name] = (fund_df['revtq_ltm'] - fund_df['cogsq_ltm']) / fund_df['atq']
        return fund_df

    @factor
    def sales_to_price(self, qtr=True, name='f_sp'):
        """
        Sales to price:
        Sales / Market Cap
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["saleq"], gvkey_list=self.gvkey_list) # saleq: sales

            fund_df['saleq_ltm'] = rolling_sum(fund_df, 'saleq')

            # merge the fund_df and price_df
            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['saleq_ltm'] / res_df['marketcap']
        return res_df

    @factor
    def btm(self, qtr=True, name='f_btm'):
        """
        BE = SEQQ + TXDITC - PSTK (book equity)
        BTM = BE / Market Cap
        Rosenberg et al. (1985)
        """
        if qtr:
            # seqq: Stockholders' Equity - Total, txditcc: deferred income tax, pstk: preferred stock
            fund_df = self.wrds_manager.get_fundq(fund_list=["seqq", "txditcq", "pstkq"], gvkey_list=self.gvkey_list)

            # forward fill
            for col in ['txditcq', 'pstkq', 'seqq']:
                fund_df[col] = fill_forward(fund_df, col)

            fund_df['be'] = fund_df['seqq'] + fund_df['txditcq'] - fund_df['pstkq']

            # get marketcap
            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['be'] / res_df['marketcap']
        return res_df

    @factor
    def debt_to_market(self, qtr=True, name='f_dtm'):
        """
        Total Debt / Market Cap
        Total Debt = Long Term Debt + Total Current Debt
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["dlttq", "dlcq"], gvkey_list=self.gvkey_list) # dlttq: long term debt, dlcq: total current debt
            
            # fillna with 0
            fund_df['dlcq'] = fill_forward(fund_df, 'dlcq')
            fund_df['dlttq'] = fill_forward(fund_df, 'dlttq')

            fund_df['total_debt'] = fund_df['dlttq'] + fund_df['dlcq']

            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['total_debt'] / res_df['marketcap']
        return res_df

    @factor
    def earnings_to_price(self, qtr=True, name='f_ep'):
        """
        Earnings to price:
        Earnings / Market Cap
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["ibq"], gvkey_list=self.gvkey_list) # ibq: Income Before Extraordinary Items
            fund_df['ibq_ltm'] = rolling_sum(fund_df, 'ibq')

            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['ibq_ltm'] / res_df['marketcap']
        return res_df

    @factor
    def cashflow_to_price(self, qtr=True, name='f_cfp'):
        """
        Cashflow to price:
        Cashflow / Market Cap
        Cashflow = Income Before Extraordinary Items + Depreciation and Amortization
        Lakonishok et al. (1994)
        """
        if qtr:
            # ibq: income before extraordinary items, dpq: depreciation and amortization
            fund_df = self.wrds_manager.get_fundq(fund_list=["ibq", "dpq"], gvkey_list=self.gvkey_list) 

            fund_df['dpq'] = fillna_with_0(fund_df, 'dpq')
            fund_df['cashflow'] = fund_df['ibq'] + fund_df['dpq']
            fund_df['cashflow_ltm'] = rolling_sum(fund_df, 'cashflow')

            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['cashflow_ltm'] / res_df['marketcap']

        return res_df

    @factor
    def payout_yield(self, qtr=True, name='f_py'):
        """
        Payout yield:
        Payout / Market Cap
        Payout = Cash Dividends + Repurchased Shares * Price to Book Ratio (not the net repurchase)
        Boudoukh et al. (2007) 
        """
        if qtr:
            # dvpsxq: cash dividends paid per share, cshoq: common shares outstanding, cshopq: common shares outstanding repurchased, prcraq: price to book ratio
            fund_df = self.wrds_manager.get_fundq(fund_list=["dvpsxq", "cshoq", "cshopq", "prcraq"], gvkey_list=self.gvkey_list)

            fund_df['dvpsxq'] = fillna_with_0(fund_df, 'dvpsxq')
            fund_df['cshopq'] = fillna_with_0(fund_df, 'cshopq')
            fund_df['prcraq'] = fillna_with_0(fund_df, 'prcraq')
            fund_df['cshoq'] = fillna_with_0(fund_df, 'cshoq')

            fund_df['dividends'] = fund_df['dvpsxq'] * fund_df['cshoq']
            fund_df['repurchases'] = fund_df['cshopq'] * fund_df['prcraq']

            fund_df['payout'] = fund_df['dividends'] + fund_df['repurchases']
            fund_df['payout_ltm'] = rolling_sum(fund_df, 'payout')

            # get market cap
            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['payout_ltm'] / res_df['marketcap']

        return res_df

    @factor
    def ev_multiple(self, qtr=True, name='f_evm'):
        """
        Enterprise Value to EBITDA
        Enterprise Value = Market Cap + Long Term Debt + Total Current Debt + Noncontrolling Intrest - Cash and Equivalents + Preferred Stock
        """
        if qtr:
            # dlttq: long term debt, dlcq: total current debt, mibtq: noncontrolling intrest, cheq: cash and equivalents, pstkq: preferred stock
            fund_df = self.wrds_manager.get_fundq(fund_list=["dlttq", "dlcq", "mibtq", "cheq", "pstkq", "oibdpq"], gvkey_list=self.gvkey_list) 

            fund_df['dlttq'] = fill_forward(fund_df, 'dlttq')
            fund_df['dlcq'] = fill_forward(fund_df, 'dlcq')
            fund_df['mibtq'] = fill_forward(fund_df, 'mibtq')
            fund_df['cheq'] = fill_forward(fund_df, 'cheq')
            fund_df['pstkq'] = fill_forward(fund_df, 'pstkq')

            fund_df['ebitda_ltm'] = rolling_sum(fund_df, 'oibdpq')

            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)

            res_df['ev'] = res_df['marketcap'] + res_df['dlttq'] + res_df['dlcq'] + res_df['mibtq'] - res_df['cheq'] + res_df['pstkq'] 
            res_df[name] = res_df['ev'] / res_df['ebitda_ltm']
        return res_df

    @factor
    def advertising_to_marketcap(self, qtr=True, name='f_adp'):
        """
        Advertising to Market Cap
        Advertising expenses / Market Cap, only has annual term

        note: the measure is not very dense, and also has inconsistency problems. for example, with apple: 
        14  001690 2014-09-30   2014  1200.0 2014-10-20
        15  001690 2015-09-30   2015  1800.0 2015-10-27
        16  001690 2016-09-30   2016     0.0 2016-10-25
        17  001690 2017-09-30   2017     0.0 2017-11-02
        18  001690 2018-09-30   2018     0.0 2018-11-01
        """
        if not qtr:
            fund_df = self.wrds_manager.get_funda(fund_list=["xad"], gvkey_list=self.gvkey_list) # adpq: advertising expenses
            _merge_on_rdq_date = self.wrds_manager.get_fundq(fund_list=["ibq"], gvkey_list=self.gvkey_list)[['gvkey', 'rdq', 'datadate']] # adpq: advertising expenses
            fund_df = merge_funda_rdq(fund_df, _merge_on_rdq_date)
            # TODO wrap up this part nicer 

            fund_df['xad'] = fillna_with_0(fund_df, 'xad')

            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['xad'] / res_df['marketcap']
        return res_df

    @factor
    def rd_to_marketcap(self, qtr=True, name='f_rdp'):
        """
        Research and Development to Market Cap
        Research and Development expenses / Market Cap
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["xrdq"], gvkey_list=self.gvkey_list) # xrdq: research and development expenses
            fund_df['xrdq'] = fillna_with_0(fund_df, 'xrdq')
            fund_df['xrdq_ltm'] = rolling_sum(fund_df, 'xrdq')

            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            res_df[name] = res_df['xrdq_ltm'] / res_df['marketcap']
        return res_df

    @factor
    def operating_leverage(self, qtr=True, name='f_ol'):
        """
        Operating Leverage
        Operating leverage, defined as annual operating costs divided by assets (Compustat item AT), 
        where operating costs is cost of goods sold (COGS) plus selling, general, and administrative expenses (XSGA).     
        Novy-Marx, 2011
        """
        if qtr:
            # xsgaq: selling, general and administrative expenses, cogsq: cost of goods sold
            fund_df = self.wrds_manager.get_fundq(fund_list=["xsgaq", "cogsq", "atq"], gvkey_list=self.gvkey_list) 

            fund_df['xsgaq'] = fillna_with_0(fund_df, 'xsgaq')
            fund_df['cogsq'] = fillna_with_0(fund_df, 'cogsq')

            fund_df['xsgaq_ltm'] = rolling_sum(fund_df, 'xsgaq')
            fund_df['cogsq_ltm'] = rolling_sum(fund_df, 'cogsq')
            
            # calculate operating leverage
            fund_df[name] = (fund_df['xsgaq_ltm'] + fund_df['cogsq_ltm']) / fund_df['atq']
        return fund_df

    @factor
    def return_on_assets(self, qtr=True, name='f_roa'):
        """
        Return on assets:
        Income Before Extraordinary Items / Total Assets
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["ibq", "atq"], gvkey_list=self.gvkey_list) # ibq: income before extraordinary items, atq: total assets
            fund_df['atq'] = fill_forward(fund_df, 'atq')

            fund_df['ibq_ltm'] = rolling_sum(fund_df, 'ibq')
            fund_df[name] = fund_df['ibq_ltm'] / fund_df['atq']
        return fund_df

    @factor
    def sales_growth_rank(self, qtr=True, name='f_sgr'):
        """
        Sales growth rank:
        Sales growth rate (CAGR) over the last 5 years
        Ranked from 1 to 10 by the sales growth rate, cross-sectional
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["saleq"], gvkey_list=self.gvkey_list)  # saleq: sales
            
            # Get lagged sale variable
            fund_df['saleq_ltm'] = rolling_sum(fund_df, 'saleq')
            fund_df['saleq_lag'] = shift_n_rows(fund_df, 'saleq_ltm', 20)

            # Calculate growth rate
            fund_df['five_year_sales_cagr'] = (fund_df['saleq_ltm'] / fund_df['saleq_lag']) ** (1/5) - 1
            # drop na
            fund_df = fund_df.dropna()

            # we dont want marketcap, but we want the tradingdate, i.e. the col "date"
            res_df = merge_mktcap_fundq(self.mktcap_df, fund_df)

            # groupby date to assign cagr into 10 classes (1-10, deciles)
            res_df[name] = res_df.groupby('date')['five_year_sales_cagr'] \
                .transform(lambda x: pd.qcut(x.rank(method='first'), 10, labels=False, duplicates='drop') + 1)
        return res_df

    @factor
    def abnormal_capital_investment(self, qtr=True, name='f_aci'):
        """
        Abnormal capital investment: 
        Current capital expenditure scaled by sales relative to historical capital expenditure scaled by sales 
        See Titman et al. (2004)
        """
        if qtr:
            # capx: capital expenditure, saleq: sales
            fund_df_quarter = self.wrds_manager.get_fundq(fund_list=["saleq"], gvkey_list=self.gvkey_list)
            fund_df_annual = self.wrds_manager.get_funda(fund_list=["capx"], gvkey_list=self.gvkey_list) 

            # Merge both data frames
            fund_df = merge_funda_fundq(fund_df_quarter, fund_df_annual)
            
            # Get variables from past four quarters
            fund_df['saleq_ltm'] = rolling_sum(fund_df, 'saleq')
            fund_df['capx'] = fillna_with_0(fund_df, 'capx') # note: do we need to or should? if the company of interest has no reporting history of capex, the nans will be printed as the factor value later anyways. so this manuever should not be disastrous.

            # Get lagged variables (4, 8, 12 quarters ago, rolling 4 quarters sum)
            for lag in [4, 8, 12]:
                fund_df[f'capx_lag_{lag}'] = shift_n_rows(fund_df, 'capx', lag)
                fund_df[f'saleq_ltm_lag_{lag}'] = shift_n_rows(fund_df, 'saleq_ltm', lag)

            # Calculate abnormal capital investment 
            fund_df['avg_capx_to_sales'] = (
                fund_df['capx_lag_4'] / fund_df['saleq_ltm_lag_4'] +
                fund_df['capx_lag_8'] / fund_df['saleq_ltm_lag_8'] +
                fund_df['capx_lag_12'] / fund_df['saleq_ltm_lag_12']
            ) / 3
            fund_df[name] = (fund_df['capx'] / fund_df['saleq_ltm']) / fund_df['avg_capx_to_sales'] - 1

        return fund_df

    @factor
    def investment_to_assets(self, qtr=True, name='f_ita'):
        """
        Investment to assets: 
        Growth rate of total assets
        See Cooper et al. (2008)
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["atq"], gvkey_list=self.gvkey_list) # atq: total assets
            fund_df['atq'] = fill_forward(fund_df, 'atq')

            # Calculate investment to assets using current and lagged assets
            fund_df['atq_lag'] = shift_n_rows(fund_df, 'atq', 4)
            fund_df[name] = (fund_df['atq'] - fund_df['atq_lag']) / fund_df['atq_lag']

        return fund_df

    @factor
    def changes_in_ppe(self, qtr=True, name='f_ppe'):
        """
        Changes in ppe, inventory to assets: Lyandres et al. (2008)
        Change in property, plant, and equipment, and inventory, scaled by assets
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["invtq", "atq", "ppegtq"], gvkey_list=self.gvkey_list) # invtq: inventories, atq: total assets, ppegtq: property, plant, and equipment

            fund_df['invtq'] = fill_forward(fund_df, 'invtq')
            fund_df['ppegtq'] = fill_forward(fund_df, 'ppegtq')
         
            # Calculate lagged values
            for col in ['atq', 'invtq', 'ppegtq']:
                fund_df[f'{col}_lag'] = shift_n_rows(fund_df, col, row=4)

            # Calculate change in ppe and inventory, scaled by lagged assets
            fund_df[name] = ((fund_df['ppegtq'] - fund_df['ppegtq_lag']) + (fund_df['invtq'] - fund_df['invtq_lag'])) / fund_df['atq_lag']

        return fund_df

    @factor
    def investment_growth(self, qtr=True, name='f_ig'):
        """
        Investment growth: 
        Growth of capital expenditure
        See Xing (2008)
        """
        if qtr:
            # capx: capital expenditure
            fund_df_quarter = self.wrds_manager.get_fundq(fund_list=["saleq"], gvkey_list=self.gvkey_list) # note: we just want to get the rdq column
            fund_df_annual = self.wrds_manager.get_funda(fund_list=["capx"], gvkey_list=self.gvkey_list) 

            # Merge both data frames
            fund_df = merge_funda_rdq(fund_df_annual, fund_df_quarter)
            
            # Get lagged capex
            fund_df['capx_lag'] = shift_n_rows(fund_df, 'capx', 1) # shift 1 because annual data

            # Calculate investment growth
            fund_df[name] = (fund_df['capx'] - fund_df['capx_lag']) / fund_df['capx_lag']

        return fund_df

    @factor
    def inventory_changes(self, qtr=True, name='f_ic'):
        """
        Inventory changes: 
        Change in inventory, scaled by assets
        See Thomas & Zhang (2002)
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["invtq", "atq"], gvkey_list=self.gvkey_list) # invtq: inventory, atq: total assets
            fund_df['invtq'] = fill_forward(fund_df, 'invtq')
            fund_df['atq'] = fill_forward(fund_df, 'atq')

            # Calculate lagged values
            for col in ['atq', 'invtq']:
                fund_df[f'{col}_lag'] = shift_n_rows(fund_df, col, 4)

            # Calculate inventory change
            fund_df[name] = (fund_df['invtq'] - fund_df['invtq_lag']) / (0.5 * (fund_df['atq'] + fund_df['atq_lag']))
            return fund_df

    @factor
    def operating_accruals(self, qtr=True, name='f_oa'):
        """
        Operating accruals:
        Non-cash component of earnings arising from changes in current assets, current liabilities, 
        and depreciation and amortization. 
        Financing transactions and income taxes payable are excluded
        See Sloan (1996)
        """
        if qtr:
            _to_retrieve = ["actq", "atq", "cheq", "lctq", "dlcq", "txpq", "dpq"]
            # actq: current assets, atq: total assets, cheq: cash and cash equivalents, lctq: current liabilities, 
            # dlcq: short-term debt, txpq: income taxes payable, dpq: depreciation and amortization
            fund_df = self.wrds_manager.get_fundq(fund_list=_to_retrieve, gvkey_list=self.gvkey_list) 

            # Get lagged values (4 quarters ago)
            for col in _to_retrieve:
                fund_df[col] = fill_forward(fund_df, col)
                fund_df[f"{col}_lag"] = shift_n_rows(fund_df, col, 4)

            # Calculate deltas
            delta_ca = fund_df["actq"] - fund_df["actq_lag"]
            delta_cash = fund_df["cheq"] - fund_df["cheq_lag"]
            delta_cl = fund_df["lctq"] - fund_df["lctq_lag"]
            delta_std = fund_df["dlcq"] - fund_df["dlcq_lag"]
            delta_tp = fund_df["txpq"] - fund_df["txpq_lag"]

            # Calculate operating accruals
            denom = 0.5 * (fund_df['atq'] + fund_df['atq_lag'])
            fund_df[name] = ((delta_ca - delta_cash) - (delta_cl - delta_std - delta_tp) - fund_df["dpq"]) / denom
            return fund_df

    @factor
    def total_accruals(self, qtr=True, name='f_ta'):
        """
        Total accruals: 
        Sum of the change in working capital, non-current operating assets, and financial assets. 
        Then scaled by total assets
        See Richardson et al. (2005)
        """
        if qtr:
            # actq: current assets, atq: total assets, cheq: cash and cash equivalents, lctq: current liabilities, dlcq: short-term debt, ivao: investments and advances, ltq: total liabilities, dlttq: long-term debt, ivstq: short-term investments, pstkq: preferred stock
            _to_retrieve = ["actq", "atq", "cheq", "lctq", "dlcq", "ltq", "dlttq", "ivstq", "pstkq"]

            fund_df_quarter = self.wrds_manager.get_fundq(fund_list=_to_retrieve, gvkey_list=self.gvkey_list)
            fund_df_annual = self.wrds_manager.get_funda(fund_list=["ivao"], gvkey_list=self.gvkey_list) 

            # Merge both data frames
            fund_df = merge_funda_fundq(fund_df_quarter, fund_df_annual)

            # Get lagged values (4 quarters ago)
            lag_cols = _to_retrieve + ["ivao"]
            for col in lag_cols:
                fund_df[col] = fill_forward(fund_df, col)
                fund_df[f"{col}_lag"] = shift_n_rows(fund_df, col, 4)

            # Calculate deltas
            delta_coaq = (fund_df['actq'] - fund_df['cheq']) - (fund_df['actq_lag'] - fund_df['cheq_lag'])
            delta_colq = (fund_df['lctq'] - fund_df['dlcq']) - (fund_df['lctq_lag'] - fund_df['dlcq_lag'])
            delta_ncoaq = (fund_df['atq'] - fund_df['actq'] - fund_df['ivao']) - (fund_df['atq_lag'] - fund_df['actq_lag'] - fund_df['ivao_lag'])
            delta_ncolq = (fund_df['ltq'] - fund_df['lctq'] - fund_df['dlttq']) - (fund_df['ltq_lag'] - fund_df['lctq_lag'] - fund_df['dlttq_lag'])
            delta_finaq = (fund_df['ivstq'] + fund_df['ivao']) - (fund_df['ivstq_lag'] + fund_df['ivao_lag'])
            delta_finlq = (fund_df['dlttq'] + fund_df['dlcq'] + fund_df['pstkq']) - (fund_df['dlttq_lag'] + fund_df['dlcq_lag'] + fund_df['pstkq_lag'])

            # Calculate total accruals
            denom = 0.5 * (fund_df['atq'] + fund_df['atq_lag'])
            fund_df[name] = ((delta_coaq - delta_colq) + (delta_ncoaq - delta_ncolq) + (delta_finaq - delta_finlq)) / denom
            return fund_df

    @factor
    def net_external_finance(self, qtr=True, name='f_nef'):
        """
        Net external finance:
        Change in equity and debt
        """
        if qtr:
            # sstk: sale of common and preferred stocks, atq: total assets, prstkc: purchase of common and preferred stocks, 
            # dvpsxq: cash dividends paid per share, cshoq: number of common shares outstanding, dltis: cash inflow issuance long-term debt, 
            # dltr: cash outflow reduction long-term debt, dlcch: change in current debt
            fund_df_quarter = self.wrds_manager.get_fundq(fund_list=["atq", "dvpsxq", "cshoq"], gvkey_list=self.gvkey_list)
            fund_df_annual = self.wrds_manager.get_funda(fund_list=["prstkc", "sstk", "dltis", "dltr", "dlcch"], gvkey_list=self.gvkey_list) 

            # Merge both data frames
            fund_df = merge_funda_fundq(fund_df_quarter, fund_df_annual)

            # for c/f term we fill na with 0
            for col in ['sstk', 'dvpsxq', 'prstkc', 'dltis', 'dltr', 'dlcch']:
                fund_df[col] = fillna_with_0(fund_df, col)
            
            # for b/s term we fill forward
            fund_df['cshoq'] = fill_forward(fund_df, 'cshoq')

            # actual dividends paid 
            fund_df["dvcq"] = fund_df["dvpsxq"] * fund_df["cshoq"]

            # Get current and lagged values
            ltm_cols = ['dvcq']
            for col in ltm_cols:
                fund_df[f'{col}_ltm'] = rolling_sum(fund_df, col)
                fund_df[f'{col}_lag'] = shift_n_rows(fund_df, f'{col}_ltm', 4)

            # we do not rolling sum becasue they are already annual data
            for col in ['atq', 'prstkc', 'sstk', 'dltis', 'dltr', 'dlcch']:
                fund_df[f'{col}_lag'] = shift_n_rows(fund_df, col, 4)
            
            # Calculate deltas
            fund_df['delta_equity'] = (fund_df['sstk'] - fund_df['prstkc'] - fund_df['dvcq_ltm']) - (fund_df['sstk_lag'] - fund_df['prstkc_lag'] - fund_df['dvcq_lag'])
            fund_df['delta_debt'] = (fund_df['dltis'] - fund_df['dltr'] - fund_df['dlcch']) - (fund_df['dltis_lag'] - fund_df['dltr_lag'] - fund_df['dlcch_lag'])

            # Calculate net external finance
            fund_df[name] = (fund_df['delta_equity'] + fund_df['delta_debt']) / (0.5*(fund_df["atq"] + fund_df["atq_lag"]))
            return fund_df

    @factor
    def return_net_operating_assets(self, qtr=True, name='f_rnoa'):
        """
        Return on net operating assets:
        Operating income / average net operating assets
        See Soliman (2008)

        note: in the paper (check under artifacts, definition is ambiguous
            on one hand, following their item lists, they claim the OA is calculated by AT - CHE - IVAO (item 32))
            on the other hand, if you just follow their words, they claim OA = AT - CHE
            after checking with the accounting intuitions and chatgpt, we go with the latter one.
        """
        if qtr:            
            # oiadpq: operating income before interest, atq: total assets, cheq: cash and short-term investments, 
            # dlttq: long-term debt, dlcq: short-term debt, ceqq: common equity, pstkq: preferred equity, mibq: minority interest
            fund_df = self.wrds_manager.get_fundq(fund_list=["oiadpq", "atq", "cheq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"], gvkey_list=self.gvkey_list)
            
            # Get current and lagged values
            fund_df['oiadpq_ltm'] = rolling_sum(fund_df, 'oiadpq') # an income term

            # Fill forward for LTM values
            ltm_cols = ["atq", "cheq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]
            for col in ltm_cols:
                fund_df[col] = fill_forward(fund_df, col) # the rest are bs terms

            # Calculate net operating assets
            fund_df['operating_assets'] = fund_df["atq"] - fund_df["cheq"]
            fund_df['operating_liabilities'] = fund_df["atq"] - fund_df["dlttq"] - fund_df["dlcq"] - fund_df["ceqq"] - fund_df["pstkq"] - fund_df["mibq"]
            fund_df["noaq"] = fund_df["operating_assets"] - fund_df["operating_liabilities"]
            fund_df["noaq_lag"] = shift_n_rows(fund_df, "noaq", 4)
            # Calculate return on net operating assets
            fund_df[name] = fund_df["oiadpq_ltm"] / (0.5*fund_df["noaq"] + 0.5*fund_df["noaq_lag"])
            return fund_df
    
    @factor
    def profit_margin(self, qtr=True, name='f_pm'):
        """
        Profit margin:
        Operating income / Sales
        See Soliman (2008)
        """
        if qtr:
            fund_df = self.wrds_manager.get_fundq(fund_list=["oiadpq", "saleq"], gvkey_list=self.gvkey_list) # oiadpq: operating income before interest, saleq: sales

            # Get current values
            fund_df['oiadpq_ltm'] = rolling_sum(fund_df, 'oiadpq')
            fund_df['saleq_ltm'] = rolling_sum(fund_df, 'saleq')

            # Calculate profit margin
            fund_df[name] = fund_df["oiadpq_ltm"] / fund_df["saleq_ltm"]
        return fund_df

    @factor
    def asset_turnover(self, qtr=True, name='f_at'):
        """
        Asset turnover:
        Sales / Average Net Operating Assets
        See Soliman (2008)
        """
        if qtr:
            # saleq: sales, atq: total assets, cheq: cash, ivao: short-term investments, dlttq: long-term debt, dlcq: short-term debt, ceqq: common equity, pstkq: preferred equity, mibq: minority interest
            fund_df = self.wrds_manager.get_fundq(fund_list=["saleq", "atq", "cheq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"], gvkey_list=self.gvkey_list) 
            
            # Get current and lagged values
            fund_df['saleq_ltm'] = rolling_sum(fund_df, 'saleq')

            ltm_cols = ["atq", "cheq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]
            for col in ltm_cols:
                fund_df[col] = fill_forward(fund_df, col)

            # Calculate net operating assets
            fund_df['operating_assets'] = fund_df["atq"] - fund_df["cheq"]
            fund_df['operating_liabilities'] = fund_df["atq"] - fund_df["dlttq"] - fund_df["dlcq"] - fund_df["ceqq"] - fund_df["pstkq"] - fund_df["mibq"]
            fund_df["noaq"] = fund_df["operating_assets"] - fund_df["operating_liabilities"]
            fund_df["noaq_lag"] = shift_n_rows(fund_df, "noaq", 4)

            # Calculate asset turnover
            fund_df[name] = fund_df["saleq_ltm"] / (0.5* fund_df["noaq"] + 0.5*fund_df["noaq_lag"])
        return fund_df

    @factor
    def operating_profits_to_equity(self, qtr=True, name='f_opte'):
        """
        Operating profits to equity:
        (Operating Income - Interest Expense) / Book equity, where Book equity is defined as Common Equity + Deferred Taxes and Investment Tax Credit - Preferred Stock
        """
        if qtr:
            # saleq: sales, cogsq: cost of goods sold, xsgaq: general and administrative expenses, 
            # xintq: interest expense, seqq: total equity, txditcq: deferred taxes and investment tax credit, pstkq: preferred stock
            fund_df = self.wrds_manager.get_fundq(fund_list=["saleq", "cogsq", "xsgaq", "xintq", "seqq", "txditcq", "pstkq"], gvkey_list=self.gvkey_list)
            
            # Get current and lagged values
            # Calculate last twelve months (LTM) sums for relevant columns
            for col in ['saleq', 'cogsq', 'xsgaq', 'xintq']:
                fund_df[col] = fillna_with_0(fund_df, col)
                fund_df[f'{col}_ltm'] = rolling_sum(fund_df, col)
            # Calculate 4-quarter lag for equity-related columns
            for col in ['seqq', 'txditcq', 'pstkq']:
                fund_df[col] = fill_forward(fund_df, col)
                fund_df[f'{col}_lag'] = shift_n_rows(fund_df, col, 4)

            # Calculate operating profits to equity
            fund_df[name] = (fund_df['saleq_ltm'] - fund_df['cogsq_ltm'] - fund_df['xsgaq_ltm'] - fund_df['xintq_ltm']) / (fund_df['seqq_lag'] + fund_df['txditcq_lag'] - fund_df['pstkq_lag'])
        return fund_df

    @factor
    def book_leverage(self, qtr=True, name='f_bl'):
        """
        Book leverage:
        Total assets / Book equity, where Book equity is defined as Common Equity + Deferred Taxes and Investment Tax Credit - Preferred Stock
        """
        if qtr:
            # atq: total assets, seqq: total equity, txditcq: deferred taxes and investment tax credit, pstkq: preferred stock
            fund_df = self.wrds_manager.get_fundq(fund_list=["atq", "seqq", "txditcq", "pstkq"], gvkey_list=self.gvkey_list) 
            
            for col in ['txditcq', 'pstkq']:
                fund_df[col] = fill_forward(fund_df, col)

            # Calculate book leverage
            fund_df[name] = fund_df["atq"] / (fund_df["seqq"] + fund_df["txditcq"] - fund_df["pstkq"])
        return fund_df
    
    @factor
    def financial_constraints(self, qtr=True, name='f_fc'):
        """
        Financial constraints:
        Kaplan-Zingales index: (Cash flow / capital) + Tobins`s Q + leverage - (dividends / capital) - (cash / capital)
        See Lamont et al. (2001) for exact formula and coefficients in front of variables
        """
        if qtr:
            # ibq: income before extraordinary items, prstkcq: purchase of common and preferred stocks, dvpsxq: cash dividends paid per share, dpq: depreciation and amorization, 
            # ppentq: property, plant, and equipment, atq: total assets, ceq: common equity, txdbq: deferred taxes, dlttq: long-term debt, dlcq: debt in current liabilities, seqq: stockholder equity, dvpq: preferred dividends, cheq: cash and short-term investments
            fund_df = self.wrds_manager.get_fundq(fund_list=["ibq", "dpq", "ppentq", "atq", "seqq", "txdbq", "dlttq", "dlcq", "ceqq", "dvpsxq", "cshoq", "dvpq", "cheq"], gvkey_list=self.gvkey_list) 
            
            # total cash dividends
            fund_df['dvpsxq'] = fillna_with_0(fund_df, 'dvpsxq')
            fund_df['cshoq'] = fillna_with_0(fund_df, 'cshoq')
            fund_df["dvcq"] = fund_df["dvpsxq"] * fund_df["cshoq"]

            # Get current values
            # LTM sums for relevant items
            for col in ['ibq', 'dpq', 'dvcq', 'dvpq']:
                fund_df[f'{col}_ltm'] = rolling_sum(fund_df, col)
            # Forward-fill for balance sheet items
            for col in ['atq', 'ceqq', 'txdbq', 'dlttq', 'dlcq', 'cheq', 'ppentq']:
                fund_df[col] = fill_forward(fund_df, col)

            # Get lagged value    
            fund_df['ppentq_lag'] = shift_n_rows(fund_df, 'ppentq', 4)     

            # Load market cap
            price_df = merge_mktcap_fundq(self.mktcap_df, fund_df)
            
            # Calculate single ratios
            price_df["cash_flow_to_capital"] = (price_df['ibq_ltm'] + price_df['dpq_ltm']) / price_df['ppentq_lag']
            price_df['tobinsq'] = (price_df['atq'] + price_df["marketcap"] - price_df['ceqq'] - price_df['txdbq']) / price_df['atq']
            price_df['leverage'] = (price_df['dlttq'] + price_df['dlcq']) / (price_df['dlttq'] + price_df['dlcq'] + price_df['ceqq'])
            price_df['dividends_to_capital'] = (price_df['dvcq_ltm'] + price_df['dvpq_ltm']) / price_df['ppentq_lag']
            price_df['cash_to_capital'] = price_df['cheq'] / price_df['ppentq_lag']

            # Calculate the KZ index
            price_df[name] = -1.001909*price_df['cash_flow_to_capital'] + 0.2826389*price_df['tobinsq'] + 3.139193*price_df['leverage'] - 39.3678*price_df['dividends_to_capital'] - 1.314759*price_df['cash_to_capital']

        return price_df
        
    @factor
    def book_scaled_asset_liquidity(self, qtr=True, name='f_bsal'):
        """
        Book-scaled asset liquidity:
        Sum of asset items scaled by total assets. Each asset item gets a coefficient based on its liquidity
        See Ortiz-Molina & Phillips (2014) for exact formula and coefficients in front of variables
        """
        if qtr:
            # cheq: cash and short-term investments, atq: total assets, actq: current assets, ppentq: property, plant, and equipment
            fund_df = self.wrds_manager.get_fundq(fund_list=["cheq", "atq", "actq", "ppentq"], gvkey_list=self.gvkey_list) 
            
            # Get current values
            for col in ['actq', 'ppentq', 'atq', 'cheq']:
                fund_df[col] = fill_forward(fund_df, col)

            # Calculate book-scaled asset liquidity
            denom = fund_df["atq"]
            fund_df[name] = -(fund_df["cheq"]/denom + 0.75*(fund_df["actq"] - fund_df["cheq"])/denom + 0.5*fund_df["ppentq"]/denom)
        return fund_df

    @factor
    def market_scaled_asset_liquidity(self, qtr=True, name='f_msal'):
        """
        Market-scaled asset liquidity:
        Sum of asset items scaled by market assets. Each asset item gets a coefficient based on its liquidity
        See Ortiz-Molina & Phillips (2014) for exact formula and coefficients in front of variables
        """
        if qtr:
            # cheq: cash and short-term investments, atq: total assets, actq: non-cash current assets, ppentq: property, plant, and equipment, ceqq: common equity, txditcq: deferred taxes and investment tax credit, pstkq: preferred stock
            fund_df = self.wrds_manager.get_fundq(fund_list=["cheq", "atq", "actq", "ppentq", "seqq", "txditcq", "pstkq"], gvkey_list=self.gvkey_list) 
            
            # Get current values
            for col in ["actq", "ppentq", "atq", "cheq", "seqq", "txditcq", "pstkq"]:
                fund_df[col] = fill_forward(fund_df, col)

            # Get market cap
            price_df = merge_mktcap_fundq(self.mktcap_df, fund_df)

            # Calculate market assets
            price_df["book_equity"] = price_df["seqq"] + price_df["txditcq"] - price_df["pstkq"]
            price_df["market_assets"] = price_df["atq"] - price_df["book_equity"] + price_df["marketcap"]

            # Calculate market-scaled asset liquidity
            denom = price_df["market_assets"]    
            price_df[name] = -(price_df["cheq"]/denom + 0.75*(price_df["actq"] - price_df["cheq"])/denom + 0.50*price_df["ppentq"]/denom)
        return price_df