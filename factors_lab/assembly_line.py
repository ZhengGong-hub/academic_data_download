import pandas as pd

from db_manager.wrds_sql import get_fundq, get_funda, marketcap_calculator
from utils.save_file import save_file
from utils.sneak_peek import sneak_peek
from utils.necessary_cond_calculation import check_if_calculation_needed

class FactorComputer():
    def __init__(self, verbose, db, gvkey_list):
        self.verbose = verbose
        self.db = db
        self.gvkey_list = gvkey_list

    def gross_profit_to_assets(self, qtr=True, name='f_gpta'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["revtq", "cogsq", "atq"]) # revtq: revenue, cogsq: cost of goods sold, atq: total assets
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)
            
            # group by gvkey and rolling sum of saleq for the last 4 quarters
            fund_df['revtq_ltm'] = fund_df.groupby('gvkey')['revtq'].transform(lambda x: x.rolling(window=4).sum())
            fund_df['cogsq_ltm'] = fund_df.groupby('gvkey')['cogsq'].transform(lambda x: x.rolling(window=4).sum())
            fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(window=4).mean())
            
            # calculate gross profit to assets
            fund_df[name] = (fund_df['revtq_ltm'] - fund_df['cogsq_ltm']) / fund_df['atq_ltm']
        else:
            raise ValueError("qtr var error!")
        # peek at the data    
        if self.verbose:
            print("peeks at the data after calculation!")
            sneak_peek(fund_df)

        if self.gvkey_list is None:
            save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def sales_to_price(self, qtr=True, name='f_sp'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["saleq"]) # saleq: sales
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)

            fund_df['saleq_ltm'] = round(fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum()), 2)

            price_df = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and price_df
            price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
            price_df[name] = price_df['saleq_ltm'] / price_df['marketcap']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(price_df)

            if self.gvkey_list is None:
                save_file(price_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def btm(self, qtr=True, name='f_btm'):
        # BE = SEQQ + TXDITC - PSTK
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["seqq", "txditcq", "pstkq"]) # seqq: retained earnings, txditcc: deferred income tax, pstk: preferred stock
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)

            # fillna with 0 or dropna
            fund_df.dropna(subset=['seqq'], inplace=True)
            fund_df['txditcq'] = fund_df['txditcq'].fillna(0)
            fund_df['pstkq'] = fund_df['pstkq'].fillna(0)

            fund_df['be'] = fund_df['seqq'] + fund_df['txditcq'] - fund_df['pstkq']

            # get marketcap
            mkt_cap = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and mkt_cap
            mkt_cap = pd.merge_asof(mkt_cap, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')

            mkt_cap[name] = mkt_cap['be'] / mkt_cap['marketcap']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(mkt_cap)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'
                
    def debt_to_equity(self, qtr=True, name='f_dte'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["dlttq", "dlcq"]) # dlttq: long term debt, dlcq: total current debt
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)
            
            # fillna with 0
            fund_df['dlcq'] = fund_df['dlcq'].fillna(0)
            fund_df['dlttq'] = fund_df['dlttq'].fillna(0)

            fund_df['total_debt'] = fund_df['dlttq'] + fund_df['dlcq']

            # get marketcap
            mkt_cap = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and mkt_cap
            mkt_cap = pd.merge_asof(mkt_cap, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')

            mkt_cap[name] = mkt_cap['total_debt'] / mkt_cap['marketcap']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(mkt_cap)
            
            if self.gvkey_list is None:
                save_file(mkt_cap, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def earnings_to_price(self, qtr=True, name='f_sp'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["ibq"]) # ibq: income before taxes
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)

            fund_df['e_ltm'] = round(fund_df.groupby('gvkey')['ibq'].transform(lambda x: x.rolling(window=4).sum()), 2)

            price_df = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and price_df
            price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
            price_df[name] = price_df['e_ltm'] / price_df['marketcap']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(price_df)

            if self.gvkey_list is None:
                save_file(price_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'


    def cashflow_to_price(self, qtr=True, name='f_cfp'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["ibq", "dpq"]) # ibq: income before taxes
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)
            fund_df['cashflow'] = fund_df['ibq'].fillna(0) + fund_df['dpq'].fillna(0)
            fund_df['cashflow_ltm'] = round(fund_df.groupby('gvkey')['cashflow'].transform(lambda x: x.rolling(window=4).sum()), 2)

            price_df = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and price_df
            price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
            price_df[name] = price_df['cashflow_ltm'] / price_df['marketcap']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(price_df)

            if self.gvkey_list is None:
                save_file(price_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'