import pandas as pd

from db_manager.wrds_sql import get_fundq, get_funda, marketcap_calculator, get_crsp_daily
from utils.save_file import save_file
from utils.necessary_cond_calculation import check_if_calculation_needed
from utils.sneak_peek import sneak_peek

class FactorComputer():
    def __init__(self, verbose, db, gvkey_list):
        self.verbose = verbose
        self.db = db
        self.gvkey_list = gvkey_list

    def gross_profit_to_assets(self, qtr=True, name='f_gpta'):
        """
        Gross profit to assets:
        (Revenue - Cost of Goods Sold) / Total Assets
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # revtq: revenue, cogsq: cost of goods sold, atq: total assets
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["revtq", "cogsq", "atq"], verbose=self.verbose) 
            
            # group by gvkey and rolling sum of saleq for the last 4 quarters
            fund_df['revtq_ltm'] = fund_df.groupby('gvkey')['revtq'].transform(lambda x: x.rolling(window=4).sum())
            fund_df['cogsq_ltm'] = fund_df.groupby('gvkey')['cogsq'].transform(lambda x: x.rolling(window=4).sum())
            # atq: total assets, fill na with last value, group by gvkey
            fund_df['atq'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.fillna(method='ffill'))
            
            # calculate gross profit to assets
            fund_df[name] = (fund_df['revtq_ltm'] - fund_df['cogsq_ltm']) / fund_df['atq']
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
        """
        Sales to price:
        Sales / Market Cap
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["saleq"]) # saleq: sales

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

    def earnings_to_price(self, qtr=True, name='f_ep'):
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

    # TODO: need to fix this
    def payout_yield(self, qtr=True, name='f_py'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["dvy", "dvpsxq", "cshoq"]) # ibq: income before taxes
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)


            price_df = get_crsp_daily(self.db)
            print(price_df)
            print(price_df.query("gvkey == '001690'"))
            assert False
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

    def ev_multiple(self, qtr=True, name='f_evm'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["dlttq", "dlcq", "mibtq", "cheq", "pstkq", "oibdpq"]) # dlttq: long term debt, dlcq: total current debt, mibtq: noncontrolling intrest, cheq: cash and equivalents, pstkq: preferred stock

            # fillna with 0
            fund_df['dlttq'] = fund_df['dlttq'].fillna(0)
            fund_df['dlcq'] = fund_df['dlcq'].fillna(0)
            fund_df['mibtq'] = fund_df['mibtq'].fillna(0)
            fund_df['cheq'] = fund_df['cheq'].fillna(0)
            fund_df['pstkq'] = fund_df['pstkq'].fillna(0)
            fund_df['oibdpq'] = fund_df['oibdpq'].fillna(0)

            fund_df['ebitda_ltm'] = round(fund_df.groupby('gvkey')['oibdpq'].transform(lambda x: x.rolling(window=4).sum()), 2)

            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)

            mktcap = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and mktcap
            mktcap = pd.merge_asof(mktcap, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward').dropna(subset=['rdq'])

            mktcap['ev'] = mktcap['marketcap'] + mktcap['dlttq'] + mktcap['dlcq'] + mktcap['mibtq'] - mktcap['cheq'] + mktcap['pstkq'] + mktcap['ebitda_ltm']
            mktcap[name] = mktcap['ev'] / mktcap['ebitda_ltm']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(mktcap)

            if self.gvkey_list is None:
                save_file(mktcap, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    # TODO: need to fix this
    def advertising_to_marketcap(self, qtr=True, name='f_adp'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if not qtr:
            fund_df = get_funda(db=self.db, gvkey_list=self.gvkey_list, fund_list=["xad"]) # adpq: advertising expenses

            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)
            
    def rd_to_marketcap(self, qtr=True, name='f_rdp'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["xrdq"]) # xrdq: research and development expenses

            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)
            
            mktcap = marketcap_calculator(self.db, self.gvkey_list)

            # merge the fund_df and mktcap
            mktcap = pd.merge_asof(mktcap, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward').dropna(subset=['rdq'])
            mktcap['xrdq'] = mktcap['xrdq'].fillna(0)

            mktcap[name] = mktcap['xrdq'] / mktcap['marketcap']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(mktcap)
            
            if self.gvkey_list is None:
                save_file(mktcap, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def operating_leverage(self, qtr=True, name='f_ol'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["xsgaq", "cogsq"]) # xsgaq: selling, general and administrative expenses, cogsq: cost of goods sold

            fund_df['xsgaq_ltm'] = round(fund_df.groupby('gvkey')['xsgaq'].transform(lambda x: x.rolling(window=4).sum()), 2)
            fund_df['cogsq_ltm'] = round(fund_df.groupby('gvkey')['cogsq'].transform(lambda x: x.rolling(window=4).sum()), 2)
            
            # calculate operating leverage
            fund_df[name] = fund_df['xsgaq_ltm'] / (fund_df['xsgaq_ltm']+fund_df['cogsq_ltm'])
            
            if self.verbose:
                print("peeks at the data right after getting from wrds")
                sneak_peek(fund_df)
            
            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    # andre's contribution
    def return_on_assets(self, qtr=True, name='f_roa'):
        """
        Return on assets:
        Income Before Extraordinary Items / Total Assets
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["ibq", "atq"]) # ibq: income before extraordinary items, atq: total assets
            
            # group by gvkey and rolling sum of ibq for the last 4 quarters
            fund_df['ibq_ltm'] = fund_df.groupby('gvkey')['ibq'].transform(lambda x: x.rolling(window=4).sum())
            fund_df['atq'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.fillna(method='ffill'))
        
            # calculate return on assets
            fund_df[name] = fund_df['ibq_ltm'] / fund_df['atq']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def sales_growth_rank(self, qtr=True, name='f_sgr'):
        """
        Sales growth rank:
        Sales growth rate (CAGR) over the last 5 years
        Ranked from 1 to 10 by the sales growth rate, cross-sectional
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["saleq"])  # saleq: sales
            
            # Get lagged sale variable
            fund_df['saleq_lag'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.shift(20).rolling(4).sum()) # by 5 years
            fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum())

            # Calculate growth rate
            fund_df['five_year_sales_cagr'] = (fund_df['saleq_ltm'] / fund_df['saleq_lag']) ** (1/5) - 1

            # TODO: from cagr to rank (CROSS SECTIONAL)

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def abnormal_capital_investment(self, qtr=True, name='f_aci'):
        """
        Abnormal capital investment: defined by Titman et al. (2004)
        Description: xxxxxxxxx
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["capxy", "saleq"]) # capxy: capital expenditure, saleq: sales
            
            # Get variables from past four quarters
            fund_df['capxy_ltm'] = fund_df.groupby('gvkey')['capxy'].transform(lambda x: x.rolling(4).sum())
            fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(4).sum())

            # Get lagged variables (4, 8, 12 quarters ago, rolling 4 quarters sum)
            for lag in [4, 8, 12]:
                fund_df[f'capxy_ltm_lag_{lag}'] = fund_df.groupby('gvkey')['capxy'].transform(lambda x: x.shift(lag).rolling(4).sum())
                fund_df[f'saleq_ltm_lag_{lag}'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.shift(lag).rolling(4).sum())

            # Calculate abnormal capital investment 
            avg_past = (
                fund_df['capxy_ltm_lag_4'] / fund_df['saleq_ltm_lag_4'] +
                fund_df['capxy_ltm_lag_8'] / fund_df['saleq_ltm_lag_8'] +
                fund_df['capxy_ltm_lag_12'] / fund_df['saleq_ltm_lag_12']
            ) / 3
            fund_df[name] = (fund_df['capxy_ltm'] / fund_df['saleq_ltm']) / avg_past - 1

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def investment_to_assets(self, qtr=True, name='f_ita'):
        """
        Investment to assets: Cooper et al. (2008)
        Growth of total assets
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["atq"]) # atq: total assets
            fund_df['atq'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.fillna(method='ffill'))
            
            # Calculate investment to assets using current and lagged assets
            fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].shift(4)
            fund_df[name] = (fund_df['atq'] - fund_df['atq_lag']) / fund_df['atq_lag']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def changes_in_ppe(self, qtr=True, name='f_ppe'):
        """
        Changes in ppe, inventory to assets: Lyandres et al. (2008)
        Change in property, plant, and equipment, and inventory, scaled by assets
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["invtq", "atq", "ppegtq"]) # invtq: inventories, atq: total assets, ppegtq: property, plant, and equipment
            fund_df['atq'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.fillna(method='ffill'))
            
            # Calculate lagged values
            for col in ['atq', 'invtq', 'ppegtq']:
                fund_df[f'{col}_lag'] = fund_df.groupby('gvkey')[col].shift(4)

            # Calculate change in ppe and inventory, scaled by lagged assets
            fund_df[name] = ((fund_df['ppegtq'] - fund_df['ppegtq_lag']) + (fund_df['invtq'] - fund_df['invtq_lag'])) / fund_df['atq_lag']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'

    def investment_growth(self, qtr=True, name='f_ig'):
        """
        Investment growth:
        Growth of capital expenditure
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["capxy"]) # capxy: capital expenditure
            
            # Get current and lagged capex
            fund_df['capxy_ltm'] = fund_df.groupby('gvkey')['capxy'].transform(lambda x: x.rolling(4).sum())
            fund_df['capxy_lag'] = fund_df.groupby('gvkey')['capxy'].transform(lambda x: x.shift(4).rolling(4).sum())

            # Calculate investment growth
            fund_df[name] = (fund_df['capxy_ltm'] - fund_df['capxy_lag']) / fund_df['capxy_lag']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}'


    def inventory_changes(self, qtr=True, name='f_ic'):
        """
        Inventory changes:
        Change in inventory, scaled by assets
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["invtq", "atq"]) # invtq: inventory, atq: total assets
            fund_df['atq'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.fillna(method='ffill'))
            fund_df['invtq'] = fund_df.groupby('gvkey')['invtq'].transform(lambda x: x.fillna(method='ffill'))

            # Calculate lagged values
            for col in ['atq', 'invtq']:
                fund_df[f'{col}_lag'] = fund_df.groupby('gvkey')[col].shift(4)

            # Calculate inventory change
            fund_df[name] = (fund_df['invtq'] - fund_df['invtq_lag']) / (0.5 * (fund_df['atq'] + fund_df['atq_lag']))

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 


    def operating_accruals(self, qtr=True, name='f_oa'):
        """
        Operating accruals:
        description:
        from paper:
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            _to_retrieve = ["actq", "atq", "cheq", "lctq", "dlcq", "ivaoq", "ltq", "dlttq", "ivstq", "pstkq"]
            # actq: current assets, atq: total assets, cheq: cash and cash equivalents, lctq: current liabilities, dlcq: short-term debt, ivaoq: investments and advances, ltq: total liabilities, dlttq: long-term debt, ivstq: short-term investments, pstkq: preferred stock
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=_to_retrieve) 

            # Get lagged values (4 quarters ago)
            lag_cols = _to_retrieve
            for col in lag_cols:
                fund_df[col] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill'))
                fund_df[f"{col}_lag"] = fund_df.groupby('gvkey')[col].shift(4)

            # Calculate deltas
            delta_coaq = (fund_df['actq'] - fund_df['cheq']) - (fund_df['actq_lag'] - fund_df['cheq_lag'])
            delta_colq = (fund_df['lctq'] - fund_df['dlcq']) - (fund_df['lctq_lag'] - fund_df['dlcq_lag'])
            delta_ncoaq = (fund_df['atq'] - fund_df['actq'] - fund_df['ivaoq']) - (fund_df['atq_lag'] - fund_df['actq_lag'] - fund_df['ivaoq_lag'])
            delta_ncolq = (fund_df['ltq'] - fund_df['lctq'] - fund_df['dlttq']) - (fund_df['ltq_lag'] - fund_df['lctq_lag'] - fund_df['dlttq_lag'])
            delta_finaq = (fund_df['ivstq'] + fund_df['ivaoq']) - (fund_df['ivstq_lag'] + fund_df['ivaoq_lag'])
            delta_finlq = (fund_df['dlttq'] + fund_df['dlcq'] + fund_df['pstkq']) - (fund_df['dlttq_lag'] + fund_df['dlcq_lag'] + fund_df['pstkq_lag'])

            # Calculate operating accruals
            denom = 0.5 * (fund_df['atq'] + fund_df['atq_lag'])
            fund_df[name] = ((delta_coaq - delta_colq) + (delta_ncoaq - delta_ncolq) + (delta_finaq - delta_finlq)) / denom

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 

    # TODO: check correctness of the "xxxxy" columns
    def net_external_finance(self, qtr=True, name='f_nef'):
        """
        Net external finance:
        Change in equity and debt
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # sstky: sale of common and preferred stocks, atq: total assets, prstkcy: purchase of common and preferred stocks, dvpsxq: cash dividends paid per share, cshoq: number of common shares outstanding, dltisy: cash inflow issuance long-term debt, dltry: cash outflow reduction long-term debt, dlcchy: change in current debt
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["sstky", "atq", "prstkcy", "dvpsxq", "cshoq", "dltisy", "dltry", "dlcchy"]) 
            
            # Declared dividends is not available on quarterly basis. Therefore, we construct an approximation
            fund_df["dvcq"] = fund_df["dvpsxq"] * fund_df["cshoq"]

            # Get current and lagged values
            ltm_cols = ['sstky', 'prstkcy', 'dvcq', 'dltisy', 'dltry', 'dlcchy']
            for col in ltm_cols:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.rolling(4).sum())
                fund_df[f'{col}_lag'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.shift(4).rolling(4).sum())
            fund_df['atq'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.fillna(method='ffill'))
            fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4))

            # Calculate deltas
            fund_df['delta_equityq'] = (fund_df['sstky_ltm'] - fund_df['prstkcy_ltm'] - fund_df['dvcq_ltm']) - (fund_df['sstky_lag'] - fund_df['prstkcy_lag'] - fund_df['dvcq_lag'])
            fund_df['delta_debtq'] = (fund_df['dltisy_ltm'] - fund_df['dltry_ltm'] - fund_df['dlcchy_ltm']) - (fund_df['dltisy_lag'] - fund_df['dltry_lag'] - fund_df['dlcchy_lag'])

            # Calculate net external finance
            fund_df[name] = (fund_df['delta_equityq'] + fund_df['delta_debtq']) / (0.5*fund_df["atq_ltm"] + 0.5*fund_df["atq_lag"])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 
    

    def return_net_operating_assets(self, qtr=True, name='f_rnoa'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # oiadpq: operating income before interest, atq: total assets, cheq: cash, ivao: short-term investments, dlttq: long-term debt, dlcq: short-term debt, ceqq: common equity, pstkq: preferred equity, mibq: minority interest
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["oiadpq", "atq", "cheq", "ivaoq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]) 
            
            # Get current and lagged values
            fund_df['oiadpq_ltm'] = fund_df.groupby('gvkey')['oiadpq'].transform(lambda x: x.rolling(4).sum()) # a income term

            # Fill forward for LTM values
            ltm_cols = ["atq", "cheq", "ivaoq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]
            for col in ltm_cols:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill')) # the rest are bs terms

            # Lagged values (4 quarters back)
            for col in ltm_cols:
                fund_df[f'{col}_lag'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.shift(4))

            # Calculate net operating assets
            fund_df["noaq"] = (fund_df["atq_ltm"] - fund_df["cheq_ltm"] - fund_df["ivaoq_ltm"]) - (fund_df["atq_ltm"] - fund_df["dlttq_ltm"] - fund_df["dlcq_ltm"] - fund_df["ceqq_ltm"] - fund_df["pstkq_ltm"] - fund_df["mibq_ltm"])
            fund_df["noaq_lag"] = (fund_df["atq_lag"] - fund_df["cheq_lag"] - fund_df["ivaoq_lag"]) - (fund_df["atq_lag"] - fund_df["dlttq_lag"] - fund_df["dlcq_lag"] - fund_df["ceqq_lag"] - fund_df["pstkq_lag"] - fund_df["mibq_lag"])
        
            # Calculate return on net operating assets
            fund_df[name] = fund_df["oiadpq_ltm"] / (0.5* fund_df["noaq"] + 0.5*fund_df["noaq_lag"])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 
    
    
    def profit_margin(self, qtr=True, name='f_pm'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["oiadpq", "saleq"]) # oiadpq: operating income before interest, saleq: sales
            
            # Get current values
            fund_df['oiadpq_ltm'] = fund_df.groupby('gvkey')['oiadpq'].transform(lambda x: x.rolling(4).sum())
            fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(4).sum())

            # Calculate profit margin
            fund_df[name] = fund_df["oiadpq_ltm"] / fund_df["saleq_ltm"]

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 

    def asset_turnover(self, qtr=True, name='f_at'):
        """
        Asset turnover:
        Sales / Net Operating Assets
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # saleq: sales, atq: total assets, cheq: cash, ivaoq: short-term investments, dlttq: long-term debt, dlcq: short-term debt, ceqq: common equity, pstkq: preferred equity, mibq: minority interest
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["saleq", "atq", "cheq", "ivaoq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]) 
            
            # Get current and lagged values
            fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(4).sum())

            ltm_cols = ["atq", "cheq", "ivaoq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]
            for col in ltm_cols:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill'))
                fund_df[f'{col}_lag'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.shift(4))

            # Calculate net operating assets
            fund_df["noaq"] = (fund_df["atq_ltm"] - fund_df["cheq_ltm"] - fund_df["ivaoq_ltm"]) - (fund_df["atq_ltm"] - fund_df["dlttq_ltm"] - fund_df["dlcq_ltm"] - fund_df["ceqq_ltm"] - fund_df["pstkq_ltm"] - fund_df["mibq_ltm"])
            fund_df["noaq_lag"] = (fund_df["atq_lag"] - fund_df["cheq_lag"] - fund_df["ivaoq_lag"]) - (fund_df["atq_lag"] - fund_df["dlttq_lag"] - fund_df["dlcq_lag"] - fund_df["ceqq_lag"] - fund_df["pstkq_lag"] - fund_df["mibq_lag"])
        
            # Calculate asset turnover
            fund_df[name] = fund_df["saleq_ltm"] / (0.5* fund_df["noaq"] + 0.5*fund_df["noaq_lag"])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 

    def operating_profits_to_equity(self, qtr=True, name='f_ope'):
        """
        Operating profits to equity:
        (Operating Income - Interest Expense) / (Common Equity + Deferred Taxes and Investment Tax Credit - Preferred Stock)
        """
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
             # saleq: sales, cogsq: cost of goods sold, xsgaq: general and administrative expenses, xintq: interest expense, ceqq: common equity, txditcq: deferred taxes and investment tax credit, pstkq: preferred stock
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["saleq", "cogsq", "xsgaq", "xintq", "ceqq", "txditcq", "pstkq"])
            
            # Get current and lagged values
            # Calculate last twelve months (LTM) sums for relevant columns
            for col in ['saleq', 'cogsq', 'xsgaq', 'xintq']:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.rolling(4).sum())
            # Calculate 4-quarter lag for equity-related columns
            for col in ['ceqq', 'txditcq', 'pstkq']:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill'))
                fund_df[f'{col}_lag'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.shift(4))

            # Calculate operating profits to equity
            fund_df[name] = (fund_df['saleq_ltm'] - fund_df['cogsq_ltm'] - fund_df['xsgaq_ltm'] - fund_df['xintq_ltm']) / (fund_df['ceqq_lag'] + fund_df['txditcq_lag'] - fund_df['pstkq_lag'])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 


    def book_leverage(self, qtr=True, name='f_bl'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # atq: total assets, ceqq: common equity, txditcq: deferred taxes and investment tax credit, pstkq: preferred stock
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["atq", "ceqq", "txditcq", "pstkq"]) 
            
            # Calculate book leverage
            fund_df[name] = fund_df["atq"] / (fund_df["ceqq"] + fund_df["txditcq"] - fund_df["pstkq"])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 
        

    def financial_constraints(self, qtr=True, name='f_fc'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # ibq: income before extraordinary items, prstkcq: purchase of common and preferred stocks, dvpsxq: cash dividends paid per share, dpq: depreciation and amorization, ppentq: property, plant, and equipment, atq: total assets, ceq: common equity, txdbq: deferred taxes, dlttq: long-term debt, dlcq: debt in current liabilities, seqq: stockholder equity, dvpq: preferred dividends, cheq: cash and short-term investments
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["ibq", "dpq", "ppentq", "atq", "ceqq", "txdbq", "dlttq", "dlcq", "seqq", "dvpsxq", "cshoq", "dvpq", "cheq"]) 
            
            # Declared dividends is not available on quarterly basis. Therefore, we construct an approximation
            fund_df["dvcq"] = fund_df["dvpsxq"] * fund_df["cshoq"]

            # Get current values
            # LTM sums for relevant items
            for col in ['ibq', 'dpq', 'dvcq', 'dvpq']:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.rolling(4).sum())
            # Forward-fill for balance sheet items
            for col in ['atq', 'ceqq', 'txdbq', 'dlttq', 'dlcq', 'seqq', 'cheq']:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill'))

            # Get lagged value    
            fund_df['ppentq_lag'] = fund_df.groupby('gvkey')['ppentq'].transform(lambda x: x.shift(4))     

            # Load market cap
            price_df = marketcap_calculator(self.db, self.gvkey_list)

            # Merge the fund_df and price_df
            price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')

            # Calculate market assets
            price_df["book_equity"] = price_df["ceqq_ltm"] + price_df["txditcq_ltm"] - price_df["pstkq_ltm"]
            price_df["market_assets"] = price_df["atq_ltm"] - price_df["book_equity"] + price_df["marketcap"]
            
            # Calculate single ratios
            price_df["cash_flow_to_capital"] = (price_df['ibq_ltm'] + price_df['dpq_ltm']) / price_df['ppentq_lag']
            price_df['tobins_q'] = (price_df['atq_ltm'] + price_df["marketcap"] - price_df['ceqq_ltm'] - price_df['txdbq_ltm']) / price_df['atq_ltm']
            price_df['leverage'] = (price_df['dlttq_ltm'] + price_df['dlcq_ltm']) / (price_df['dlttq_ltm'] + price_df['dlcq_ltm'] + price_df['seqq_ltm'])
            price_df['dividends_to_capital'] = (price_df['dvcq_ltm'] + price_df['dvpq_ltm']) / price_df['ppentq_lag']
            price_df['cash_to_capital'] = price_df['cheq_ltm'] / price_df['ppentq_lag']

            # Calculate the KZ index
            price_df[name] = -1.001909*price_df['cash_flow_to_capital'] + 0.2826389*price_df['tobins_q'] + 3.139193*price_df['leverage'] - 39.3678*price_df['dividends_to_capital'] - 1.314759*price_df['cash_to_capital']

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(price_df)

            if self.gvkey_list is None:
                save_file(price_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 
        

    def book_scaled_asset_liquidity(self, qtr=True, name='f_bsal'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # cheq: cash and short-term investments, atq: total assets, actq: non-cash current assets, ppentq: property, plant, and equipment
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["cheq", "atq", "actq", "ppentq"]) 
            
            # Get current values
            for col in ['actq', 'ppentq', 'atq', 'cheq']:
                fund_df[f'{col}_ltm'] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill'))

            # Calculate book-scaled asset liquidity
            fund_df[name] = -(fund_df["cheq_ltm"]/fund_df["atq_ltm"] + 0.75*(fund_df["actq_ltm"] - fund_df["cheq_ltm"])/fund_df["atq_ltm"] + 0.5*fund_df["ppentq_ltm"]/fund_df["atq_ltm"])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(fund_df)

            if self.gvkey_list is None:
                save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 


    def market_scaled_asset_liquidity(self, qtr=True, name='f_msal'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            # cheq: cash and short-term investments, atq: total assets, actq: non-cash current assets, ppentq: property, plant, and equipment, ceqq: common equity, txditcq: deferred taxes and investment tax credit, pstkq: preferred stock
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["cheq", "atq", "actq", "ppentq", "ceqq", "txditcq", "pstkq"]) 
            
            # Get current values
            for col in ["actq", "ppentq", "atq", "cheq", "ceqq", "txditcq", "pstkq"]:
                fund_df[f"{col}_ltm"] = fund_df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill'))

            # Get market cap
            price_df = marketcap_calculator(self.db, self.gvkey_list)

            # Merge the fund_df and marketcap_df
            price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')

            # Calculate market assets
            price_df["book_equity"] = price_df["ceqq_ltm"] + price_df["txditcq_ltm"] - price_df["pstkq_ltm"]
            price_df["market_assets"] = price_df["atq_ltm"] - price_df["book_equity"] + price_df["marketcap"]
            
            # Calculate market-scaled asset liquidity
            price_df[name] = -(price_df["cheq_ltm"]/price_df["market_assets"] + 0.75*(price_df["actq_ltm"] - price_df["cheq_ltm"])/price_df["market_assets"] + 0.50*price_df["ppentq_ltm"]/price_df["market_assets"])

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(price_df)

            if self.gvkey_list is None:
                save_file(price_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 


    def dividend_yield(self, qtr=True, name='f_dy'):
        if not check_if_calculation_needed(name, self.gvkey_list):
            return f'Done with {name}'
        if qtr:
            fund_df = get_fundq(db=self.db, gvkey_list=self.gvkey_list, fund_list=["dvpsxq", "cshoq"]) # dvpsxq: cash dividends paid per share, cshoq: common shares outstanding
            
            # Calculate paid dividends
            fund_df["divq"] = fund_df["dvpsxq"] * fund_df["cshoq"]
            fund_df["divq_ltm"] = fund_df.groupby('gvkey')['divq'].transform(lambda x: x.rolling(4).sum())

            # Get market cap
            mktcap_df = marketcap_calculator(self.db, self.gvkey_list)

            # Merge the fund_df and marketcap_df
            mktcap_df = pd.merge_asof(mktcap_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')

            # Calculate dividend yield
            mktcap_df["price"] = mktcap_df["marketcap"] / mktcap_df["cshoq"] # marketcap is already in the mktcap_df
            mktcap_df[name] = mktcap_df["divq_ltm"] / mktcap_df["marketcap"]

            if self.verbose:
                print("peeks at the data after calculation!")
                sneak_peek(mktcap_df)

            if self.gvkey_list is None:
                save_file(mktcap_df, name) # only save the file if gvkey_list is None (meaning select all)
        return f'Done with {name}' 