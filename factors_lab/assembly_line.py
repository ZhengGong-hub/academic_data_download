import pandas as pd

from db_manager.wrds_sql import get_fundq, get_funda, marketcap_calculator
from utils.save_file import save_file

def gross_profit_to_assets(db, annual=False, verbose=False, name='f_gpta'):
    if annual:
        fund_df = get_funda(db, fund_list=["revt", "cogs", "at"]) # revt: revenue, cogsq: cost of goods sold, atq: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        fund_df[name] = (fund_df['revt'] - fund_df['cogs']) / fund_df['at']
    else:
        fund_df = get_fundq(db, fund_list=["revtq", "cogsq", "atq"]) # revtq: revenue, cogsq: cost of goods sold, atq: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))
        
        # group by gvkey and rolling sum of saleq for the last 4 quarters
        fund_df['revtq_ltm'] = fund_df.groupby('gvkey')['revtq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['cogsq_ltm'] = fund_df.groupby('gvkey')['cogsq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(window=4).mean())
        
        # calculate gross profit to assets
        fund_df[name] = (fund_df['revtq_ltm'] - fund_df['cogsq_ltm']) / fund_df['atq_ltm']

    # peek at the data    
    if verbose:
        print(fund_df.query("gvkey == '001690'"))

    # save the file 
    save_file(fund_df, name)
    return

def sales_to_price(db, annual=False, verbose=False, name='f_sp'):
    if not annual:
        fund_df = get_fundq(db, fund_list=["saleq"]) # saleq: sales
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        fund_df['saleq_ltm'] = round(fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum()), 2)

        marketcap_df = marketcap_calculator(db)

        # merge the fund_df and marketcap_df
        marketcap_df = pd.merge_asof(marketcap_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
        marketcap_df[name] = marketcap_df['saleq_ltm'] / marketcap_df['marketcap']

        if verbose:
            print(marketcap_df.query("gvkey == '001690'"))

        save_file(marketcap_df, name)
    return

def return_on_assets(db, annual=False, verbose=False, name='f_roa'):
    if annual:
        fund_df = get_funda(db, fund_list=["ib", "at"]) # ib: income before extraordinary items, at: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        fund_df[name] = fund_df['ib'] / fund_df['at']
    else:
        fund_df = get_fundq(db, fund_list=["ibq", "atq"]) # ibq: income before extraordinary items, atq: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))
        
        # group by gvkey and rolling sum of ibq for the last 4 quarters
        fund_df['ibq_ltm'] = fund_df.groupby('gvkey')['ibq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(window=4).mean())
        
        # calculate return on assets
        fund_df[name] = fund_df['ibq_ltm'] / fund_df['atq_ltm']

    # peek at the data    
    if verbose:
        print(fund_df.query("gvkey == '001690'"))

    # save the file 
    save_file(fund_df, name)
    return

import numpy as np




def sales_growth_rank(db, annual=False, verbose=False, name='f_sgr'):
    if annual:
        fund_df = get_funda(db, fund_list=["sale"])  # sale: sales
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged sale variable
        fund_df['sale_lag5'] = fund_df.groupby('gvkey')['sale'].shift(5)

        # Calculate growth rate
        fund_df['five_year_sales_cagr'] = (fund_df['sale'] / fund_df['sale_lag5']) ** (1/5) - 1

    else:
        fund_df = get_fundq(db, fund_list=["saleq"])  # saleq: sales
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged sale variable
        fund_df['saleq_lag'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.shift(20).rolling(4).sum())
        fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum())

        # Calculate growth rate
        fund_df['five_year_sales_cagr'] = (fund_df['saleq_ltm'] / fund_df['saleq_lag']) ** (1/5) - 1

    # peek and save
    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return


def abnormal_capital_investment(db, annual=False, verbose=False, name='f_aci'):
    if annual:
        fund_df = get_funda(db, fund_list=["capx", "sale"]) # capx: capital expenditure, sale: sales
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged variables
        fund_df['sale_lag1'] = fund_df.groupby('gvkey')['sale'].shift(1)
        fund_df['sale_lag2'] = fund_df.groupby('gvkey')['sale'].shift(2)
        fund_df['sale_lag3'] = fund_df.groupby('gvkey')['sale'].shift(3)

        fund_df['capx_lag1'] = fund_df.groupby('gvkey')['capx'].shift(1)
        fund_df['capx_lag2'] = fund_df.groupby('gvkey')['capx'].shift(2)
        fund_df['capx_lag3'] = fund_df.groupby('gvkey')['capx'].shift(3)

        # Calculate abnormal capital investment
        fund_df[name] = ((fund_df['capx'] / fund_df['sale']) / ((1/3) * (
                         (fund_df['capx_lag1'] / fund_df['sale_lag1']) + 
                         (fund_df['capx_lag2'] / fund_df['sale_lag2']) + 
                         (fund_df['capx_lag3'] / fund_df['sale_lag3']) ))) - 1
        
    else:
        fund_df = get_fundq(db, fund_list=["capxq", "saleq"]) # capxq: capital expenditure, saleq: sales
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get variables from past four quarters
        fund_df['capxq_ltm'] = fund_df.groupby('gvkey')['capxq'].transform(lambda x: x.rolling(4).sum())
        fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(4).sum())

        # Get lagged variables
        fund_df['capxq_ltm_lag_4'] = fund_df.groupby('gvkey')['capxq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['saleq_ltm_lag_4'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.shift(4).rolling(4).sum())

        fund_df['capxq_ltm_lag_8'] = fund_df.groupby('gvkey')['capxq'].transform(lambda x: x.shift(8).rolling(4).sum())
        fund_df['saleq_ltm_lag_8'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.shift(8).rolling(4).sum())

        fund_df['capxq_ltm_lag_12'] = fund_df.groupby('gvkey')['capxq'].transform(lambda x: x.shift(12).rolling(4).sum())
        fund_df['saleq_ltm_lag_12'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.shift(12).rolling(4).sum())

         # Calculate abnormal capital investment
        fund_df[name] = ((fund_df['capxq_ltm'] / fund_df['saleq_ltm']) / ((1/3) * (
                         (fund_df['capxq_ltm_lag_4'] / fund_df['saleq_ltm_lag_4']) + 
                         (fund_df['capxq_ltm_lag_8'] / fund_df['saleq_ltm_lag_8']) + 
                         (fund_df['capxq_ltm_lag_12'] / fund_df['saleq_ltm_lag_12']) ))) - 1

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return


def investment_to_assets(db, annual=False, verbose=False, name='f_ita'):
    if annual:
        fund_df = get_funda(db, fund_list=["at"]) # at: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged assets
        fund_df['at_lag'] = fund_df.groupby('gvkey')['at'].shift(1)

        # Calculate investment to assets
        fund_df[name] = (fund_df['at']-fund_df['at_lag']) / fund_df['at_lag']
        
    else:
        fund_df = get_fundq(db, fund_list=["atq"]) # atq: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged assets
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(4).mean())
        fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4).rolling(4).mean())

         # Calculate investment to assets
        fund_df[name] = (fund_df['atq_ltm']-fund_df['atq_lag']) / fund_df['atq_lag']

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return


def changes_in_ppe(db, annual=False, verbose=False, name='f_ppe'):
    if annual:
        fund_df = get_funda(db, fund_list=["invt", "at", "ppegt"]) # invt: inventories, at: total assets, ppegt: property, plant, and equipment
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged values
        fund_df['invt_lag'] = fund_df.groupby('gvkey')['invt'].shift(1)
        fund_df['ppegt_lag'] = fund_df.groupby('gvkey')['ppegt'].shift(1)
        fund_df['at_lag'] = fund_df.groupby('gvkey')['at'].shift(1)

        # Calculate change in ppe and inventory, scaled by assets
        fund_df[name] = ((fund_df['ppegt'] - fund_df['ppegt_lag']) + (fund_df['invt'] - fund_df['invt_lag'])) / fund_df['at_lag']
        
    else:
        fund_df = get_fundq(db, fund_list=["invtq", "atq", "ppegtq"]) # invt: inventories, at: total assets, ppegt: property, plant, and equipment
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged assets
        fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4).rolling(4).mean())

        fund_df['invtq_ltm'] = fund_df.groupby('gvkey')['invtq'].transform(lambda x: x.rolling(4).mean())
        fund_df['invtq_lag'] = fund_df.groupby('gvkey')['invtq'].transform(lambda x: x.shift(4).rolling(4).mean())

        fund_df['ppegtq_ltm'] = fund_df.groupby('gvkey')['ppegtq'].transform(lambda x: x.rolling(4).mean())
        fund_df['ppegtq_lag'] = fund_df.groupby('gvkey')['ppegtq'].transform(lambda x: x.shift(4).rolling(4).mean())

        # Calculate change in ppe and inventory, scaled by assets
        fund_df[name] = ((fund_df['ppegtq_ltm'] - fund_df['ppegtq_lag']) + (fund_df['invtq_ltm'] - fund_df['invtq_lag'])) / fund_df['atq_lag']

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return



def investment_growth(db, annual=False, verbose=False, name='f_ig'):
    if annual:
        fund_df = get_funda(db, fund_list=["capx"]) # capx: capital expenditure
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged values
        fund_df['capx_lag'] = fund_df.groupby('gvkey')['capx'].shift(1)

        # Calculate investment growth
        fund_df[name] = (fund_df['capx'] - fund_df['capx_lag']) / fund_df['capx_lag']
        
    else:
        fund_df = get_fundq(db, fund_list=["capxq"]) # capx: capital expenditure
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged capex
        fund_df['capxq_ltm'] = fund_df.groupby('gvkey')['capxq'].transform(lambda x: x.rolling(4).sum())
        fund_df['capxq_lag'] = fund_df.groupby('gvkey')['capxq'].transform(lambda x: x.shift(4).rolling(4).sum())

        # Calculate investment growth
        fund_df[name] = (fund_df['capxq_ltm'] - fund_df['capxq_lag']) / fund_df['capxq_lag']

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return


def inventory_changes(db, annual=False, verbose=False, name='f_ic'):
    if annual:
        fund_df = get_funda(db, fund_list=["invt", "at"]) # invt: inventory, at: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged values
        fund_df['invt_lag'] = fund_df.groupby('gvkey')['invt'].shift(1)
        fund_df['at_lag'] = fund_df.groupby('gvkey')['at'].shift(1)

        # Calculate inventory change
        fund_df[name] = (fund_df['invt'] - fund_df['invt_lag']) / (0.5*(fund_df['at'] + fund_df['at_lag']))
        
    else:
        fund_df = get_fundq(db, fund_list=["invtq", "atq"]) # invt: inventory, at: total assets
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged values
        fund_df['invtq_ltm'] = fund_df.groupby('gvkey')['invtq'].transform(lambda x: x.rolling(4).mean())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(4).mean())

        fund_df['invtq_lag'] = fund_df.groupby('gvkey')['invtq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4).rolling(4).mean())

        # Calculate inventory change
        fund_df[name] = (fund_df['invtq_ltm'] - fund_df['invtq_lag']) / (0.5*(fund_df['atq_ltm'] + fund_df['atq_lag']))

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return


def operating_accruals(db, annual=False, verbose=False, name='f_oa'):
    if annual:
        fund_df = get_funda(db, fund_list=["act", "at", "che", "lct", "dlc", "ivao", "lt", "dltt", "ivst", "pstkl"]) # act: current assets, at: total assets, che: cash and cash equivalents, lct: current liabilities, dlc: short-term debt, ivao: investments and advances, lt: total liabilities, dltt: long-term debt, ivst: short-term investments, pstkl: preferred stock
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged values
        fund_df['act_lag'] = fund_df.groupby('gvkey')['act'].shift(1)
        fund_df['at_lag'] = fund_df.groupby('gvkey')['at'].shift(1)
        fund_df['che_lag'] = fund_df.groupby('gvkey')['che'].shift(1)
        fund_df['lct_lag'] = fund_df.groupby('gvkey')['lct'].shift(1)
        fund_df['dlc_lag'] = fund_df.groupby('gvkey')['dlc'].shift(1)
        fund_df['ivao_lag'] = fund_df.groupby('gvkey')['ivao'].shift(1)
        fund_df['lt_lag'] = fund_df.groupby('gvkey')['lt'].shift(1)
        fund_df['dltt_lag'] = fund_df.groupby('gvkey')['dltt'].shift(1)
        fund_df['ivst_lag'] = fund_df.groupby('gvkey')['ivst'].shift(1)
        fund_df['pstkl_lag'] = fund_df.groupby('gvkey')['pstkl'].shift(1)

        # Calculate deltas
        fund_df['delta_coa'] = (fund_df['act'] - fund_df['che']) - (fund_df['act_lag'] - fund_df['che_lag'])
        fund_df['delta_col'] = (fund_df['lct'] - fund_df['dlc']) - (fund_df['lct_lag'] - fund_df['dlc_lag'])

        fund_df['delta_ncoa'] = (fund_df['at'] - fund_df['act'] - fund_df['ivao']) - (fund_df['at_lag'] - fund_df['act_lag'] - fund_df['ivao_lag'])
        fund_df['delta_ncol'] = (fund_df['lt'] - fund_df['lct'] - fund_df['dltt']) - (fund_df['lt_lag'] - fund_df['lct_lag'] - fund_df['dltt_lag'])

        fund_df['delta_fina'] = (fund_df['ivst'] + fund_df['ivao']) - (fund_df['ivst_lag'] + fund_df['ivao_lag'])
        fund_df['delta_finl'] = (fund_df['dltt'] + fund_df['dlc'] + fund_df['pstkl']) - (fund_df['dltt_lag'] + fund_df['dlc_lag'] + fund_df['pstkl_lag'])

        # Calculate operating accruals
        fund_df[name] = ((fund_df['delta_coa'] - fund_df['delta_col']) + (fund_df['delta_ncoa'] - fund_df['delta_ncol']) + (fund_df['delta_fina'] - fund_df['delta_finl'])) / (0.5*fund_df['at'] + 0.5*fund_df['at_lag'])
        
    else:
        fund_df = get_fundq(db, fund_list=["actq", "atq", "cheq", "lctq", "dlcq", "ivaoq", "ltq", "dlttq", "ivstq", "pstklq"]) # actq: current assets, atq: total assets, cheq: cash and cash equivalents, lctq: current liabilities, dlcq: short-term debt, ivaoq: investments and advances, ltq: total liabilities, dlttq: long-term debt, ivstq: short-term investments, pstklq: preferred stock

        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged values
        fund_df['actq_ltm'] = fund_df.groupby('gvkey')['actq'].transform(lambda x: x.rolling(4).mean())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(4).mean())
        fund_df['cheq_ltm'] = fund_df.groupby('gvkey')['cheq'].transform(lambda x: x.rolling(4).mean())
        fund_df['lctq_ltm'] = fund_df.groupby('gvkey')['lctq'].transform(lambda x: x.rolling(4).mean())
        fund_df['dlcq_ltm'] = fund_df.groupby('gvkey')['dlcq'].transform(lambda x: x.rolling(4).mean())
        fund_df['ivaoq_ltm'] = fund_df.groupby('gvkey')['ivaoq'].transform(lambda x: x.rolling(4).mean())
        fund_df['ltq_ltm'] = fund_df.groupby('gvkey')['ltq'].transform(lambda x: x.rolling(4).mean())
        fund_df['dlttq_ltm'] = fund_df.groupby('gvkey')['dlttq'].transform(lambda x: x.rolling(4).mean())
        fund_df['ivstq_ltm'] = fund_df.groupby('gvkey')['ivstq'].transform(lambda x: x.rolling(4).mean())
        fund_df['pstklq_ltm'] = fund_df.groupby('gvkey')['pstklq'].transform(lambda x: x.rolling(4).mean())

        fund_df['actq_lag'] = fund_df.groupby('gvkey')['actq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['cheq_lag'] = fund_df.groupby('gvkey')['cheq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['lctq_lag'] = fund_df.groupby('gvkey')['lctq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['dlcq_lag'] = fund_df.groupby('gvkey')['dlcq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['ivaoq_lag'] = fund_df.groupby('gvkey')['ivaoq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['ltq_lag'] = fund_df.groupby('gvkey')['ltq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['dlttq_lag'] = fund_df.groupby('gvkey')['dlttq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['ivstq_lag'] = fund_df.groupby('gvkey')['ivstq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['pstklq_lag'] = fund_df.groupby('gvkey')['pstklq'].transform(lambda x: x.shift(4).rolling(4).mean())

        # Calculate deltas
        fund_df['delta_coaq'] = (fund_df['actq_ltm'] - fund_df['cheq_ltm']) - (fund_df['actq_lag'] - fund_df['cheq_lag'])
        fund_df['delta_colq'] = (fund_df['lctq_ltm'] - fund_df['dlcq_ltm']) - (fund_df['lctq_lag'] - fund_df['dlcq_lag'])

        fund_df['delta_ncoaq'] = (fund_df['atq_ltm'] - fund_df['actq_ltm'] - fund_df['ivaoq_ltm']) - (fund_df['atq_lag'] - fund_df['actq_lag'] - fund_df['ivaoq_lag'])
        fund_df['delta_ncolq'] = (fund_df['ltq_ltm'] - fund_df['lctq_ltm'] - fund_df['dlttq_ltm']) - (fund_df['ltq_lag'] - fund_df['lctq_lag'] - fund_df['dlttq_lag'])

        fund_df['delta_finaq'] = (fund_df['ivstq_ltm'] + fund_df['ivaoq_ltm']) - (fund_df['ivstq_lag'] + fund_df['ivaoq_lag'])
        fund_df['delta_finlq'] = (fund_df['dlttq_ltm'] + fund_df['dlcq_ltm'] + fund_df['pstklq_ltm']) - (fund_df['dlttq_lag'] + fund_df['dlcq_lag'] + fund_df['pstklq_lag'])

        # Calculate operating accruals
        fund_df[name] = ((fund_df['delta_coaq'] - fund_df['delta_colq']) + (fund_df['delta_ncoaq'] - fund_df['delta_ncolq']) + (fund_df['delta_finaq'] - fund_df['delta_finlq'])) / (0.5*fund_df['atq_ltm'] + 0.5*fund_df['atq_lag'])
    
    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return


def net_external_finance(db, annual=False, verbose=False, name='f_nef'):
    if annual:
        fund_df = get_funda(db, fund_list=["sstk", "at", "prstkc", "dvc", "dltis", "dltr", "dlcch"]) # sstk: sale of common and preferred stocks, at: total assets, prstkc: purchase of common and preferred stocks, dvc: cash dividends, dltis: cash inflow issuance long-term debt, dltr: cash outflow reduction long-term debt, dlcch: change in current debt
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged values
        fund_df['sstk_lag'] = fund_df.groupby('gvkey')['sstk'].shift(1)
        fund_df['prstkc_lag'] = fund_df.groupby('gvkey')['prstkc'].shift(1)
        fund_df['dvc_lag'] = fund_df.groupby('gvkey')['dvc'].shift(1)
        fund_df['dltis_lag'] = fund_df.groupby('gvkey')['dltis'].shift(1)
        fund_df['dltr_lag'] = fund_df.groupby('gvkey')['dltr'].shift(1)
        fund_df['dlcch_lag'] = fund_df.groupby('gvkey')['dlcch'].shift(1)
        fund_df['at_lag'] = fund_df.groupby('gvkey')['at'].shift(1)

        # Calculate deltas
        fund_df['delta_equity'] = (fund_df['sstk'] - fund_df['prstkc'] - fund_df['dvc']) - (fund_df['sstk_lag'] - fund_df['prstkc_lag'] - fund_df['dvc_lag'])
        fund_df['delta_debt'] = (fund_df['dltis'] - fund_df['dltr'] - fund_df['dlcch']) - (fund_df['dltis_lag'] - fund_df['dltr_lag'] - fund_df['dlcch_lag'])

        # Calculate net external finance
        fund_df[name] = (fund_df['delta_equity'] + fund_df['delta_debt']) / (0.5*fund_df["at"] + 0.5*fund_df["at_lag"])
        
    else:
        fund_df = get_fundq(db, fund_list=["sstkq", "atq", "prstkcq", "dvcq", "dltisq", "dltrq", "dlcchq"]) # sstk: sale of common and preferred stocks, at: total assets, prstkc: purchase of common and preferred stocks, dvc: cash dividends, dltis: cash inflow issuance long-term debt, dltr: cash outflow reduction long-term debt, dlcch: change in current debt
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged values
        fund_df['sstkq_ltm'] = fund_df.groupby('gvkey')['sstkq'].transform(lambda x: x.rolling(4).sum())
        fund_df['prstkcq_ltm'] = fund_df.groupby('gvkey')['prstkcq'].transform(lambda x: x.rolling(4).sum())
        fund_df['dvcq_ltm'] = fund_df.groupby('gvkey')['dvcq'].transform(lambda x: x.rolling(4).sum())
        fund_df['dltisq_ltm'] = fund_df.groupby('gvkey')['dltisq'].transform(lambda x: x.rolling(4).sum())
        fund_df['dltrq_ltm'] = fund_df.groupby('gvkey')['dltrq'].transform(lambda x: x.rolling(4).sum())
        fund_df['dlcchq_ltm'] = fund_df.groupby('gvkey')['dlcchq'].transform(lambda x: x.rolling(4).sum())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(4).mean())

        fund_df['sstkq_lag'] = fund_df.groupby('gvkey')['sstkq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['prstkcq_lag'] = fund_df.groupby('gvkey')['prstkcq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['dvcq_lag'] = fund_df.groupby('gvkey')['dvcq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['dltisq_lag'] = fund_df.groupby('gvkey')['dltisq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['dltrq_lag'] = fund_df.groupby('gvkey')['dltrq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['dlcchq_lag'] = fund_df.groupby('gvkey')['dlcchq'].transform(lambda x: x.shift(4).rolling(4).sum())
        fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4).rolling(4).mean())

        # Calculate deltas
        fund_df['delta_equityq'] = (fund_df['sstkq_ltm'] - fund_df['prstkcq_ltm'] - fund_df['dvcq_ltm']) - (fund_df['sstkq_lag'] - fund_df['prstkcq_lag'] - fund_df['dvcq_lag'])
        fund_df['delta_debtq'] = (fund_df['dltisq_ltm'] - fund_df['dltrq_ltm'] - fund_df['dlcchq_ltm']) - (fund_df['dltisq_lag'] - fund_df['dltrq_lag'] - fund_df['dlcchq_lag'])

        # Calculate net external finance
        fund_df[name] = (fund_df['delta_equityq'] + fund_df['delta_debtq']) / (0.5*fund_df["atq_ltm"] + 0.5*fund_df["atq_lag"])

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return




def return_net_operating_assets(db, annual=False, verbose=False, name='f_rnoa'):
    if annual:
        fund_df = get_funda(db, fund_list=["oiadp", "at", "che", "ivao", "dltt", "dlc", "ceq", "pstk", "mib"]) # oiadp: operating income before interest, at: total assets, che: cash, ivao: short-term investments, dltt: long-term debt, dlc: short-term debt, ceq: common equity, pstk: preferred equity, mib: minority interest
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get lagged values
        fund_df['che_lag'] = fund_df.groupby('gvkey')['che'].shift(1)
        fund_df['ivao_lag'] = fund_df.groupby('gvkey')['ivao'].shift(1)
        fund_df['at_lag'] = fund_df.groupby('gvkey')['at'].shift(1)
        fund_df['dltt_lag'] = fund_df.groupby('gvkey')['dltt'].shift(1)
        fund_df['dlc_lag'] = fund_df.groupby('gvkey')['dlc'].shift(1)
        fund_df['ceq_lag'] = fund_df.groupby('gvkey')['ceq'].shift(1)
        fund_df['pstk_lag'] = fund_df.groupby('gvkey')['pstk'].shift(1)
        fund_df['mib_lag'] = fund_df.groupby('gvkey')['mib'].shift(1)

        # Calculate net operating assets
        fund_df["noa"] = (fund_df["at"] - fund_df["che"] - fund_df["ivao"]) - (fund_df["at"] - fund_df["dltt"] - fund_df["dlc"] - fund_df["ceq"] - fund_df["pstk"] - fund_df["mib"])
        fund_df["noa_lag"] = (fund_df["at_lag"] - fund_df["che_lag"] - fund_df["ivao_lag"]) - (fund_df["at_lag"] - fund_df["dltt_lag"] - fund_df["dlc_lag"] - fund_df["ceq_lag"] - fund_df["pstk_lag"] - fund_df["mib_lag"])
       
        # Calculate return on net operating assets
        fund_df[name] = fund_df["oiadp"] / (0.5* fund_df["noa"] + 0.5*fund_df["noa_lag"])
        
    else:
        fund_df = get_fundq(db, fund_list=["oiadpq", "atq", "cheq", "ivaoq", "dlttq", "dlcq", "ceqq", "pstkq", "mibq"]) # oiadp: operating income before interest, at: total assets, che: cash, ivao: short-term investments, dltt: long-term debt, dlc: short-term debt, ceq: common equity, pstk: preferred equity, mib: minority interest
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        # Get current and lagged values
        fund_df['oiadpq_ltm'] = fund_df.groupby('gvkey')['oiadpq'].transform(lambda x: x.rolling(4).mean())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(4).mean())
        fund_df['cheq_ltm'] = fund_df.groupby('gvkey')['cheq'].transform(lambda x: x.rolling(4).mean())
        fund_df['ivaoq_ltm'] = fund_df.groupby('gvkey')['ivaoq'].transform(lambda x: x.rolling(4).mean())
        fund_df['dlttq_ltm'] = fund_df.groupby('gvkey')['dlttq'].transform(lambda x: x.rolling(4).mean())
        fund_df['dlcq_ltm'] = fund_df.groupby('gvkey')['dlcq'].transform(lambda x: x.rolling(4).mean())
        fund_df['ceqq_ltm'] = fund_df.groupby('gvkey')['ceqq'].transform(lambda x: x.rolling(4).mean())
        fund_df['pstkq_ltm'] = fund_df.groupby('gvkey')['pstkq'].transform(lambda x: x.rolling(4).mean())
        fund_df['mibq_ltm'] = fund_df.groupby('gvkey')['mibq'].transform(lambda x: x.rolling(4).mean())

        fund_df['cheq_lag'] = fund_df.groupby('gvkey')['cheq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['ivaoq_lag'] = fund_df.groupby('gvkey')['ivaoq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['atq_lag'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['dlttq_lag'] = fund_df.groupby('gvkey')['dlttq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['dlcq_lag'] = fund_df.groupby('gvkey')['dlcq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['ceqq_lag'] = fund_df.groupby('gvkey')['ceqq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['pstkq_lag'] = fund_df.groupby('gvkey')['pstkq'].transform(lambda x: x.shift(4).rolling(4).mean())
        fund_df['mibq_lag'] = fund_df.groupby('gvkey')['mibq'].transform(lambda x: x.shift(4).rolling(4).mean())

        # Calculate net operating assets
        fund_df["noaq"] = (fund_df["atq_ltm"] - fund_df["cheq_ltm"] - fund_df["ivaoq_ltm"]) - (fund_df["atq_ltm"] - fund_df["dlttq_ltm"] - fund_df["dlcq_ltm"] - fund_df["ceqq_ltm"] - fund_df["pstkq_ltm"] - fund_df["mibq_ltm"])
        fund_df["noaq_lag"] = (fund_df["atq_lag"] - fund_df["cheq_lag"] - fund_df["ivaoq_lag"]) - (fund_df["atq_lag"] - fund_df["dlttq_lag"] - fund_df["dlcq_lag"] - fund_df["ceqq_lag"] - fund_df["pstkq_lag"] - fund_df["mibq_lag"])
       
        # Calculate return on net operating assets
        fund_df[name] = fund_df["oiadpq_ltm"] / (0.5* fund_df["noaq"] + 0.5*fund_df["noaq_lag"])

    if verbose:
        print(fund_df.query("gvkey == '001690'"))
    save_file(fund_df, name)
    return
