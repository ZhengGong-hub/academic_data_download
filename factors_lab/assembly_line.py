import pandas as pd

from db_manager.wrds_sql import get_fundq, get_funda, marketcap_calculator
from utils.save_file import save_file
from utils.sneak_peek import sneak_peek

def gross_profit_to_assets(db,gvkey_list=None, annual=False, verbose=False, name='f_gpta'):
    if annual:
        fund_df = get_funda(db, gvkey_list=gvkey_list, fund_list=["revt", "cogs", "at"]) # revt: revenue, cogsq: cost of goods sold, atq: total assets
        fund_df[name] = (fund_df['revt'] - fund_df['cogs']) / fund_df['at']
    else:
        fund_df = get_fundq(db, gvkey_list=gvkey_list, fund_list=["revtq", "cogsq", "atq"]) # revtq: revenue, cogsq: cost of goods sold, atq: total assets
        if verbose:
            print("peeks at the data right after getting from wrds")
            sneak_peek(fund_df)
        
        # group by gvkey and rolling sum of saleq for the last 4 quarters
        fund_df['revtq_ltm'] = fund_df.groupby('gvkey')['revtq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['cogsq_ltm'] = fund_df.groupby('gvkey')['cogsq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(window=4).mean())
        
        # calculate gross profit to assets
        fund_df[name] = (fund_df['revtq_ltm'] - fund_df['cogsq_ltm']) / fund_df['atq_ltm']

    # peek at the data    
    if verbose:
        print("peeks at the data after calculation!")
        sneak_peek(fund_df)

    if gvkey_list is None:
        save_file(fund_df, name) # only save the file if gvkey_list is None (meaning select all)
    return f'Done with {name}'

def sales_to_price(db, gvkey_list=None, annual=False, verbose=False, name='f_sp'):
    if not annual:
        fund_df = get_fundq(db, gvkey_list=gvkey_list, fund_list=["saleq"]) # saleq: sales
        if verbose:
            print("peeks at the data right after getting from wrds")
            sneak_peek(fund_df)

        fund_df['saleq_ltm'] = round(fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum()), 2)

        price_df = marketcap_calculator(db, gvkey_list=gvkey_list)

        # merge the fund_df and price_df
        price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
        price_df[name] = price_df['saleq_ltm'] / price_df['marketcap']

        if verbose:
            print("peeks at the data after calculation!")
            sneak_peek(price_df)

        if gvkey_list is None:
            save_file(price_df, name) # only save the file if gvkey_list is None (meaning select all)
    return f'Done with {name}'