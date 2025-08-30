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

    print(f"Saved gross_profit_to_assets {name} to data/factors/{name}.csv")
    return

def sales_to_price(db, annual=False, verbose=False, name='f_sp'):
    if not annual:
        fund_df = get_fundq(db, fund_list=["saleq"]) # saleq: sales
        if verbose:
            print(fund_df.query("gvkey == '001690'"))

        fund_df['saleq_ltm'] = round(fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum()), 2)

        price_df = marketcap_calculator(db)

        # merge the fund_df and price_df
        price_df = pd.merge_asof(price_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
        price_df[name] = price_df['saleq_ltm'] / price_df['marketcap']

        if verbose:
            print(price_df.query("gvkey == '001690'"))

        save_file(price_df, name)
    
    print(f"Saved sales_to_price {name} to data/factors/{name}.csv")
    return