from db_manager.wrds_sql import get_fundq, get_funda, get_crsp_daily, permco_gvkey_link
from utils.save_file import save_file

def gross_profit_to_assets(db, annual=True, verbose=False, name='f_gpta'):
    if annual:
        fund_df = get_funda(db, fund_list=["sale", "cogs", "at"]) # saleq: sales, cogsq: cost of goods sold, atq: total assets
        if verbose:
            print(fund_df)

        fund_df[name] = (fund_df['sale'] - fund_df['cogs']) / fund_df['at']
        
    else:
        fund_df = get_fundq(db, fund_list=["saleq", "cogsq", "atq"]) # saleq: sales, cogsq: cost of goods sold, atq: total assets
        if verbose:
            print(fund_df)
        
        # group by gvkey and rolling sum of saleq for the last 4 quarters
        fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['cogsq_ltm'] = fund_df.groupby('gvkey')['cogsq'].transform(lambda x: x.rolling(window=4).sum())
        fund_df['atq_ltm'] = fund_df.groupby('gvkey')['atq'].transform(lambda x: x.rolling(window=4).mean())
        
        # calculate gross profit to assets
        fund_df[name] = (fund_df['saleq_ltm'] - fund_df['cogsq_ltm']) / fund_df['atq_ltm']

    # peek at the data    
    if verbose:
        print(fund_df.query("gvkey == '001690'"))

    # save the file 
    save_file(fund_df, name)

    print(f"Saved gross_profit_to_assets {name} to data/factors/{name}.csv")
    return fund_df

def sales_to_price(db, annual=False, verbose=False, name='f_sp'):
    if not annual:
        fund_df = get_fundq(db, fund_list=["saleq"]) # saleq: sales
        if verbose:
            print(fund_df)

        # fund_df['saleq_ltm'] = fund_df.groupby('gvkey')['saleq'].transform(lambda x: x.rolling(window=4).sum())

        price_df = get_crsp_daily(db)

        # check whether for a gvkey and a date there are multiple permco
        print(price_df.groupby(['gvkey', 'date'])['permco'].nunique().sort_values(ascending=False).reset_index().query("permco>1").drop_duplicates(subset=['gvkey'], keep='first'))
        print(price_df.query("gvkey == '029830' and date == '2002-08-15'"))

        # print(price_df)
        assert False