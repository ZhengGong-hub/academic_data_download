from db_manager.wrds_sql import get_fundq, get_funda
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
