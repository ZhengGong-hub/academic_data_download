import pandas as pd

def crsp_clean(crsp_df):
    """
    Clean the CRSP daily data.
    """
    crsp_df['date'] = pd.to_datetime(crsp_df['date'])
    crsp_df.sort_values(by=['date'], inplace=True, ascending=True)
    crsp_df['shrout'] = crsp_df['shrout'] / 1000 # change from thousands to millions
    return crsp_df