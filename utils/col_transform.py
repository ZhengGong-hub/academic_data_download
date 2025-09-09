import pandas as pd

def rolling_sum(df, col):
    return round(df.groupby('gvkey')[col].transform(lambda x: x.rolling(window=4, min_periods=4).sum()), 2)


def fill_forward(df, col):
    return df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill', limit=4))


def fillna_with_0(df, col):
    return df[col].fillna(0)


def merge_mktcap_fundq(mktcap_df, fund_df):
    return pd.merge_asof(mktcap_df, fund_df, left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')
