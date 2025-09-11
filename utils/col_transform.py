import pandas as pd

def rolling_sum(df, col):
    return round(df.groupby('gvkey')[col].transform(lambda x: x.rolling(window=4, min_periods=4).sum()), 2)


def fill_forward(df, col):
    return df.groupby('gvkey')[col].transform(lambda x: x.fillna(method='ffill', limit=4))


def fillna_with_0(df, col):
    return df[col].fillna(0)


def shift_n_rows(df, col, row):
    return df.groupby('gvkey')[col].transform(lambda x: x.shift(row))


def merge_mktcap_fundq(mktcap_df, fund_df):
    return pd.merge_asof(mktcap_df, fund_df.dropna(subset=['rdq']), left_on=['date'], right_on=['rdq'], by=['gvkey'], direction='backward')


def merge_funda_rdq(funda_df, fundq_df):
    merged = pd.merge(funda_df, fundq_df, on=['gvkey', 'datadate'], how='left').drop_duplicates(subset=['gvkey', 'datadate'])
    merged.sort_values(by=['rdq', 'gvkey'], inplace=True)
    return merged


def merge_funda_fundq(fundq_df, funda_df):
    """
    Merge funda and fundq data frames.
    merge fundq first, then merge funda
    so that the dataframe output will be quarterly data
    """
    fundq_df.sort_values(by=['datadate', 'gvkey'], inplace=True)
    funda_df.sort_values(by=['datadate', 'gvkey'], inplace=True)
    merged = pd.merge_asof(fundq_df, funda_df, on=['datadate'], by=['gvkey'], direction='backward')
    merged.sort_values(by=['datadate', 'gvkey'], inplace=True)
    return merged