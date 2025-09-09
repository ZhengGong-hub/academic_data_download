import pandas as pd

def merge_link_table_crsp(link_df, crsp_df):
    """
    Merge the link table with the CRSP daily data.
    """
    merged_df = crsp_df.merge(link_df, on=['permco', 'permno'], how='left')
    merged_df['linkdt'] = pd.to_datetime(merged_df['linkdt'])
    merged_df['linkenddt'] = pd.to_datetime(merged_df['linkenddt'])
    merged_df = merged_df.query("date < linkenddt and date > linkdt")
    return merged_df

def merge_link_table_msp500list(link_df, msp500list_df):
    """
    Merge the link table with the MSP500 list.
    """
    merged_df = msp500list_df.merge(link_df, on=['permno'], how='left')
    merged_df['linkdt'] = pd.to_datetime(merged_df['linkdt'])
    merged_df['linkenddt'] = pd.to_datetime(merged_df['linkenddt'])
    merged_df['start'] = pd.to_datetime(merged_df['start'])
    merged_df['ending'] = pd.to_datetime(merged_df['ending'])
    merged_df = merged_df.query("start > linkdt and ending < linkenddt")
    return merged_df