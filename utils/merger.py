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