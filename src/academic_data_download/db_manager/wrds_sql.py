import os
import pandas as pd
import tqdm
import numpy as np
import glob
from academic_data_download.utils.merger import merge_link_table_crsp, merge_link_table_msp500list
from academic_data_download.utils.clean import crsp_clean
from academic_data_download.utils.sneak_peek import sneak_peek

def get_fundq(db, fund_list, verbose=False, gvkey_list=None, start_year=2000):
    """
    Get quarterly fundamental data from Compustat FUNDQ.

    Parameters
    ----------
    db : database connection
    fund_list : list of str
        Columns to retrieve (e.g., ['saleq', 'cogsq', 'atq']).
    verbose : bool, optional
        Whether to print the SQL query.
    start_year : int, optional
        Earliest fiscal year (default 2000).
    gvkey_list : list of str, optional
    Returns
    -------
    pandas.DataFrame
        Quarterly data with requested columns.
    """
    fund_list_sql = ", ".join([f"f.{col}" for col in fund_list])
    if verbose: 
        print(fund_list_sql)
    # this is a sample gvkey_list: ['001690', '002176']
    if gvkey_list is not None:
        gvkey_list_sql = "AND f.gvkey IN ("+", ".join([f"'{col}'" for col in gvkey_list])+")"
    else:
        gvkey_list_sql = ""
    if verbose:
        print(gvkey_list_sql)
    sql = f"""
        SELECT
            f.gvkey,
            f.datadate,
            f.fyearq,
            f.fqtr,
            f.rdq, -- report date
            {fund_list_sql}
        FROM comp.fundq f
        WHERE f.indfmt = 'INDL' -- industrial format (excluding financial companies, but financial services companies ok)
        AND f.datafmt = 'STD' -- standard format
        AND f.consol  = 'C' -- consolidated financials (parents + subsidiaries, standard)
        AND f.popsrc  = 'D' -- domestic companies only
        AND f.curncdq = 'USD' -- US dollars as native currency of reporting only
        AND f.fyearq >= {start_year}
        AND f.rdq IS NOT NULL
        AND f.datadate IS NOT NULL
        {gvkey_list_sql}
        ORDER BY f.rdq ASC
    """

    df = db.raw_sql(sql)
    df['datadate'] = pd.to_datetime(df['datadate'])
    df['rdq'] = pd.to_datetime(df['rdq'])

    # the fund list value should have precision of 2
    for col in fund_list:
        df[col] = df[col].astype(float).round(2)

    if verbose:
        print("peeks at the data right after getting from wrds")
        sneak_peek(df)
    return df

def get_funda(db, fund_list, start_year=2000, gvkey_list=None, verbose=False):
    """
    Get annual fundamental data from Compustat FUNDA.

    Parameters
    ----------
    db : database connection
    fund_list : list of str
        Columns to retrieve (e.g., ['sale', 'cogs', 'at']).
    start_year : int, optional
        Earliest fiscal year (default 2000).
    gvkey_list : list of str, optional
    verbose : bool, optional
        Whether to print the SQL query.
    Returns
    -------
    pandas.DataFrame
        Annual data with requested columns.
    """
    fund_list_sql = ", ".join([f"f.{col}" for col in fund_list])
    # this is a sample gvkey_list: ['001690', '002176']
    if gvkey_list is not None:
        gvkey_list_sql = "AND f.gvkey IN ("+", ".join([f"'{col}'" for col in gvkey_list])+")"
    else:
        gvkey_list_sql = ""
    if verbose:
        print(gvkey_list_sql)
    sql = f"""
        SELECT
            f.gvkey,
            f.datadate,
            f.fyear,
            {fund_list_sql}
        FROM comp.funda f
        WHERE f.indfmt = 'INDL' -- industrial format (excluding financial companies, but financial services companies ok)
        AND f.datafmt = 'STD' -- standard format
        AND f.consol  = 'C' -- consolidated financials (parents + subsidiaries, standard)
        AND f.popsrc  = 'D' -- domestic companies only
        AND f.curncd = 'USD' -- US dollars as native currency of reporting only
        AND f.fyear >= {start_year}
        AND f.datadate IS NOT NULL
        {gvkey_list_sql}
        ORDER BY f.datadate ASC
    """

    df = db.raw_sql(sql)
    df['datadate'] = pd.to_datetime(df['datadate'])
    # the fund list value should have precision of 2
    for col in fund_list:
        df[col] = df[col].astype(float).round(2)
    return df

def get_crsp_daily(db, start_date='2000-01-01'):
    """
    Retrieve daily CRSP stock data (price, return, volume, shares, adjustment factors) for all available PERMCOs,
    with optional local caching to avoid repeated expensive SQL queries.

    This function will:
      - Check for a local Parquet cache of the full CRSP daily dataset.
      - If the cache exists, load and return it.
      - If not, query the database in manageable chunks (by PERMCO), save each chunk to disk,
        merge all chunks, cache the result, and return the full DataFrame.

    Parameters
    ----------
    db : object
        Database connection object with a .raw_sql() method for executing SQL queries.
    start_date : str, optional
        Earliest date to retrieve (format: 'YYYY-MM-DD'). Default is '2000-01-01'.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing daily CRSP data with columns:
        ['permco', 'permno', 'date', 'prc', 'ret', 'vol', 'shrout', 'cfacpr', 'cfacshr']

    Notes
    -----
    - The function uses a local Parquet file at 'data/crsp/crsp_daily.parquet' as a cache.
    - If the cache is missing, the function will:
        * Create necessary directories.
        * Retrieve the list of unique PERMCOs from the Compustat-CRSP link table.
        * Split the PERMCOs into 10 chunks to avoid overwhelming the database.
        * For each chunk, query the CRSP DSF table for all dates >= start_date.
        * Save each chunk as a separate Parquet file in 'data/crsp/parts/'.
        * Concatenate all chunk files into a single DataFrame, save to the cache, and return.
    - This process may take a long time and require significant disk space.
    """
    # Retrieve unique PERMCOs from the Compustat-CRSP link table
    link_df = permco_gvkey_link(db)
    permco_list = link_df['permco'].dropna().unique()

    cache_path = "data/crsp/crsp_daily.parquet"
    if os.path.exists(cache_path):
        print("Cache file for CRSP daily data found. Loading from disk...")
        return merge_link_table_crsp(link_df, crsp_clean(pd.read_parquet(cache_path)))

    print("Cache file for CRSP daily data not found. Starting SQL queries. This may take a while...")

    # Ensure required directories exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/crsp', exist_ok=True)
    os.makedirs('data/crsp/parts', exist_ok=True)

    # Split PERMCOs into 10 roughly equal chunks for manageable queries
    permco_chunks = np.array_split(permco_list, 10)

    for idx, permco_chunk in enumerate(tqdm.tqdm(permco_chunks, desc="Downloading CRSP daily chunks")):
        # Build SQL query for this chunk
        permco_str = ', '.join(str(permco) for permco in permco_chunk)
        sql = f"""
            SELECT
                a.permco,        
                a.permno,
                a.date,
                a.prc,
                a.ret,
                a.vol,
                a.shrout,
                a.cfacpr,
                a.cfacshr
            FROM crsp.dsf a
            WHERE 
                a.permco IN ({permco_str})
                AND a.date >= '{start_date}'
            ORDER BY a.date;
        """

        # Execute query and save result to a Parquet part file
        df = db.raw_sql(sql)
        part_path = f'data/crsp/parts/crsp_daily_{idx}.parquet'
        df.to_parquet(part_path, index=False)

    # Merge all part files into a single DataFrame
    print("Merging all CRSP daily part files into a single DataFrame...")
    price_df_agg = pd.concat([pd.read_parquet(f) for f in glob.glob('data/crsp/parts/crsp_daily_*.parquet')], ignore_index=True)
    price_df_agg.to_parquet(cache_path, index=False)
    print(f"CRSP daily data SQL query complete. Data cached at {cache_path}")

    # merge link table with price_df_agg
    link_df = permco_gvkey_link(db)
    return merge_link_table_crsp(link_df, crsp_clean(price_df_agg))


def get_crsp_daily_by_permno_by_year(db, permno_list=None, year=2020):
    """
    Retrieve daily CRSP stock data (price, return, volume, shares, adjustment factors) for all available PERMCOs,
    with optional local caching to avoid repeated expensive SQL queries.

    This function will:
      - Check for a local Parquet cache of the full CRSP daily dataset.
      - If the cache exists, load and return it.
      - If not, query the database in manageable chunks (by PERMCO), save each chunk to disk,
        merge all chunks, cache the result, and return the full DataFrame.

    Parameters
    ----------
    db : object
        Database connection object with a .raw_sql() method for executing SQL queries.
    permno_list : list of int, optional
        List of PERMNOs to retrieve. If None, all PERMNOs will be retrieved.
    year : int, optional
        Earliest date to retrieve (format: 'YYYY-MM-DD'). Default is '2000-01-01'.
        if 'all', will retrieve all data.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing daily CRSP data with columns:
        ['permco', 'permno', 'date', 'prc', 'ret', 'vol', 'shrout', 'cfacpr', 'cfacshr']
    """
    # Ensure required directories exist
    os.makedirs('data', exist_ok=True)
    os.makedirs('data/crsp', exist_ok=True)

    if year == 'all':
        year_condition = ""
    else:
        year_condition = f"AND a.date >= '{year}-01-01' AND a.date <= '{year}-12-31'"

    # Build SQL query for this chunk
    permno_str = ', '.join(str(permno) for permno in permno_list)
    sql = f"""
        SELECT
            a.permco,        
            a.permno,
            a.date,
            a.prc,
            a.ret,
            a.vol,
            a.shrout,
            a.cfacpr,
            a.cfacshr,
            a.hsiccd,
            a.openprc
        FROM crsp.dsf a
        WHERE 
            a.permno IN ({permno_str})
            {year_condition}
        ORDER BY a.date;
    """

    # Execute query and save result to a Parquet part file
    df = db.raw_sql(sql)
    return df

def permco_gvkey_link(db):
    """
    Get table mapping Compustat GVKEYs to CRSP PERMCOs and PERMNOs.

    Parameters
    ----------
    db : database connection

    Returns
    -------
    pandas.DataFrame
        Link table with GVKEY, PERMNO, PERMCO, etc.
    """
    sql = """
    SELECT gvkey, liid, linkdt, COALESCE(linkenddt, '2059-12-31') as linkenddt, lpermno, lpermco, linkprim
    FROM crsp.ccmxpf_linktable
    WHERE linktype IN ('LU', 'LC')   -- standard links (LU = link to common, LC = link to company)
      AND linkprim IN ('P', 'J', 'C')     -- primary links (P = primary, C = company)
      AND lpermno IS NOT NULL   
    """
    
    link_df = db.raw_sql(sql).rename(columns={'lpermno': 'permno', 'lpermco': 'permco'})
    link_df['permno'] = link_df['permno'].astype(int)
    link_df['permco'] = link_df['permco'].astype(int)
    return link_df


def marketcap_calculator(db, gvkey_list=None, verbose=False):
    """
    Calculate market cap from CRSP daily data.
    """
    os.makedirs('data/crsp', exist_ok=True)
    if not os.path.exists(f'data/crsp/marketcap.parquet'):
        crsp_df = get_crsp_daily(db)
        crsp_df['marketcap_permno'] = crsp_df['prc'] * crsp_df['shrout']

        # sum across different permno for each permco (account for different share class)
        crsp_df = crsp_df.groupby(['date','permco', 'gvkey']).agg({'marketcap_permno': 'sum'}).reset_index()
        crsp_df.rename(columns={'marketcap_permno': 'marketcap'}, inplace=True)
        
        # round to integer
        crsp_df['marketcap'] = crsp_df['marketcap'].astype(int)
        crsp_df['gvkey'] = crsp_df['gvkey'].astype("string")
        crsp_df.to_parquet(f'data/crsp/marketcap.parquet', index=False)
    else:
        crsp_df = pd.read_parquet(f'data/crsp/marketcap.parquet')
        crsp_df['gvkey'] = crsp_df['gvkey'].astype("string")

    # only keep the gvkey in the gvkey_list
    if gvkey_list is not None:
        crsp_df = crsp_df[crsp_df['gvkey'].isin(gvkey_list)]
        
    if verbose:
        sneak_peek(crsp_df)
    return crsp_df


def get_sp500_constituents(db):
    """
    Get SP500 list from CRSP. with link table and gic sector.
    """
    sql = """
    SELECT a.*
    , link_df.gvkey
    , link_df.linkdt
    , link_df.linkenddt
    , gic_sector_df.gsector
    , gic_sector_df.indfrom
    , gic_sector_df.indthru

    FROM crsp.msp500list AS a

    JOIN (
        SELECT gvkey,
            liid,
            linkdt,
            COALESCE(linkenddt, '2059-12-31') AS linkenddt,
            lpermno as permno,
            lpermco as permco,
            linkprim
        FROM crsp.ccmxpf_linktable
        WHERE linktype IN ('LU', 'LC')         -- standard links
        AND linkprim IN ('P', 'J', 'C')      -- primary links
        AND lpermno IS NOT NULL
    ) AS link_df
    ON a.permno = link_df.permno

    JOIN (
        select co_hgic.gsector
        , co_hgic.gvkey
        , co_hgic.indfrom
        , COALESCE(co_hgic.indthru, '2059-12-31') AS indthru
        from comp.co_hgic as co_hgic
        where indtype = 'GICS'
    ) AS gic_sector_df
    ON link_df.gvkey = gic_sector_df.gvkey

    WHERE 
    ending < linkenddt + INTERVAL '1 year';
    """
    return db.raw_sql(sql)


def get_sp500_constituents_snapshot(db, year):
    """
    Get SP500 list from CRSP. with link table and gic sector. at a given year.
    """
    sql = f"""
    SELECT a.*
    , link_df.gvkey
    , link_df.linkdt
    , link_df.linkenddt
    , gic_sector_df.gsector
    , gic_sector_df.indfrom
    , gic_sector_df.indthru

    FROM crsp.msp500list AS a

    JOIN (
        SELECT gvkey,
            liid,
            linkdt,
            COALESCE(linkenddt, '2059-12-31') AS linkenddt,
            lpermno as permno,
            lpermco as permco,
            linkprim
        FROM crsp.ccmxpf_linktable
        WHERE linktype IN ('LU', 'LC')         -- standard links
        AND linkprim IN ('P', 'J', 'C')      -- primary links
        AND lpermno IS NOT NULL
    ) AS link_df
    ON a.permno = link_df.permno

    JOIN (
        select co_hgic.gsector
        , co_hgic.gvkey
        , co_hgic.indfrom
        , COALESCE(co_hgic.indthru, '2059-12-31') AS indthru
        from comp.co_hgic as co_hgic
        where indtype = 'GICS'
    ) AS gic_sector_df
    ON link_df.gvkey = gic_sector_df.gvkey

    WHERE 
    ending < linkenddt + INTERVAL '1 year'
    AND
    ending > '{year}-01-01'
    AND 
    start <= '{year}-01-01'
    AND 
    '{year}-01-01' >= indfrom
    AND
    '{year}-01-01' < indthru
    ;
    """
    return db.raw_sql(sql)


def get_raven_full_equities(db, year=2024, relevance_threshold=75, event_similarity_days_threshold=90):
    sql = f"""
        SELECT
            agg.*,
            map.cusip,
            ds.ticker,
            ds.namedt,
            ds.nameendt,
            ds.permco
        FROM (
            SELECT
                raven.rp_entity_id,
                raven.country_code,
                -- Convert UTC to US/Eastern and assign trading day: post-4:00 pm ET → next day
                CASE
                    WHEN (
                        ((raven.rpa_date_utc + raven.rpa_time_utc) AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern'
                    )::time >= TIME '16:00:00'
                    THEN (
                        ((raven.rpa_date_utc + raven.rpa_time_utc) AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern'
                    )::date + INTERVAL '1 day'
                    ELSE (
                        ((raven.rpa_date_utc + raven.rpa_time_utc) AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern'
                    )::date
                END AS trading_day_et,
                COUNT(raven.event_sentiment_score) AS event_count,
                AVG(raven.event_sentiment_score) AS mean_ess,
                AVG(raven.bmq) AS mean_bmq,
                AVG(raven.bee) AS mean_bee,
                AVG(raven.bam) AS mean_bam,
                AVG(raven.bca) AS mean_bca,
                AVG(raven.css) AS mean_css,
                AVG(raven.ber) AS mean_ber
            FROM rpna.rpa_full_equities_{year} raven
            WHERE raven.entity_type = 'COMP'
                AND raven.country_code = 'US'
                AND raven.event_sentiment_score IS NOT NULL
                AND raven.relevance >= {relevance_threshold}
                AND raven.event_similarity_days >= {event_similarity_days_threshold}
            GROUP BY
                raven.rp_entity_id,
                raven.country_code,
                trading_day_et
        ) agg
        LEFT JOIN rpna.wrds_all_mapping map
            ON agg.rp_entity_id = map.rp_entity_id
        LEFT JOIN crsp.dsenames ds
            ON LEFT(UPPER(REGEXP_REPLACE(map.cusip, '[^A-Z0-9]', '', 'g')), 8) = ds.cusip
            AND agg.trading_day_et >= ds.namedt
            AND agg.trading_day_et <= ds.nameendt
        ORDER BY agg.trading_day_et, ds.ticker;
    """
    return db.raw_sql(sql).dropna(subset=['permco']).drop_duplicates(subset=['trading_day_et', 'permco'])


def get_raven_global_macro(db, year=2024, relevance_threshold=75, event_similarity_days_threshold=90, us=True):

    sql = f"""
        SELECT
            -- Convert UTC to US/Eastern and assign trading day: post-4:00 pm ET → next day
            CASE
                WHEN (
                    ((raven.rpa_date_utc + raven.rpa_time_utc) AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern'
                )::time >= TIME '16:00:00'
                THEN (
                    ((raven.rpa_date_utc + raven.rpa_time_utc) AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern'
                )::date + INTERVAL '1 day'
                ELSE (
                    ((raven.rpa_date_utc + raven.rpa_time_utc) AT TIME ZONE 'UTC') AT TIME ZONE 'US/Eastern'
                )::date
            END AS trading_day_et,
            CASE WHEN raven.country_code = 'US' THEN 'US' ELSE 'RoW' END AS us_bucket,
            COUNT(*)      AS event_count,
            AVG(raven.event_sentiment_score)      AS mean_ess
        FROM rpna.rpa_full_global_macro_{year} AS raven
        WHERE raven.entity_type = 'PLCE'
            AND raven.event_sentiment_score IS NOT NULL
            AND raven.relevance >= {relevance_threshold}
            AND raven.event_similarity_days >= {event_similarity_days_threshold}
        GROUP BY trading_day_et, us_bucket
        ORDER BY trading_day_et, us_bucket;
    """
    return db.raw_sql(sql)
