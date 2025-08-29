def get_fundq(db, fund_list, start_year=2000):
    # the columns is a list, we need to get to the formaat of f.column1, f.column2, ...
    fund_list_sql = ", ".join([f"f.{col}" for col in fund_list])
    print(fund_list_sql)
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
        ORDER BY f.datadate ASC
    """

    return db.raw_sql(sql)

def get_funda(db, fund_list, start_year=2000):
    # the columns is a list, we need to get to the formaat of f.column1, f.column2, ...
    fund_list_sql = ", ".join([f"f.{col}" for col in fund_list])
    print(fund_list_sql)
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
        ORDER BY f.datadate ASC
    """

    return db.raw_sql(sql)