# Get versions
import sqlalchemy, wrds, pandas as pd
print("sqlalchemy:", sqlalchemy.__version__)
print("wrds:", wrds.__version__)
print("pandas:", pd.__version__)

db = wrds.Connection(wrds_username="zhenggong123", wrds_password="zishak-zighi8-bohCig", use_keyring=False)

security_identifier = db.raw_sql("""
    SELECT *
    FROM comp.security
    where 
    tic in ('TWTR', 'META', 'AAPL', 'NVDA', 'MSFT')
    AND excntry = 'USA'
    limit 10
    
""")

gvkey = list(security_identifier['gvkey'])
print(gvkey)



aapl_bs = db.raw_sql(f"""
    SELECT datadate, gvkey, rdq, apdedateq, pdateq, fdateq, tic
    ,atq, ltq, seqq, ceqq, cheq, actq, lctq, rectq, invtq, ppegtq, dlcq, dlttq, cshopq, prcraq
    FROM comp.fundq
    WHERE 
    gvkey in ({', '.join([f"'{id}'" for id in gvkey])})
    AND 
    rdq >= '2000-01-01'
    ORDER BY rdq DESC
""")

print(aapl_bs.head(50))