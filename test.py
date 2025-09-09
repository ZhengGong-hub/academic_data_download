from db_manager.wrds_sql import get_sp500_constituents_snapshot
from utils.wrds_connect import connect_wrds
import os
import dotenv
dotenv.load_dotenv()

db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))
sp500_df = get_sp500_constituents_snapshot(db, 2020)
print(sp500_df)

# print if permno is duplicated
print(sp500_df.query("permno.duplicated()"))

