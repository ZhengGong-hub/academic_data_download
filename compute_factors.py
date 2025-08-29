# compute factors
from factors_lab.assembly_line import gross_profit_to_assets
from utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

# get data
gross_profit_to_assets(db, verbose=True, annual=False, name='f_gpta')

# compute factors

# save results
