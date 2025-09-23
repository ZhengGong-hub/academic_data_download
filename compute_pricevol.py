# compute factors
from academic_data_download.pricevol.assembly_line import PriceVolComputer
from academic_data_download.utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# hyperparameters
FACTOR_PATH = 'data/factors'

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

permno_list = None
# permno_list = [
#     # 14542, # Google class C
#     14593, # apple
#     # 17778, # berkshire class A
#     ]

PriceVolComputer = PriceVolComputer(permno_list=permno_list, verbose=True, db=db)
PriceVolComputer.pricevol_raw(name='crsp_daily')
PriceVolComputer.pricevol_calc(name='crsp_daily_processed')

