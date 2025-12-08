# compute factors
from academic_data_download.factors_lab.earnings import EarningsBuilder
from academic_data_download.utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# hyperparameters
EARNINGS_PATH = 'data/earnings'

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

gvkey_list = ['001690', 
    '002176',
    "002817"
    ] # berkshire and apple, CAT
gvkey_list = None

EarningsBuilder = EarningsBuilder(gvkey_list=gvkey_list, verbose=True, db=db, save_path=EARNINGS_PATH)

EarningsBuilder.trading_date_with_adjacent_earnings_date()