# compute factors
from factors_lab.assembly_line import FactorComputer
from utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# hyperparameters
FACTOR_PATH = 'data/factors'

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

gvkey_list = ['001690', '002176'] # berkshire and apple

FactorComputer = FactorComputer(gvkey_list=gvkey_list, verbose=True, db=db)

FactorComputer.gross_profit_to_assets(qtr=True, name='f_gpta')

FactorComputer.sales_to_price(qtr=True, name='f_sp')

FactorComputer.btm(qtr=True, name='f_btm')

FactorComputer.debt_to_equity(qtr=True, name='f_dte')

FactorComputer.earnings_to_price(qtr=True, name='f_ep')

FactorComputer.cashflow_to_price(qtr=True, name='f_cfp')

# pe 
# btm

# compute factors

# save results
