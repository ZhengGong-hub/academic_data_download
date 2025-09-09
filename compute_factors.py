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

gvkey_list = ['001690', 
    '002176'
    ] # berkshire and apple
# gvkey_list = None

FactorComputer = FactorComputer(gvkey_list=gvkey_list, verbose=True, db=db)

FactorComputer.gross_profit_to_assets(qtr=True, name='f_gpta')

FactorComputer.sales_to_price(qtr=True, name='f_sp')

# FactorComputer.btm(qtr=True, name='f_btm')

# FactorComputer.debt_to_market(qtr=True, name='f_dtm')

# FactorComputer.earnings_to_price(qtr=True, name='f_ep')

# FactorComputer.cashflow_to_price(qtr=True, name='f_cfp')

# FactorComputer.payout_yield(qtr=True, name='f_py')

# FactorComputer.ev_multiple(qtr=True, name='f_evm')

# FactorComputer.advertising_to_marketcap(qtr=False, name='f_adp')

# FactorComputer.rd_to_marketcap(qtr=True, name='f_rdp')

# FactorComputer.operating_leverage(qtr=True, name='f_ol')

# compute factors

# save results
