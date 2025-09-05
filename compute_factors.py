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

#gvkey_list = ['001690', '002176'] # berkshire and apple

gvkey_list = ['001690', '002817'] # AAPL and CAT

FactorComputer = FactorComputer(gvkey_list=gvkey_list, verbose=True, db=db)

#FactorComputer.gross_profit_to_assets(qtr=True, name='f_gpta')

#FactorComputer.sales_to_price(qtr=True, name='f_sp')

#FactorComputer.btm(qtr=True, name='f_btm')

#FactorComputer.debt_to_equity(qtr=True, name='f_dte')

#FactorComputer.earnings_to_price(qtr=True, name='f_ep')

#FactorComputer.cashflow_to_price(qtr=True, name='f_cfp')

#FactorComputer.return_on_assets(qtr=True, name="f_roa")

#FactorComputer.sales_growth_rank(qtr=True, name="f_sgr")

#FactorComputer.abnormal_capital_investment(qtr=True, name="f_aci")

#FactorComputer.investment_to_assets(qtr=True, name="f_ita")

#FactorComputer.changes_in_ppe(qtr=True, name="f_ppe")

#FactorComputer.investment_growth(qtr=True, name="f_ig")

#FactorComputer.inventory_changes(qtr=True, name="f_ic")

#FactorComputer.operating_accruals(qtr=True, name="f_oa") # problem with ivaoq

#FactorComputer.net_external_finance(qtr=True, name="f_nef") # problem with sstky

#FactorComputer.return_net_operating_assets(qtr=True, name="f_rnoa") 

#FactorComputer.profit_margin(qtr=True, name="f_pm") 

#FactorComputer.asset_turnover(qtr=True, name="f_at")

#FactorComputer.operating_profits_to_equity(qtr=True, name="f_ope") # problem with xintq, txditcq

#FactorComputer.book_leverage(qtr=True, name="f_bl")

#FactorComputer.financial_constraints(qtr=True, name="f_fc")

#FactorComputer.book_scaled_asset_liquidity(qtr=True, name="f_bsal")

FactorComputer.market_scaled_asset_liquidity(qtr=True, name="f_msal")

# pe 
# btm

# compute factors

# save results
