# compute factors
from factors_lab.assembly_line import gross_profit_to_assets, sales_to_price, return_on_assets, sales_growth_rank, abnormal_capital_investment, investment_to_assets, changes_in_ppe, investment_growth, inventory_changes, operating_accruals, net_external_finance, return_net_operating_assets
from utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# hyperparameters
FACTOR_PATH = 'data/factors'

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

#if not os.path.exists(f'{FACTOR_PATH}/f_gpta.csv'):
#    gross_profit_to_assets(db, verbose=True, annual=False, name='f_gpta')

#if not os.path.exists(f'{FACTOR_PATH}/f_sp.csv'):
#    sales_to_price(db, verbose=True, annual=False, name='f_sp')

#if not os.path.exists(f'{FACTOR_PATH}/f_roa.csv'):
#    return_on_assets(db, verbose=True, annual=False, name='f_roa')

#if not os.path.exists(f'{FACTOR_PATH}/f_sgr.csv'):
#    sales_growth_rank(db, verbose=True, annual=False, name='f_sgr')

#if not os.path.exists(f'{FACTOR_PATH}/f_aci.csv'):
#    abnormal_capital_investment(db, verbose=True, annual=False, name='f_aci')

#if not os.path.exists(f'{FACTOR_PATH}/f_ita.csv'):
#    investment_to_assets(db, verbose=True, annual=False, name='f_ita')

#if not os.path.exists(f'{FACTOR_PATH}/f_ppe.csv'):
#    changes_in_ppe(db, verbose=True, annual=False, name='f_ppe')

#if not os.path.exists(f'{FACTOR_PATH}/f_ig.csv'):
#    investment_growth(db, verbose=True, annual=False, name='f_ig')

#if not os.path.exists(f'{FACTOR_PATH}/f_ic.csv'):
#    inventory_changes(db, verbose=True, annual=False, name='f_ic')

#if not os.path.exists(f'{FACTOR_PATH}/f_oa.csv'):
#    operating_accruals(db, verbose=True, annual=False, name='f_oa')


if not os.path.exists(f'{FACTOR_PATH}/f_nef.csv'):
    net_external_finance(db, verbose=True, annual=False, name='f_nef')

if not os.path.exists(f'{FACTOR_PATH}/f_rnoa.csv'):
    return_net_operating_assets(db, verbose=True, annual=False, name='f_rnoa')


# pe 
# btm

# compute factors

# save results
