# compute factors
from factors_lab.assembly_line import gross_profit_to_assets, sales_to_price, return_on_assets, sales_growth_rank, abnormal_capital_investment, investment_to_assets, changes_in_ppe, investment_growth, inventory_changes, operating_accruals, net_external_finance, return_net_operating_assets, profit_margin, asset_turnover, operating_profits_to_equity, book_leverage, financial_constraints, book_scaled_asset_liquidity, market_scaled_asset_liquidity
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

# Make NA in ivaoq to 0

#if not os.path.exists(f'{FACTOR_PATH}/f_rnoa.csv'):
#    return_net_operating_assets(db, verbose=True, annual=False, name='f_rnoa')

#if not os.path.exists(f'{FACTOR_PATH}/f_pm.csv'):
#    profit_margin(db, verbose=True, annual=False, name='f_pm')

#if not os.path.exists(f'{FACTOR_PATH}/f_at.csv'):
#    asset_turnover(db, verbose=True, annual=False, name='f_at')

#if not os.path.exists(f'{FACTOR_PATH}/f_ope.csv'):
#    operating_profits_to_equity(db, verbose=True, annual=False, name='f_ope')

if not os.path.exists(f'{FACTOR_PATH}/f_bl.csv'):
    book_leverage(db, verbose=True, annual=False, name='f_bl')

if not os.path.exists(f'{FACTOR_PATH}/f_fc.csv'):
    financial_constraints(db, verbose=True, annual=False, name='f_fc')

if not os.path.exists(f'{FACTOR_PATH}/f_bsal.csv'):
    book_scaled_asset_liquidity(db, verbose=True, annual=False, name='f_bsal')

if not os.path.exists(f'{FACTOR_PATH}/f_msal.csv'):
    market_scaled_asset_liquidity(db, verbose=True, annual=False, name='f_msal')

# pe 
# btm

# compute factors

# save results
