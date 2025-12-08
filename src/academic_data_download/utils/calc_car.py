# gz sep10
# https://quant.stackexchange.com/questions/10048/whats-the-meaning-of-the-intercept-in-asset-pricing-model
import pandas as pd 
import sys
import numpy as np
from statsmodels.regression.rolling import RollingOLS
import os


def calculate_car(companyid, price, addr = 'data/car/individual/', type = "ff6", factor_calc_lag = 0, start_date = '2000-01-01', end_date = '2055-06-01', rolling_window = 252):
    # calculate CAR cumulative abnormal return
    #   FOR ONE STOCK

    # step 1: pull out daily return of one individual stock
    # if the file already exists, return 0
    if os.path.exists(addr + f'{companyid}.parquet'):
        return 0

    if len(price) <= rolling_window:
        return 1 # price history too short 

    if type == "ff6":
        ff_list = ['mktrf','smb','hml','rmw', 'cma', 'umd']
    elif type == "ff3":
        ff_list = ['mktrf','smb','hml']
    elif type == "ff1":
        ff_list = ['mktrf']
    else:
        raise ValueError(f"Invalid type: {type}")


    # step 2: pull out fama french factors 
    ff_factor = pd.read_parquet("data/pricevol/fama_french_5_with_mom_factors.parquet")[['date', 'rf'] + ff_list]

    # attach factors on price_df
    price = pd.merge(price, ff_factor, on = 'date', how = 'inner')
    price['ret-rf'] = price['ret'] - price['rf']

    # step 3: decide rolling window and other parameters 
    rwindow = rolling_window 

    # step 4: run linear regression
    model = RollingOLS(endog=price['ret-rf'].values , exog=price[ff_list],window=rwindow)

    rres = model.fit()
    params = rres.params.shift(factor_calc_lag)

    price['y_hat'] = (params * price[ff_list]).sum(axis = 1)
    price['ar'] = round(price['ret-rf'] - price['y_hat'], 4)
    
    price = price.query('y_hat != 0') # filter out the first xxx rows


    # calculate cumulative abnormal return from ar 
    for _day in [252, 126, 22, 5, 4, 3, 2, 1]:
        price[f'car_{_day}d'] = round(price['ar'].transform(
            lambda x: x.rolling(window=_day).apply(lambda y: np.prod(1 + y) - 1)
        ), 4)
        price[f'fwd_car_{_day}d'] = round(price[f'car_{_day}d'].transform(
            lambda x: x.shift(-_day)
        ), 4)

    # step 5: map out and save CAR
    os.makedirs(addr, exist_ok=True)

    # drop unnecessary columns
    price.drop(columns=['shrout', 'retx', 'cfacpr', 'cfacshr', 'vol', 'prc', 'openprc'], inplace=True)
    price.to_parquet(addr + f'{companyid}.parquet')

    return 0