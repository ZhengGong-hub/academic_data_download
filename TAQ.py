# Get TAQ data

from utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# hyperparameters
#FACTOR_PATH = 'data/factors'

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))