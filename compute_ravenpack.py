# compute factors
from academic_data_download.factors_lab.ravenpack_builder import RavenpackBuilder
from academic_data_download.utils.wrds_connect import connect_wrds
import os
import dotenv
import pandas as pd
dotenv.load_dotenv()

# hyperparameters
FACTOR_PATH = 'data/ravenpack'

# permno_list = None
permno_list = [
    14542, # Google class C
    14593, # apple
    17778, # berkshire class A
    ]

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

RPBuilder = RavenpackBuilder(permno_list=permno_list, verbose=True, db=db, save_path=FACTOR_PATH)

# ravenpack
RPBuilder.ravenpack_equities(name=f'f_rp_ess', start_year=2009, end_year=2025)
# RPBuilder.ravenpack_global_macro(name=f'f_rp_global_macro', start_year=2009, end_year=2025)