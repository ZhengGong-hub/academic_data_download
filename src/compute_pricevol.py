# compute factors
from academic_data_download.factors_lab.pricevol_builder import PriceVolComputer
from academic_data_download.utils.wrds_connect import connect_wrds
import os
import dotenv
dotenv.load_dotenv()

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

permno_list = None
# permno_list = [
#     # 14542, # Google class C
#     # 14593, # apple
#     17778, # berkshire class A
#     ]

PriceVolComputer = PriceVolComputer(permno_list=permno_list, verbose=True, db=db)
PriceVolComputer.pricevol_processed(name='pricevol_processed')
# PriceVolComputer.stock_split(name='stock_split')
# PriceVolComputer.marketcap(name='marketcap')
# PriceVolComputer.live_pricevol(name='live_pricevol', start_date='2025-10-20', end_date='2025-10-20')
# PriceVolComputer.pricevol_processed_past_prc(name='pricevol_processed_past_prc')