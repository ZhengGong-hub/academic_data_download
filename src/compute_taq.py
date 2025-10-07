# compute factors
from academic_data_download.factors_lab.taq_builder import TAQBuilder
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
TAQBuilder = TAQBuilder(verbose=True, db=db)
# TAQBuilder.taq_peek(name='taq_peek')
# TAQBuilder.taq_tables(name='taq_tables')
# TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=500, retail_cutoff_lower=0, start_date='2013-01-01', end_date='2016-12-31')
# TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=500, retail_cutoff_lower=0, start_date='2017-01-01', end_date='2019-12-31')
# TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=500, retail_cutoff_lower=0, start_date='2020-01-01', end_date='2021-12-31')
# TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=500, retail_cutoff_lower=0, start_date='2022-01-01', end_date='2022-12-31')
# TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=500, retail_cutoff_lower=0, start_date='2023-01-01', end_date='2023-12-31')
# TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=500, retail_cutoff_lower=0, start_date='2024-01-01', end_date='2024-12-31')
TAQBuilder.taq_retail_markethour(name='taq_retail_markethour', retail_cutoff_upper=100000, retail_cutoff_lower=0, start_date='2013-01-01', end_date='2024-12-31')