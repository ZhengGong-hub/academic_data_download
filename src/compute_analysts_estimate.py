# compute factors
from academic_data_download.factors_lab.analyst_estimation_builder import AnalystEstimationBuilder
from academic_data_download.utils.wrds_connect import connect_wrds
import os
import dotenv
dotenv.load_dotenv()

# connect to db
db = connect_wrds(username=os.getenv("WRDS_USERNAME"), password=os.getenv("WRDS_PASSWORD"))

permno_list = None
# permno_list = [
#     # 14542, # Google class C
#     14593, # apple
#     # 17778, # berkshire class A
#     ]

AnalystEstimationBuilder = AnalystEstimationBuilder(permno_list=permno_list, verbose=True, db=db)
# AnalystEstimationBuilder.price_target_summary(name='price_target_summary')
# AnalystEstimationBuilder.price_target_detail(name='price_target_detail')
# AnalystEstimationBuilder.price_target_detail_revision(name='price_target_detail_revision')
# AnalystEstimationBuilder.eps_detail_qtr(name='eps_detail_qtr')
# AnalystEstimationBuilder.eps_detail_ann(name='eps_detail_ann')
AnalystEstimationBuilder.pt_detail_with_eps_estimate(name='pt_detail_with_eps_estimate')


