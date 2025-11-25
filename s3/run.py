from s3 import S3Handler
import os
import dotenv
dotenv.load_dotenv()

s3_handler = S3Handler(
    aws_access_key_id=os.getenv("S3_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("S3_SECRET_ACCESS_KEY")
)

# s3_handler.download_file(
#     s3_key='trade/futu/vwap_trade/sell.parquet',
#     local_path='target/sell.parquet'
# )

print(s3_handler.list_files(
    prefix='fin-research/'
))

s3_handler.download_folder(
    s3_prefix='fin-research/',
    local_dir='fin-research/'
)