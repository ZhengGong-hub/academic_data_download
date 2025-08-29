# Get versions
import sqlalchemy, wrds, pandas as pd
print("sqlalchemy:", sqlalchemy.__version__)
print("wrds:", wrds.__version__)
print("pandas:", pd.__version__)

db = wrds.Connection(wrds_username="zhenggong123", wrds_password="zishak-zighi8-bohCig", use_keyring=False)

list_lib = db.list_libraries()

print("here are the libraries:")
print(list_lib)
print("--------------------------------")

library = "comp"
print(f"here are the tables in {library}:")
print(db.list_tables(library=library))
print("--------------------------------")

table = "wrds_ratios"
print(f"here are the columns in {table} in {library}:")
print(db.describe_table(library=library, table=table))
print("--------------------------------")



