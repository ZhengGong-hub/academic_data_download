# Get versions
import sqlalchemy, wrds, pandas as pd
print("sqlalchemy:", sqlalchemy.__version__)
print("wrds:", wrds.__version__)
print("pandas:", pd.__version__)

# Create connection
import wrds
conn = wrds.Connection(use_keyring=False)



