import os

def check_if_calculation_needed(name, gvkey_list):
    if gvkey_list is None:
        if os.path.exists(f'data/factors/{name}.parquet'):
            return False
        else:
            return True
    else:
        return True
