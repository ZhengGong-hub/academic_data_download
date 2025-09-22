import os

def check_if_calculation_needed(name, gvkey_list, save_path='data/factors'):
    if gvkey_list is None:
        if os.path.exists(f'{save_path}/{name}.parquet'):
            return False
        else:
            return True
    else:
        return True
