# save file to the data folder
import os

def save_file(df, name, path='data/factors'):
    # create the data folder if it doesn't exist
    os.makedirs('data', exist_ok=True)
    os.makedirs(path, exist_ok=True)

    # round to 2 decimal places for all columns start with 'f_'
    df[df.columns[df.columns.str.startswith('f_')]] = df[df.columns[df.columns.str.startswith('f_')]].round(4)

    # save the file
    df.to_parquet(f'{path}/{name}.parquet', index=False)
    print(f"Saved {name} to {path}/{name}.parquet")
