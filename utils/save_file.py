# save file to the data folder
import os

def save_file(df, name):
    # create the data folder if it doesn't exist
    os.makedirs('data', exist_ok=True)
    os.makedirs(f'data/factors', exist_ok=True)

    # get rid of the row where column name is 'nan'
    df = df.dropna(subset=[name])

    # round to 2 decimal places
    df[name] = df[name].round(2)

    # save the file
    df.to_parquet(f'data/factors/{name}.parquet', index=False)