# save file to the data folder
import os

def save_file(df, name):
    # create the data folder if it doesn't exist
    os.makedirs('data', exist_ok=True)
    os.makedirs(f'data/factors', exist_ok=True)
    # save the file
    df.to_csv(f'data/factors/{name}.csv', index=False)