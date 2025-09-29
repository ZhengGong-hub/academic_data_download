from typing import Callable
import numpy as np
import tqdm
import pandas as pd
import glob
import os
from typing import Callable


def sql_execution_in_chunks(db, sql_renderer: Callable, column_to_chunk: str, cache_path: str, chunk_size: int = 100) -> pd.DataFrame:
    """
    Execute a SQL query in chunks.
    """
    # Split chunk_column into chunk_size roughly equal chunks for manageable queries. Build chunk lists (dedupe, drop NaN)
    chunks_list = [
        pd.Series(ch).dropna().tolist()
        for ch in np.array_split(column_to_chunk, chunk_size)
    ]

    for idx, chunks in enumerate(tqdm.tqdm(chunks_list, desc="executing sql in chunks")):
        sql = sql_renderer(chunks) # Build SQL query for this chunk

        # Execute query and save result to a Parquet part file
        df = db.raw_sql(sql)
        os.makedirs(f'{os.path.dirname(cache_path)}/parts', exist_ok=True)
        df.to_parquet(f'{os.path.dirname(cache_path)}/parts/{idx}.parquet', index=False)

    # Merge all part files into a single DataFrame
    print("Merging all part files into a single DataFrame...")
    df_agg = pd.concat([pd.read_parquet(f) for f in glob.glob(f'{os.path.dirname(cache_path)}/parts/*.parquet')], ignore_index=True)
    return df_agg
