import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

datapath = Path(__file__).parent.parent / 'data/data.duckdb'

def get_headers(token):
    """
    """
    headers = {
        'accept': 'application/json',
        'Authorization': 'Bearer ' + token,
    }
    return headers


def data_to_df(data):
    """
    """
    df = pd.json_normalize(
        data.json()['result'][0]['MyEnergyData_MarketDocument']['TimeSeries'][0]['Period']
        , record_path="Point"
        , meta=[['timeInterval', 'end']]
    )
    df = df.drop(columns=["out_Quantity.quality"])
    
    df.columns = ['hour', 'kWh', 'date']
    df['date'] = pd.to_datetime(df['date']) + pd.to_timedelta(df["hour"].astype(int), unit='hours')
    return df[['date', 'kWh']]


def parquet_append(filepath:Path or str, df: pd.DataFrame) -> None:
    """
    Append to dataframe to existing .parquet file. Reads original .parquet file
    in, appends new dataframe, writes new .parquet file out. 
    :param filepath: Filepath for parquet file. 
    :param df: Pandas dataframe to append. Must be same schema as original.
    """
    # Use memory map for speed.
    table_original_file = pq.read_table(source=filepath,  pre_buffer=False, use_threads=True, memory_map=True)
    table_to_append = pa.Table.from_pandas(df)
    # Attempt to cast new schema to existing, e.g. datetime64[ns] to datetime64[us] (may throw otherwise).
    table_to_append = table_to_append.cast(table_original_file.schema)
    # Overwrite old file with empty. 
    # WARNING: PRODUCTION LEVEL CODE SHOULD BE MORE ATOMIC: WRITE TO A TEMPORARY FILE, DELETE THE OLD, RENAME. THEN FAILURES WILL NOT LOSE DATA.
    handle = pq.ParquetWriter(filepath, table_original_file.schema)
    handle.write_table(table_original_file)
    handle.write_table(table_to_append)
    # Writes binary footer. Until this occurs, .parquet file is not usable.
    handle.close()


def tabledef_consumption(tablename: str) -> str:
    query = f"""
    CREATE TABLE {tablename} (
        date TIMESTAMP
        , kWh DECIMAL(9, 2)
    );
    """
    return query
