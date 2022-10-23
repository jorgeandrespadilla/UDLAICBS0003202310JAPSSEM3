from typing import List
from sqlalchemy.engine import Engine
import pandas as pd

def create_etl_process(db_con: Engine) -> int:
    """Creates an ETL process record in the database and returns its ID."""
    etl_process_id = db_con.execute('INSERT INTO etl_processes VALUES ()').lastrowid
    return int(etl_process_id)

def read_sql_by_process(
    table_name: str, 
    columns: List[str],
    etl_process_id: int,
    con: Engine,
    etl_process_column: str = 'etl_proc_id',
) -> pd.DataFrame:
    """Reads a table records from the database filtered by ETL process ID."""
    columns_str = ', '.join(columns)
    df = pd.read_sql_query(
        sql=f'SELECT {columns_str} FROM {table_name} WHERE {etl_process_column} = {etl_process_id}',
        con=con
    )
    return df
