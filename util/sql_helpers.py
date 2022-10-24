import functools
from re import L
import traceback
from typing import List, Tuple
from sqlalchemy.engine import Engine
import pandas as pd
from util.db_connection import DbConnection
from config import DbConfig

class SchemaConnection:
    STG: Engine
    SOR: Engine

    def __init__(self):
        self.STG = self.__configure_connection(DbConfig.Schema.STG)
        self.SOR = self.__configure_connection(DbConfig.Schema.SOR)

    def begin(self):
        self.STG.begin()
        self.SOR.begin()

    def dispose(self):
        self.STG.dispose()
        self.SOR.dispose()

    def __configure_connection(self, schema: str) -> Engine:
        db_type = 'mysql'
        con_db = DbConnection(
            type = db_type,
            host = DbConfig.HOST,
            port = DbConfig.PORT,
            user = DbConfig.USER,
            password = DbConfig.PASSWORD,
            database = schema
        )
        ses_db = con_db.start()
        if ses_db == -1:
            raise Exception(f"The give database type '{db_type}' is not valid")
        elif ses_db == -2:
            raise Exception(f"Error trying connect to the database '{schema}'")
        return ses_db

def connection_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            schema_con = SchemaConnection()
            schema_con.begin()
            func(schema_con, *args, **kwargs)
            schema_con.dispose()
        except:
            traceback.print_exc()
    return wrapper

def map_relations(
    df: pd.DataFrame,
    con: Engine,
    relations: List[Tuple[str, str, str]],
    id_column: str = "ID",
) -> None:
    """Creates a new dataframe including the relations between the tables."""
    mapped_df = df.copy()
    for relation in relations:
        destination_column, source_column, source_table = relation
        mapped_ids = pd.read_sql_query(
            sql=f'SELECT {id_column}, {source_column} FROM {source_table}',
            con=con,
        ).set_index(source_column).to_dict()[id_column]
        mapped_df[destination_column] = mapped_df[source_column].apply(lambda x: mapped_ids[x])
    return mapped_df
            
def merge_and_insert(
    source_table: str,
    source_df: pd.DataFrame,
    target_table: str,
    target_df: pd.DataFrame,
    key_column: str,
    db_con: Engine,
    id_column: str = "ID",
) -> None:
    """
    Merges two dataframes and inserts the new records into the target table.
    If the data already exists, it will be updated.
    Otherwise, it will be inserted.
    """
    filtered_target_df = target_df.drop(columns=[id_column])
    df_result = source_df.merge(filtered_target_df, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    if not df_result.empty:
        df_to_insert = df_result[~df_result[key_column].isin(target_df[key_column])]
        if not df_to_insert.empty:
            df_to_insert.to_sql(name=target_table, con=db_con, if_exists='append', index=False)
        df_to_update = df_result[df_result[key_column].isin(target_df[key_column])]
        if not df_to_update.empty:
            # Add ID column
            df_to_update = df_to_update.merge(target_df[[id_column, key_column]], how='left', on=key_column).set_index(id_column)
            for index, row in df_to_update.iterrows():
                query_fields = []
                query_data = []
                for column, value in row.items():
                    query_fields.append(f'{column} = %s')
                    query_data.append(value)
                query_str = str(f'UPDATE {target_table} SET {", ".join(query_fields)} WHERE {id_column} = {index}')
                db_con.execute(query_str, query_data)
