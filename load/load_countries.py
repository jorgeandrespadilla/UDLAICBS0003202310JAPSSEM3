from util.sql_helpers import SchemaConnection, merge_and_insert, read_table

table_columns = [
    'COUNTRY_ID',
    'COUNTRY_NAME',
    'COUNTRY_REGION',
    'COUNTRY_REGION_ID',
]

def load_countries(schema_con: SchemaConnection, etl_process_id: int) -> None:
    countries_tra = read_table(
        table_name='countries_tra',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    countries_sor = read_table(
        table_name='countries',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    merge_and_insert(
        source_df=countries_tra,
        target_table='countries',
        target_df=countries_sor,
        key_columns=['COUNTRY_ID'],
        db_con=schema_con.SOR
    )
