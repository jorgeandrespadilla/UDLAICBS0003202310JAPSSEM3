from util.sql_helpers import SchemaConnection, merge_and_insert, read_table

table_columns = [
    'PROMO_ID',
    'PROMO_NAME',
    'PROMO_COST',
    'PROMO_BEGIN_DATE',
    'PROMO_END_DATE',
]

def load_promotions(schema_con: SchemaConnection, etl_process_id: int) -> None:
    promotions_tra = read_table(
        table_name='PROMOTIONS_TRA',
        columns=table_columns,
        con=schema_con.STG,
        with_process_id=etl_process_id
    )
    promotions_sor = read_table(
        table_name='PROMOTIONS',
        columns=['ID', *table_columns],
        con=schema_con.SOR
    )
    merge_and_insert(
        source_df=promotions_tra,
        target_table='PROMOTIONS',
        target_df=promotions_sor,
        key_columns=['PROMO_ID'],
        db_con=schema_con.SOR
    )
