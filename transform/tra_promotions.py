from sqlalchemy.engine import Engine
from util.sql_helpers import read_table
from transform.transformations import parse_date

table_columns = [
    'PROMO_ID',
    'PROMO_NAME',
    'PROMO_COST',
    'PROMO_BEGIN_DATE',
    'PROMO_END_DATE',
]

def transform_promotions(db_con: Engine, etl_process_id: int) -> None:
    # Read from extract table
    promotions_ext = read_table(
        table_name='promotions_ext',
        columns=table_columns,
        con=db_con
    )

    df_promotions = promotions_ext.copy(deep=True)
    if not promotions_ext.empty:
        # Tranform columns
        df_promotions['PROMO_ID'] = df_promotions['PROMO_ID'].astype(int)
        df_promotions['PROMO_COST'] = df_promotions['PROMO_COST'].astype(float)
        df_promotions['PROMO_BEGIN_DATE'] = df_promotions['PROMO_BEGIN_DATE'].apply(parse_date)
        df_promotions['PROMO_END_DATE'] = df_promotions['PROMO_END_DATE'].apply(parse_date)
        # Add ETL process ID
        df_promotions['ETL_PROC_ID'] = etl_process_id
        # Write to transform table
        df_promotions.to_sql('promotions_tra', con=db_con, if_exists='append',index=False)
