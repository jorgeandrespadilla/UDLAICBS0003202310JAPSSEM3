from sqlalchemy.engine import Engine
from transform.tra_channels import transform_channels
from transform.tra_countries import transform_countries
from transform.tra_customers import transform_customers
from transform.tra_products import transform_products
from transform.tra_promotions import transform_promotions
from transform.tra_sales import transform_sales
from transform.tra_times import transform_times

def transform(db_con: Engine, etl_process_id: int):
    transform_times(db_con, etl_process_id)
    transform_channels(db_con, etl_process_id)
    transform_countries(db_con, etl_process_id)
    transform_promotions(db_con, etl_process_id)
    transform_customers(db_con, etl_process_id)
    transform_products(db_con, etl_process_id)
    transform_sales(db_con, etl_process_id)