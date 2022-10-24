from util.sql_helpers import SchemaConnection
from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times


def load(schema_con: SchemaConnection, etl_process_id: int):
    load_times(schema_con, etl_process_id)
    load_channels(schema_con, etl_process_id)
    load_countries(schema_con, etl_process_id)
    load_promotions(schema_con, etl_process_id)
    load_customers(schema_con, etl_process_id)
    load_products(schema_con, etl_process_id)
    load_sales(schema_con, etl_process_id)