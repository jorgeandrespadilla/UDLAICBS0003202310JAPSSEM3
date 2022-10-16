from config import DbConfig
from sqlalchemy.engine import Engine
from util.connection_handler import connection_handler
from extract.ext_channels import extract_channels, truncate_channels
from extract.ext_countries import extract_countries, truncate_countries
from extract.ext_customers import extract_customers, truncate_customers
from extract.ext_products import extract_products, truncate_products
from extract.ext_promotions import extract_promotions, truncate_promotions
from extract.ext_sales import extract_sales, truncate_sales
from extract.ext_times import extract_times, truncate_times
from util.timer import timer

def truncate(db_con: Engine):
    db_con.execute("SET FOREIGN_KEY_CHECKS = 0")
    truncate_sales(db_con)
    truncate_products(db_con)
    truncate_customers(db_con)
    truncate_promotions(db_con)
    truncate_countries(db_con)
    truncate_channels(db_con)
    truncate_times(db_con)
    db_con.execute("SET FOREIGN_KEY_CHECKS = 1")

@connection_handler(DbConfig.Schema.STG)
def extract(db_con: Engine):
    truncate(db_con)
    
    extract_times(db_con)
    extract_channels(db_con)
    extract_countries(db_con)
    extract_promotions(db_con)
    extract_customers(db_con)
    extract_products(db_con)
    extract_sales(db_con)