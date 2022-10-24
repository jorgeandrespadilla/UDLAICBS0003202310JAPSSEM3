from sqlalchemy.engine import Engine
from extract.ext_channels import extract_channels
from extract.ext_countries import extract_countries
from extract.ext_customers import extract_customers
from extract.ext_products import extract_products
from extract.ext_promotions import extract_promotions
from extract.ext_sales import extract_sales
from extract.ext_times import extract_times

def extract(db_con: Engine):
    extract_times(db_con)
    extract_channels(db_con)
    extract_countries(db_con)
    extract_promotions(db_con)
    extract_customers(db_con)
    extract_products(db_con)
    extract_sales(db_con)