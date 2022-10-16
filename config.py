import os
from jproperties import Properties

from util.db_connection import DbConnection

def __read_config(file_path: str):
    data_configs = Properties()
    with open(file_path, 'rb') as config_file:
        data_configs.load(config_file)
    data_dict = { key : str(data_configs.get(key).data) for key in data_configs }
    return data_dict

__db_config = __read_config('./config/db.properties')
class DbConfig:
    HOST = __db_config['DB_HOST']
    PORT = __db_config['DB_PORT']
    USER = __db_config['DB_USER']
    PASSWORD = __db_config['DB_PASSWORD']
    
    class Schema:
        SOR = __db_config['DB_SOR_SCHEMA']
        STG = __db_config['DB_STG_SCHEMA']

__data_config = __read_config('./config/data.properties')
class DataConfig:
    csv_path = __data_config['CSV_PATH']

def configure_db_connection(schema: str):
    return DbConnection(
        type="mysql",
        host=DbConfig.HOST,
        port=DbConfig.PORT,
        user=DbConfig.USER,
        password=DbConfig.PASSWORD,
        schema=schema
    )
