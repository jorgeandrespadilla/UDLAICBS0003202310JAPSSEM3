import os
from jproperties import Properties

def __read_config(file_path: str):
    data_configs = Properties()
    with open(file_path, 'rb') as config_file:
        data_configs.load(config_file)
    data_dict = { key : data_configs.get(key).data for key in data_configs }
    return data_dict

__db_config = __read_config('./config/db.properties')
db_config = {
    'host': __db_config['DB_HOST'],
    'port': __db_config['DB_PORT'],
    'user': __db_config['DB_USER'],
    'password': __db_config['DB_PASSWORD'],
    'sor_schema': __db_config['DB_SOR_SCHEMA'],
    'stg_schema': __db_config['DB_STG_SCHEMA'],
}

__data_config = __read_config('./config/data.properties')
data_config = {
    'csv_path': os.path.abspath(__data_config['DATA_CSV_PATH']),
}
