import os
from jproperties import Properties

from util.db_connection import DbConnection

def __read_config(file_path: str):
    data_configs = Properties()
    with open(file_path, 'rb') as config_file:
        data_configs.load(config_file)
    data_dict = { key : str(data_configs.get(key).data) for key in data_configs }
    return data_dict

db_config = __read_config('./config/db.properties')
data_config = __read_config('./config/data.properties')

class DbConfig:
    HOST = db_config['DB_HOST']
    PORT = db_config['DB_PORT']
    USER = db_config['DB_USER']
    PASSWORD = db_config['DB_PASSWORD']
    
    class Schema:
        SOR = db_config['DB_SOR_SCHEMA']
        STG = db_config['DB_STG_SCHEMA']

class DataConfig:
    csv_path = os.path.abspath(data_config['DATA_CSV_PATH'])
    @staticmethod
    def get_csv_path(file_name: str) -> str:
        return os.path.join(DataConfig.csv_path, file_name)