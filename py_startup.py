import traceback
from util.sql_helpers import SchemaConnection, connection_handler
from util.etl_process import create_etl_process
import extract
import transform
import load

@connection_handler
def main(schema_con: SchemaConnection):
    process_id = create_etl_process(schema_con.STG)
    print(f'ETL process ID: {process_id}')
    print('Extracting data...')
    extract.extract(schema_con.STG)
    print('Transforming data...')
    transform.transform(schema_con.STG, process_id)
    print('Loading data...')
    load.load(schema_con, process_id)
    print('Process finished')

try:
    main()
except:
    traceback.print_exc()
