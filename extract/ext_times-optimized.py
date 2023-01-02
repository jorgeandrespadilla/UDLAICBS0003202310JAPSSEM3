import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig

table_name = 'times_ext'
columns_map = {
    # csv_col_name: db_col_name
    'TIME_ID': 'time_id',
    'DAY_NAME': 'day_name',
    'DAY_NUMBER_IN_WEEK': 'day_integer_in_week',
    'DAY_NUMBER_IN_MONTH': 'day_integer_in_month',
    'CALENDAR_WEEK_NUMBER': 'calendar_week_integer',
    'CALENDAR_MONTH_NUMBER': 'calendar_month_integer',
    'CALENDAR_MONTH_DESC': 'calendar_month_desc',
    'END_OF_CAL_MONTH': 'end_of_cal_month',
    'CALENDAR_QUARTER_DESC': 'calendar_quarter_desc',
    'CALENDAR_YEAR': 'calendar_year'
}

def extract_times(db_con: Engine):
    # Read CSV
    times_csv = pd.read_csv(DataConfig.get_csv_path('times.csv'), dtype=str)
    if not times_csv.empty:
        # Assign database column names
        times_df = times_csv.rename(columns=columns_map)
        times_df['calendar_month_name'] = ''
        # Write to database
        db_con.connect().execute(f'TRUNCATE TABLE {table_name}')
        times_df.to_sql(table_name, db_con, if_exists="append",index=False)
