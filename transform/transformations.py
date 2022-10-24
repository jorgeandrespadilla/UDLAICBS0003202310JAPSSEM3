from datetime import datetime

def get_month_name(month_number):
    return datetime.strptime(str(month_number), '%m').strftime('%B').upper()

def parse_date(date_str):
    return datetime.strptime(date_str, '%d-%b-%y')
