import pandas as pd
import sqlalchemy as sal
from datetime import datetime

def log_progress(message):
    '''Logs the entioned message of a given stange 
    of the code execution to the log file. function returns nothing.'''
    with open('./code_log.txt','a') as f:
        f.write(f'{datetime.now()}: {message}\n')

def extract_data():
    # Read the data
    df = pd.read_csv('.\orders.csv', na_values=['Not Available','unknown'])
    log_progress('Data is extracted successfully')
    return df

def transform_data():
    log_progress('Data is transformed successfully')
    dd.columns = dd.columns.str.lower()
    dd.columns = dd.columns.str.replace(' ', '_')
    dd['discount'] = dd['list_price']*(dd['discount_percent']/100)
    dd['sales_price'] = dd['list_price']-dd['discount']
    dd['profit'] = dd['sales_price']-dd['cost_price']
    pd.to_datetime(dd['order_date'],format='%Y-%m-%d')
    dd.drop(columns=['list_price','cost_price','discount_percent'],inplace=True)
    return dd

def load_to_SQL():
    log_progress('Data is loaded successfully to SQL')
    SQLALCHEMY_DATABASE_URL = ("mssql+pyodbc://admin:12345@localhost/master?driver=ODBC+Driver+18+for+SQL+Server&Trusted_Connection=No&TrustServerCertificate=Yes")
    engine = sal.create_engine(SQLALCHEMY_DATABASE_URL)
    conn = engine.connect()
    dd.to_sql('Retail',con = conn, index=False,if_exists='replace')

if __name__ == '__main__':
    dd = extract_data()
    transform_data()
    load_to_SQL()