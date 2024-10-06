import pyodbc
import pandas as pd
from sqlalchemy import create_engine, URL
from sqlalchemy import Column, Integer, Row, String, create_engine, select
from sqlalchemy.engine import Row

driver = 'ODBC Driver 18 for SQL Server'
server = ''
database = ''
username = ''
password = ''

conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
conn = pyodbc.connect(conn_str)

####################### SELECT #######################
sql_query = """
SELECT bidding_zone_id,provider_id,CAST(issue_date AS varchar(255)) AS issue_date
FROM STT.load_forecasts
"""
df = pd.read_sql(sql_query, conn)
print(df.to_string())

####################### INSERT #######################
data = {
    'datetime_from': ['2000-03-22 00:00:00+01:00'],
    'value': ['53'],
    'tag': ['ISP1 Results'],
    'bidding_zone_id': ['1'],
    'duration': ['00:30:00'],
    'issue_date': ['2024-03-21 17:20:00+0100'],
    'registration_date': ['2024-03-22 16:46:36+0100'],
    'reserve_direction': ['up'],
    'reserve_type': ['FCR'],
    'provider_id': ['4']
}

df = pd.DataFrame(data)
print(df.dtypes)

db_url = URL.create(
    drivername="mssql+pyodbc",
    username=username,
    password=password,
    host=server,
    database=database,
    query={"driver": "ODBC Driver 18 for SQL Server"},
)

engine = create_engine(db_url)

table_name = ''
schema_name = ''

df.to_sql(table_name, con=engine, schema=schema_name, if_exists='append', index=False)

