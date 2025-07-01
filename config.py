import os
import urllib
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DB_SERVER = os.getenv('DB_SERVER')
DB_NAME = os.getenv('DB_NAME')
DB_DRIVER = os.getenv('DB_DRIVER')


params = urllib.parse.quote_plus(
    f"Driver={{{DB_DRIVER}}};"
    f"Server={DB_SERVER};"
    f"Database={DB_NAME};"
    "Trusted_Connection=yes;"
)

DB_CONNECTION_STRING = f"mssql+pyodbc:///?odbc_connect={params}"
create_engine = create_engine(DB_CONNECTION_STRING, fast_executemany=True)
