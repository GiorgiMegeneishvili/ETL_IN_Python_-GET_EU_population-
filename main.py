import pandas as pd
import requests
from io import StringIO
from sqlalchemy import create_engine, MetaData, Table
from utils.logger import get_logger
from utils.config import DB_SERVER
from utils.config import DB_NAME

logger = get_logger(__name__)


def get_data_EU_population(url):
    try:
        response = requests.get(url)

        # Wrap response.text in StringIO
        html_data = StringIO(response.text)
        return html_data
    except Exception as e:
        logger.error(f"Error : {e}")
        return None
def transfrom_EU_data(html_data):
    try:
        # Now read the table safely
        tables = pd.read_html(html_data)

        # Get the first table (Population of Georgia (2025 and historical))
        eu_population = tables[0]
        eu_population.rename(columns={'#': 'ID'}, inplace=True)

        return  eu_population
    except Exception as e:
        logger.error(f"Error : {e}")
        return None

def connect_to_sql_server_and_load_Countries_population_data_EU_DATA( eu_population , DB_SERVER, DB_NAME  ):
    # SQLAlchemy engine-ით კავშირი
    engine = create_engine(f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

    try:
        eu_population.to_sql('eu_population', engine, if_exists='', index=False)
        logger.info("eu_population Data has been loaded successfully.")
    except Exception as e:
        logger.error(f"Error : {e}")
def get_data_Countries_population_data(url):
    try:
        response = requests.get(url)

        # Wrap response.text in StringIO
        html_data = StringIO(response.text)
        return html_data
    except Exception as e:
        logger.error(f"Error : {e}")
        return None

def transfrom_Countries_population_data(html_data):
    try:
        # Now read the table safely
        tables = pd.read_html(html_data)
        # Get the first table (Population of Georgia (2025 and historical))
        country_historical_df = tables[0]
        # Get the second table (Georgia Population Forecast)
        country_population_df = tables[1]
        # print(georgia_historical_df.head())
        return  country_historical_df, country_population_df
    except Exception as e:
        logger.error(f"Error : {e}")
        return None

def connect_to_sql_server_and_load_Countries_population_data(country_historical_df, country_population_df , DB_SERVER, DB_NAME ):
    # SQLAlchemy engine-ით კავშირი
    engine = create_engine(f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

    try:
        # pandas DataFrame-ის გადატანა SQL-ში
        #country_historical_df.insert(0, f'{}', range(1, len(country_historical_df) + 1))
        country_historical_df.to_sql("country_historical_df", engine, if_exists='append', index=False)
        country_population_df.to_sql("country_population_df", engine, if_exists='append', index=False)
        logger.info("Data has been loaded successfully.")
    except Exception as e:
        logger.error(f"Error : {e}")
        return None

def clean_population_df(df):
    try:
        # Replace the Unicode minus sign with ASCII minus
        df = df.replace('−', '-', regex=True)

        # Remove '%' and convert to float
        percent_cols = ['Yearly % Change', 'Urban Pop %', "Country's Share of World Pop"]
        for col in percent_cols:
            df[col] = df[col].astype(str).str.replace('%', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Remove commas and convert to integers
        comma_cols = ['Yearly Change', 'Migrants (net)', 'Urban Population', 'World Population']
        for col in comma_cols:
            df[col] = df[col].astype(str).str.replace(',', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Convert remaining numeric columns
        numeric_cols = ['Year', 'Population', 'Median Age', 'Fertility Rate', 'Density (P/Km²)']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        return df
    except Exception as e:
        logger.error(f"Error : {e}")
        return None


if __name__ == '__main__':
    url = f'https://www.worldometers.info/population/countries-in-the-eu-by-population/'
    html_data = get_data_EU_population(url)
    eu_population = transfrom_EU_data(html_data)
    connect_to_sql_server_and_load_Countries_population_data_EU_DATA(eu_population,  DB_SERVER, DB_NAME)
    countries = eu_population['Country (or dependency)'].tolist()

    # Create SQLAlchemy engine
    engine = create_engine(f"mssql+pyodbc://{DB_SERVER}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server", fast_executemany=True)

    # Initialize MetaData object
    metadata = MetaData()

    # Reflect the existing table from the database
    table_name1 = 'country_historical_df'
    table_name2 = 'country_population_df'
    # Bind metadata to engine and reflect only the target table
    metadata.reflect(bind=engine, only=[table_name1,table_name2])
    # Drop table if it exists
    if table_name1 and table_name2 in metadata.tables:
        table1 = metadata.tables[table_name1]
        table2 = metadata.tables[table_name2]
        table1.drop(engine)
        table2.drop(engine)

    for country in countries:
        country1 = country
        country = country.lower()
        if country == "czech republic (czechia)":
            country = 'czechia'
        url = f'https://www.worldometers.info/world-population/{country}-population/'
        html_data = get_data_Countries_population_data(url)
        country_historical_df, country_population_df = transfrom_Countries_population_data(html_data)
        table_historical_df = f'{country}_historical_df'
        table_population_df = f'{country}_population_df'
        country_population_df.drop(country_population_df.columns[-1], axis=1, inplace=True)
        country_historical_df.drop(country_historical_df.columns[-1], axis=1, inplace=True)
        country_population_df = clean_population_df(country_population_df)
        country_historical_df.insert(0, f"country_historical_df", f"{country1}")
        country_population_df.insert(0, f"country_population_df", f"{country1}")
        connect_to_sql_server_and_load_Countries_population_data(country_historical_df,country_population_df, DB_SERVER,DB_NAME)