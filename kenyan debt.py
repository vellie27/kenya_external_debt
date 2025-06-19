import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Database configuration from .env
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    SSL_MODE = os.getenv('SSL_MODE')

def fetch_world_bank_data():
    """Fetch Kenya's external debt data from World Bank API"""
    url = "https://api.worldbank.org/v2/country/KE/indicator/DT.DOD.DECT.CD"
    params = {
        "format": "json",
        "date": "2010:2024",
        "per_page": 1000
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()[1]  # Actual data is in second element
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        return df[['date', 'value']].rename(columns={
            'date': 'year',
            'value': 'external_debt'
        })
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching World Bank data: {e}")
        return None

def transform_data(df):
    """Clean and prepare the data for storage"""
    if df is None or df.empty:
        return None
    
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Data cleaning using .loc
    df.loc[:, 'year'] = df['year'].astype(int)
    df.loc[:, 'external_debt'] = df['external_debt'].astype(float)
    df.loc[:, 'country'] = 'Kenya'
    
    return df[['country', 'year', 'external_debt']]

def create_db_connection():
    """Create connection to Aiven PostgreSQL"""
    connection_string = (
        f"postgresql://{Config.DB_USER}:{Config.DB_PASSWORD}@"
        f"{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}?"
        f"sslmode={Config.SSL_MODE}"
    )
    
    try:
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def setup_database(engine):
    """Create the database table if it doesn't exist"""
    create_table_sql = text("""
    CREATE TABLE IF NOT EXISTS kenya_external_debt (
        country VARCHAR(50),
        year INT PRIMARY KEY,
        external_debt FLOAT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    create_index_sql = text("""
    CREATE INDEX IF NOT EXISTS idx_kenya_debt_year 
    ON kenya_external_debt(year)
    """)
    
    try:
        with engine.begin() as conn:  # Automatically commits
            conn.execute(create_table_sql)
            conn.execute(create_index_sql)
        print("Database setup completed successfully")
    except Exception as e:
        print(f"Error setting up database: {e}")

def load_to_postgres(df, engine):
    """Load transformed data to PostgreSQL"""
    if df is None or df.empty:
        print("No data to load")
        return False
    
    upsert_sql = text("""
    INSERT INTO kenya_external_debt (country, year, external_debt)
    VALUES (:country, :year, :external_debt)
    ON CONFLICT (year) 
    DO UPDATE SET 
        external_debt = EXCLUDED.external_debt,
        last_updated = CURRENT_TIMESTAMP
    """)
    
    try:
        with engine.begin() as conn:
            # Convert DataFrame to list of dictionaries
            data = df.to_dict('records')
            conn.execute(upsert_sql, data)
        print(f"Successfully loaded {len(df)} records to PostgreSQL")
        return True
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        return False

def main():
    print("Starting Kenya External Debt ETL Pipeline...")
    
    # Step 1: Extract data
    print("Fetching data from World Bank API...")
    raw_data = fetch_world_bank_data()
    
    # Step 2: Transform data
    print("Cleaning and transforming data...")
    clean_data = transform_data(raw_data)
    
    # Step 3: Load data
    print("Connecting to Aiven PostgreSQL...")
    engine = create_db_connection()
    if engine:
        setup_database(engine)
        load_to_postgres(clean_data, engine)
    
    print("ETL process completed")

if __name__ == "__main__":
    main()