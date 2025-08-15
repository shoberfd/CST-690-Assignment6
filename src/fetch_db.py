# src/fetch_db.py
import sqlite3
import pandas as pd
import logging

def get_db_data(db_path: str) -> pd.DataFrame:
    # Loads weather station data from the SQLite database w/error checking
    logging.info(f"Fetching data from database: {db_path}")
    try:
        con = sqlite3.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM weather_station_data", con)
        con.close()
        logging.info(f"Successfully fetched {len(df)} records from the database.")
        return df
    except Exception as e:
        logging.error(f"Error fetching data from database: {e}")
        raise