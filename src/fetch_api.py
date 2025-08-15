# src/fetch_api.py
import requests
import pandas as pd
import logging
from datetime import datetime, timedelta, timezone

def get_api_data(city: str, offline: bool = False) -> pd.DataFrame:

    # Fetches air quality data for a given city from the OpenAQ API
    # or generates simulated data if in offline mode.

    if offline:
        logging.warning("Offline mode enabled. Generating simulated API data.")
        return pd.DataFrame({
            'city': [city],
            'aqi': [75],
            'pollutant_co_ppm': [1.2],
            'pollutant_pm25_ugm3': [22.5]
        })

    # Fetch data for the last 60 days to match our synthetic DB
    date_from = (datetime.now(timezone.utc) - timedelta(days=60)).strftime('%Y-%m-%d')
    api_url = f"https://api.openaq.org/v2/measurements"
    params = {
        "city": city,
        "parameter": ["pm25", "co"],
        "date_from": date_from,
        "limit": 1000, # Fetch a reasonable number of recent records
    }
    logging.info(f"Fetching API data for {city} from {api_url}")
    try:
        response = requests.get(api_url, params=params, timeout=10)
        response.raise_for_status() # Raises an HTTPError for bad responses
        data = response.json()['results']
        if not data:
            logging.warning(f"No API data returned for {city}. Returning empty DataFrame.")
            return pd.DataFrame()
        
        df = pd.json_normalize(data)
        # Standardize column names
        df = df.rename(columns={
            "location": "city",
            "parameter": "pollutant",
            "value": "measurement"
        })
        # Pivot the table to have one row per location/date
        df_pivot = df.pivot_table(index=['city', 'date.utc'], columns='pollutant', values='measurement').reset_index()
        df_pivot = df_pivot.rename(columns={
            'co': 'pollutant_co_ppm',
            'pm25': 'pollutant_pm25_ugm3'
        })
        logging.info(f"Successfully fetched {len(df_pivot)} records from API for {city}.")
        return df_pivot
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {city}: {e}")
        raise