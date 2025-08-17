# src/fetch_api.py
import requests
import pandas as pd
import logging
import os
from datetime import datetime, timedelta, timezone

def get_api_data(city: str, offline: bool = False) -> pd.DataFrame:
    """
    Fetches air quality data for a given city from the OpenAQ v3 API
    or generates simulated data if in offline mode.
    """
    if offline:
        logging.warning("Offline mode enabled. Generating simulated API data.")
        # Create a more realistic offline dataframe with the required date column
        today = datetime.now(timezone.utc)
        return pd.DataFrame({
            'city': [city],
            'date.utc': [today.isoformat()], # <-- ADDED: Date column
            'pollutant_pm25_ugm3': [22.5]
        })

    api_key = os.getenv("OPENAQ_API_KEY")
    if not api_key:
        logging.error("OpenAQ API key not found. Please set OPENAQ_API_KEY in your .env file.")
        raise ValueError("API Key not configured.")

    API_BASE_URL = "https://api.openaq.org/v3"
    headers = {"accept": "application/json", "X-API-Key": api_key}
    
    # --- Step 1: Find a location ID for the city that measures PM2.5 ---
    logging.info(f"Searching for up to 5 potential PM2.5 sensor locations in: {city}")
    try:
        locations_response = requests.get(
            f"{API_BASE_URL}/locations",
            headers=headers,
            params={"city": city, "limit": 5, "parameter": "pm25"},
            timeout=15
        )
        locations_response.raise_for_status()
        locations_data = locations_response.json()['results']
        
        if not locations_data:
            logging.warning(f"No potential sensor locations found for city: {city}.")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get locations for {city}: {e}")
        raise

    # --- Step 2: Iterate through locations to find one with recent data ---
    date_from = (datetime.now(timezone.utc) - timedelta(days=60)).strftime('%Y-%m-%d')
    final_df = pd.DataFrame()

    for location in locations_data:
        location_id = location['id']
        logging.info(f"Attempting to fetch measurements for location ID: {location_id}")
        try:
            measurements_response = requests.get(
                f"{API_BASE_URL}/measurements",
                headers=headers,
                params={
                    'location_id': str(location_id),
                    'date_from': date_from,
                    'parameter': 'pm25',
                    'limit': 1000
                },
                timeout=15
            )
            
            if measurements_response.status_code != 200:
                logging.warning(f"Location {location_id} returned status {measurements_response.status_code}. Trying next location.")
                continue

            data = measurements_response.json()['results']
            if not data:
                logging.warning(f"Location {location_id} returned no data. Trying next location.")
                continue
            
            logging.info(f"Successfully found data for location ID: {location_id}")
            df = pd.json_normalize(data)
            final_df = df.rename(columns={'value': 'pollutant_pm25_ugm3', 'location': 'city'})
            final_df['city'] = city
            break

        except requests.exceptions.RequestException as e:
            logging.warning(f"Request failed for location {location_id}: {e}. Trying next location.")
            continue
    
    if final_df.empty:
        logging.error(f"Could not retrieve valid measurement data for {city} after trying {len(locations_data)} locations.")
    
    return final_df