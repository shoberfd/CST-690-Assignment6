# src/transform.py
import pandas as pd
import logging

def transform_data(db_df: pd.DataFrame, api_df: pd.DataFrame) -> pd.DataFrame:
    # Merges database and API data, cleans it, and calculates a health risk KPI.

    if db_df.empty or api_df.empty:
        logging.warning("One of the dataframes is empty. Skipping transformation.")
        return pd.DataFrame()

    # Convert date columns to datetime objects for proper merging
    db_df['measurement_date'] = pd.to_datetime(db_df['measurement_date'])
    api_df['date.utc'] = pd.to_datetime(api_df['date.utc']).dt.date
    api_df['measurement_date'] = pd.to_datetime(api_df['date.utc'])

    # Merge data on city and date
    logging.info("Merging database and API data.")
    merged_df = pd.merge(db_df, api_df, on=["city", "measurement_date"], how="inner")

    # Clean data - fill missing values if any
    merged_df.fillna({
        'pollutant_pm25_ugm3': 0,
        'pollutant_co_ppm': 0
    }, inplace=True)

    logging.info("Calculating Health Risk Index KPI.")
    merged_df['health_risk_index'] = calculate_health_risk(
        merged_df['pollutant_pm25_ugm3'],
        merged_df['temperature_celsius']
    )

    # Select and reorder final columns
    final_df = merged_df[[
        'city',
        'station_id',
        'measurement_date',
        'temperature_celsius',
        'humidity_percent',
        'pollutant_pm25_ugm3',
        'pollutant_co_ppm',
        'health_risk_index'
    ]]

    logging.info(f"Transformation complete. Processed {len(final_df)} records.")
    return final_df

def calculate_health_risk(pm25: pd.Series, temperature: pd.Series) -> pd.Series:
    # Calculates a simple health risk index based on PM2.5 and temperature.
    # Formula: (PM2.5 * 1.5) + (Temperature Penalty)
    # Temperature Penalty is 0 below 25°C, and increases for temps above that.
 
    temp_penalty = temperature.apply(lambda t: max(0, (t - 25) * 2))
    return (pm25 * 1.5) + temp_penalty