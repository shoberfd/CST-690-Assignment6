# src/main.py
import argparse
import logging
import os
from dotenv import load_dotenv
import pandas as pd

from fetch_api import get_api_data
from fetch_db import get_db_data
from transform import transform_data

# Load environment variables from .env file
load_dotenv()

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main(offline_mode: bool, city: str):
    """Main pipeline orchestration function."""
    logging.info("Starting data pipeline...")

    # Get configuration from environment variables
    db_path = os.getenv("DB_PATH")
    output_path = os.getenv("OUTPUT_PATH")

    if not db_path or not output_path:
        logging.error("DB_PATH or OUTPUT_PATH environment variables not set.")
        return

    final_df = pd.DataFrame() # Initialize an empty dataframe

    # --- UPDATED LOGIC FOR OFFLINE VS. LIVE MODE ---
    if offline_mode:
        logging.info("Offline mode enabled. Processing all 5 guaranteed-match cities.")
        offline_cities = ["Chicago", "New York", "Delhi", "Minneapolis", "St. Paul"]
        all_enriched_data = []

        # Fetch the entire DB once
        db_data = get_db_data(db_path)

        for offline_city in offline_cities:
            logging.info(f"--- Processing offline city: {offline_city} ---")
            # Step 1: Get simulated API data for the current city in the loop
            api_data = get_api_data(offline_city, offline=True)
            
            # Step 2: Transform the data
            enriched_data = transform_data(db_data, api_data)
            if not enriched_data.empty:
                all_enriched_data.append(enriched_data)

        if all_enriched_data:
            final_df = pd.concat(all_enriched_data, ignore_index=True)
            logging.info(f"Successfully processed {len(final_df)} records from {len(offline_cities)} cities.")

    else: # This is the original logic for a live API call
        logging.info(f"Live mode enabled. Processing city: {city}")
        # Step 1: Fetch data from sources
        db_data = get_db_data(db_path)
        api_data = get_api_data(city, offline=False)

        # Step 2: Transform data
        final_df = transform_data(db_data, api_data)
    # --------------------------------------------------

    # Step 3: Save output
    if not final_df.empty:
        try:
            # Ensure the output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            final_df.to_parquet(output_path, index=False)
            logging.info(f"Successfully saved {len(final_df)} enriched records to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save data to parquet file: {e}")
    else:
        logging.warning("Pipeline finished with no data to save.")

    logging.info("Data pipeline finished.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL pipeline for environmental data.")
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Run in offline mode using simulated API data."
    )
    parser.add_argument(
        "--city",
        type=str,
        default="Los Angeles",
        help="City to fetch API data for (ignored in --offline mode)."
    )
    args = parser.parse_args()
    
    main(offline_mode=args.offline, city=args.city)