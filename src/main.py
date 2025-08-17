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
    # Main pipeline orchestration function.
    logging.info("Starting data pipeline...")

    # Get configuration from environment variables
    db_path = os.getenv("DB_PATH")
    output_path = os.getenv("OUTPUT_PATH")

    if not db_path or not output_path:
        logging.error("DB_PATH or OUTPUT_PATH environment variables not set.")
        return

    # Step 1: Fetch data from sources
    db_data = get_db_data(db_path)
    api_data = get_api_data(city, offline=offline_mode)

    # Step 2: Transform data
    enriched_data = transform_data(db_data, api_data)

    # Step 3: Save output
    if not enriched_data.empty:
        try:
            # Ensure the output directory exists
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            enriched_data.to_parquet(output_path, index=False)
            logging.info(f"Successfully saved enriched data to {output_path}")
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
        help="City to fetch API data for."
    )
    args = parser.parse_args()

    main(offline_mode=args.offline, city=args.city)