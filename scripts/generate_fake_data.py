# scripts/generate_fake_data.py
import sqlite3
import pandas as pd
import random
from faker import Faker
from datetime import date # <-- Import date

# Initialize Faker
fake = Faker()

# Define cities and their corresponding station IDs
locations = {
    "Los Angeles": "ST001",
    "New York": "ST002",
    "Chicago": "ST003",
    "Houston": "ST004",
    "Phoenix": "ST005"
}

rows = []
for _ in range(500):
    city = random.choice(list(locations.keys()))
    station_id = locations[city]
    rows.append({
        "station_id": station_id,
        "city": city,
        "measurement_date": fake.date_between("-60d", "-1d"), # Use past dates for random data
        "temperature_celsius": round(random.uniform(5, 35), 1),
        "humidity_percent": random.randint(30, 90),
    })

# --- ADD THIS BLOCK TO GUARANTEE A MATCH FOR OFFLINE MODE ---
# Add one record for today's date for the default city 'Los Angeles'
rows.append({
    "station_id": "ST001",
    "city": "Los Angeles",
    "measurement_date": date.today(),
    "temperature_celsius": 25.0,
    "humidity_percent": 60,
})
# -----------------------------------------------------------

# Create DataFrame and save to SQLite database
df = pd.DataFrame(rows)
con = sqlite3.connect("data/env_metrics.db")
df.to_sql("weather_station_data", con, if_exists="replace", index=False)
con.close()

print("Synthetic database with a guaranteed match for today has been saved to data/env_metrics.db")
