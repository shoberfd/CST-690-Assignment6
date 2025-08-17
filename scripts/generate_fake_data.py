# scripts/generate_fake_data.py
import sqlite3
import pandas as pd
import random
from faker import Faker
from datetime import date

# Initialize Faker
fake = Faker()

# Define locations and their corresponding station IDs
locations = {
    "Chicago": "ST101",
    "New York": "ST102",
    "Delhi": "ST103",
    "Minneapolis": "ST104",
    "St. Paul": "ST105",
    "Los Angeles": "ST001"
}

rows = []
for _ in range(500):
    city = random.choice(list(locations.keys()))
    station_id = locations[city]
    rows.append({
        "station_id": station_id,
        "city": city,
        "measurement_date": fake.date_between("-60d", "-1d"),
        "temperature_celsius": round(random.uniform(5, 35), 1),
        "humidity_percent": random.randint(30, 90),
    })

# --- ADD THIS BLOCK TO GUARANTEE 5 MATCHES FOR OFFLINE MODE ---
offline_match_cities = ["Chicago", "New York", "Delhi", "Minneapolis", "St. Paul"]
print(f"Generating 5 guaranteed offline matches for today's date...")

for city in offline_match_cities:
    rows.append({
        "station_id": locations[city],
        "city": city,
        "measurement_date": date.today(),
        "temperature_celsius": round(random.uniform(15, 25), 1), # Give them some varied temps
        "humidity_percent": random.randint(50, 70),
    })
# -----------------------------------------------------------

# Create DataFrame and save to SQLite database
df = pd.DataFrame(rows)
con = sqlite3.connect("data/env_metrics.db")
df.to_sql("weather_station_data", con, if_exists="replace", index=False)
con.close()

print(f"✅ Synthetic database with {len(rows)} total records has been saved to data/env_metrics.db")