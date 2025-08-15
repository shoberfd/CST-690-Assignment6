# scripts/generate_fake_data.py
import sqlite3
import pandas as pd
import random
from faker import Faker

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
        "measurement_date": fake.date_between("-60d", "today"),
        "temperature_celsius": round(random.uniform(5, 35), 1),
        "humidity_percent": random.randint(30, 90),
    })

# Create DataFrame and save to SQLite database
df = pd.DataFrame(rows)
con = sqlite3.connect("data/env_metrics.db")
df.to_sql("weather_station_data", con, if_exists="replace", index=False)
con.close()

print("Synthetic database saved to data/env_metrics.db")
