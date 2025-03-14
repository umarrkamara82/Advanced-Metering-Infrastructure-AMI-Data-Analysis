import random
from datetime import datetime, timedelta
import mysql.connector

# DBconnection
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Markovic@22",
    "database": "employee_data"     # 
}
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Helper functions
def get_season(month):
    """
    Return the season based on the month.
    For this example, months 11, 12, 1, 2, 3, and 4 are "Dry" and the rest "Rain".
    """
    return "Dry" if month in [11, 12, 1, 2, 3, 4] else "Rain"

def generate_meter_profile():
    """
    Create profiles for Residential, Commercial, and Industrial meters.
    These values will affect the consumption calculation.
    """
    return {
        "Residential": {"base": 0.5, "growth": 0.05, "noise": 0.1},
        "Commercial": {"base": 2.0, "growth": 0.2, "noise": 0.3},
        "Industrial": {"base": 5.0, "growth": 0.5, "noise": 0.5},
    }

def generate_consumption(base, growth, noise, hour):
    """
    Simulate hourly consumption based on the meter profile.
    A peak factor is applied for hours between 18 and 22.
    """
    hourly_factor = 1.5 if 18 <= hour <= 22 else 1.0
    return round((base + growth * hour + random.uniform(-noise, noise)) * hourly_factor, 2)

def generate_downtime():
    """
    Simulate random downtime with a 1% chance.
    Returns True for downtime, False otherwise.
    """
    return random.random() < 0.01  # 1% chance

# Base meter ID 
BASE_METER_ID = 34160191070

def create_table_if_not_exists():
    """
    Create the ami_data table if it does not exist.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ami_data (
            meter_id BIGINT NOT NULL,
            timestamp DATETIME NOT NULL,
            location VARCHAR(50) NOT NULL,
            season VARCHAR(10) NOT NULL,
            meter_type VARCHAR(50) NOT NULL,
            consumption DECIMAL(10, 2) NOT NULL,
            billing DECIMAL(10, 2) NOT NULL,
            is_downtime TINYINT(1) NOT NULL,
            PRIMARY KEY (meter_id, timestamp)
        );
    """)
    conn.commit()

# Call the function to create the table
create_table_if_not_exists()

# Data generation and insertion
def generate_and_insert_data():
    """
    Generate and insert AMI data into the database.

    """
    # Here we generate 2023.
    start_date = datetime(2023, 1, 1, 0, 0)
    end_date = datetime(2023, 12, 31, 23, 0)  # Data generated on the hour boundaries
    profiles = generate_meter_profile()
    rate_per_kwh = 4.22  # Billing rate per kWh
    batch_size = 50000   # Number of rows to insert per batch
    data_batch = []

    meter_count = 1000   # 1,000 meters (1000 * 8760 = 8,760,000 rows)
    locations = ["West", "East", "North", "South"]
    location_weights = [0.4, 0.2, 0.2, 0.2]  # Distribution for locations
    meter_types = ["Residential", "Commercial", "Industrial"]
    meter_type_weights = [0.6, 0.25, 0.15]    # Distribution for meter types

    current_time = start_date
    # Loop over every hour between start_date and end_date
    while current_time <= end_date:
        for i in range(meter_count):
            # Generate meter_id based on base plus the index
            meter_id = BASE_METER_ID + i

            # Randomize the timestamp by adding random minutes and seconds
            random_minutes = random.randint(0, 59)
            random_seconds = random.randint(0, 59)
            # Create a new timestamp within the current hour boundary
            timestamp = current_time.replace(minute=random_minutes, second=random_seconds)

            # Randomly select location and meter type
            location = random.choices(locations, location_weights)[0]
            meter_type = random.choices(meter_types, meter_type_weights)[0]
            season = get_season(timestamp.month)
            profile = profiles[meter_type]

            consumption = generate_consumption(profile["base"], profile["growth"], profile["noise"], timestamp.hour)
            # Randomly determine downtime; if downtime, set consumption to 0.0
            is_downtime = generate_downtime()
            if is_downtime:
                consumption = 0.0

            billing = round(consumption * rate_per_kwh, 2)

            # Append the generated row data
            data_batch.append((
                meter_id, timestamp, location, season, meter_type, consumption, billing, int(is_downtime)
            ))

            # Insert in batches for performance
            if len(data_batch) >= batch_size:
                cursor.executemany("""
                    INSERT INTO ami_data 
                    (meter_id, timestamp, location, season, meter_type, consumption, billing, is_downtime)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, data_batch)
                conn.commit()
                data_batch = []  # Clear the batch

        # Move to the next hour
        current_time += timedelta(hours=1)

    # Insert any remaining data in the batch
    if data_batch:
        cursor.executemany("""
            INSERT INTO ami_data 
            (meter_id, timestamp, location, season, meter_type, consumption, billing, is_downtime)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, data_batch)
        conn.commit()

# Run the data generation and insertion
generate_and_insert_data()

# Close the database connection
cursor.close()
conn.close()

print("AMI database populated successfully!")